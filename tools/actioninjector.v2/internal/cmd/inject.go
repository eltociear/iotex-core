// Copyright (c) 2019 IoTeX Foundation
// This source code is provided 'as is' and no warranties are given as to title or non-infringement, merchantability
// or fitness for purpose and, to the extent permitted by law, all liability for your use of the code is disclaimed.
// This source code is governed by Apache License 2.0 that can be found in the LICENSE file.

package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/iotexproject/go-pkgs/crypto"
	"github.com/iotexproject/go-pkgs/hash"
	"github.com/iotexproject/iotex-address/address"
	"github.com/iotexproject/iotex-antenna-go/v2/account"
	"github.com/iotexproject/iotex-antenna-go/v2/iotex"
	"github.com/iotexproject/iotex-proto/golang/iotexapi"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	yaml "gopkg.in/yaml.v2"

	"github.com/iotexproject/iotex-core/pkg/log"
	"github.com/iotexproject/iotex-core/tools/util"
)

var (
	transferPayload = []byte{}
)

// KeyPairs indicate the keypair of accounts getting transfers from Creator in genesis block
type KeyPairs struct {
	Pairs []KeyPair `yaml:"pkPairs"`
}

// KeyPair contains the public and private key of an address
type KeyPair struct {
	PK string `yaml:"pubKey"`
	SK string `yaml:"priKey"`
}

type injectProcessor struct {
	api            iotexapi.APIServiceClient
	accountManager *util.AccountManager
}

func newInjectionProcessor() (*injectProcessor, error) {
	var conn *grpc.ClientConn
	var err error
	grpcctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	log.L().Info("Server Addr", zap.String("endpoint", injectCfg.serverAddr))
	if injectCfg.insecure {
		log.L().Info("insecure connection")
		conn, err = grpc.DialContext(grpcctx, rawInjectCfg.serverAddr, grpc.WithBlock(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		log.L().Info("secure connection")
		conn, err = grpc.DialContext(grpcctx, rawInjectCfg.serverAddr, grpc.WithBlock(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}
	if rawInjectCfg.transferPayloadSize != "0" {
		payloadSz := util.ParseHumanSize(rawInjectCfg.transferPayloadSize)
		if payloadSz > 0 {
			randomBytes := make([]byte, payloadSz)
			_, err := rand.Read(randomBytes)
			if err != nil {
				panic(err)
			}
			transferPayload = randomBytes
		}
	}
	if err != nil {
		return nil, err
	}
	api := iotexapi.NewAPIServiceClient(conn)
	if err != nil {
		return nil, err
	}
	p := &injectProcessor{
		api: api,
	}
	log.L().Info("generate random accounts for injector")
	if err = p.randAccounts(injectCfg.randAccounts); err != nil {
		return p, err
	}
	if injectCfg.loadTokenAmount.BitLen() != 0 {
		if err := p.loadAccounts(injectCfg.configPath); err != nil {
			return p, err
		}
	}
	log.L().Info("sync nonce for injector")
	p.syncNonces(context.Background())
	return p, nil
}

func (p *injectProcessor) randAccounts(num int) error {
	addrKeys := make([]*util.AddressKey, 0, num)
	for i := 0; i < num; i++ {
		s := hash.Hash256b([]byte{byte(i), byte(100)})
		private, err := crypto.BytesToPrivateKey(s[:])
		if err != nil {
			return err
		}
		a, _ := account.PrivateKeyToAccount(private)
		addrKeys = append(addrKeys, &util.AddressKey{PriKey: private, EncodedAddr: a.Address().String()})
	}
	p.accountManager = util.NewAccountManager(addrKeys)
	return nil
}

func (p *injectProcessor) loadAccounts(keypairsPath string) error {
	keyPairBytes, err := os.ReadFile(filepath.Clean(keypairsPath))
	if err != nil {
		return errors.Wrap(err, "failed to read key pairs file")
	}
	var keypairs KeyPairs
	if err := yaml.Unmarshal(keyPairBytes, &keypairs); err != nil {
		return errors.Wrap(err, "failed to unmarshal key pairs bytes")
	}

	// Construct iotex addresses from loaded key pairs
	addrKeys := make([]*util.AddressKey, 0)
	for _, pair := range keypairs.Pairs {
		pk, err := crypto.HexStringToPublicKey(pair.PK)
		if err != nil {
			return errors.Wrap(err, "failed to decode public key")
		}
		sk, err := crypto.HexStringToPrivateKey(pair.SK)
		if err != nil {
			return errors.Wrap(err, "failed to decode private key")
		}
		addr := pk.Address()
		log.L().Info("loaded account", zap.String("addr", addr.String()))
		if addr == nil {
			return errors.New("failed to get address")
		}
		addrKeys = append(addrKeys, &util.AddressKey{EncodedAddr: addr.String(), PriKey: sk})
	}

	// send tokens
	for i, recipientAddr := range p.accountManager.GetAllAddr() {
		sender := addrKeys[i%len(addrKeys)]
		operatorAccount, _ := account.PrivateKeyToAccount(sender.PriKey)
		recipient, _ := address.FromString(recipientAddr)
		log.L().Info("generated account", zap.String("addr", recipient.String()))
		c := iotex.NewAuthedClient(p.api, injectCfg.chainID, operatorAccount)
		caller := c.Transfer(recipient, injectCfg.loadTokenAmount).SetGasPrice(injectCfg.transferGasPrice).SetGasLimit(injectCfg.transferGasLimit)
		if _, err := caller.Call(context.Background()); err != nil {
			log.L().Error("Failed to inject.", zap.Error(err))
		}
		if i != 0 && i%len(addrKeys) == 0 {
			time.Sleep(10 * time.Second)
		}
	}
	return nil
}

func (p *injectProcessor) syncNoncesProcess(ctx context.Context) {
	reset := time.NewTicker(injectCfg.resetInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-reset.C:
			p.syncNonces(context.Background())
		}
	}
}

func (p *injectProcessor) syncNonces(ctx context.Context) {
	for _, addr := range p.accountManager.GetAllAddr() {
		err := backoff.Retry(func() error {
			resp, err := p.api.GetAccount(ctx, &iotexapi.GetAccountRequest{Address: addr})
			if err != nil {
				return err
			}
			p.accountManager.Set(addr, resp.GetAccountMeta().GetPendingNonce())
			return nil
		}, backoff.NewExponentialBackOff())
		if err != nil {
			log.L().Fatal("Failed to inject actions by APS",
				zap.Error(err),
				zap.String("addr", addr))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (p *injectProcessor) injectProcess(ctx context.Context) {
	log.L().Info("Start to inject actions")
	var workers sync.WaitGroup
	ticks := make(chan uint64)
	for i := uint64(0); i < injectCfg.workers; i++ {
		workers.Add(1)
		go p.inject(&workers, ticks)
	}

	defer workers.Wait()
	defer close(ticks)
	interval := uint64(time.Second.Nanoseconds() / int64(injectCfg.aps))
	began, count := time.Now(), uint64(0)
	for {
		now, next := time.Now(), began.Add(time.Duration(count*interval))
		time.Sleep(next.Sub(now))
		select {
		case <-ctx.Done():
			return
		case ticks <- count:
			count++
		default:
			workers.Add(1)
			go p.inject(&workers, ticks)
		}
	}
}

func (p *injectProcessor) inject(workers *sync.WaitGroup, ticks <-chan uint64) {
	defer workers.Done()
	for range ticks {
		go func() {
			caller, err := p.pickAction()
			if err != nil {
				log.L().Error("Failed to create an action", zap.Error(err))
			}
			var actionHash hash.Hash256
			bo := backoff.WithMaxRetries(backoff.NewConstantBackOff(injectCfg.retryInterval), injectCfg.retryNum)
			if rerr := backoff.Retry(func() error {
				actionHash, err = caller.Call(context.Background())
				return err
			}, bo); rerr != nil {
				log.L().Error("Failed to inject.", zap.Error(rerr))
			}

			c := iotex.NewReadOnlyClient(p.api)

			if injectCfg.checkReceipt {
				time.Sleep(25 * time.Second)
				var response *iotexapi.GetReceiptByActionResponse
				if rerr := backoff.Retry(func() error {
					response, err = c.GetReceipt(actionHash).Call(context.Background())
					return err
				}, bo); rerr != nil {
					log.L().Error("Failed to get receipt.", zap.Error(rerr))
				}
				if response.ReceiptInfo.Receipt.Status != 1 {
					log.L().Error("Receipt has failed status.", zap.Uint64("status", response.ReceiptInfo.Receipt.Status))
				}
			}
		}()
	}
}

func (p *injectProcessor) pickAction() (iotex.Caller, error) {
	switch injectCfg.actionType {
	case "transfer":
		return p.transferCaller()
	case "execution":
		return p.executionCaller()
	case "mixed":
		if rand.Intn(2) == 0 {
			return p.transferCaller()
		}
		return p.executionCaller()
	default:
		return p.executionCaller()
	}
}

func (p *injectProcessor) executionCaller() (iotex.ExecuteContractCaller, error) {
	var nonce uint64
	sender := p.accountManager.AccountList[rand.Intn(len(p.accountManager.AccountList))]
	nonce = p.accountManager.GetAndInc(sender.EncodedAddr)

	operatorAccount, _ := account.PrivateKeyToAccount(sender.PriKey)
	c := iotex.NewAuthedClient(p.api, injectCfg.chainID, operatorAccount)
	address, _ := address.FromString(injectCfg.contract)
	abiJSONVar, _ := abi.JSON(strings.NewReader(_abiStr))
	contract := c.Contract(address, abiJSONVar)

	caller := contract.Execute("consumeGas", big.NewInt(int64(injectCfg.arg1))).
		SetNonce(nonce).
		SetAmount(injectCfg.executionAmount).
		SetGasPrice(injectCfg.executionGasPrice).
		SetGasLimit(injectCfg.executionGasLimit)

	return caller, nil
}

func (p *injectProcessor) transferCaller() (iotex.SendActionCaller, error) {
	var nonce uint64
	sender := p.accountManager.AccountList[rand.Intn(len(p.accountManager.AccountList))]
	nonce = p.accountManager.GetAndInc(sender.EncodedAddr)

	operatorAccount, _ := account.PrivateKeyToAccount(sender.PriKey)
	c := iotex.NewAuthedClient(p.api, injectCfg.chainID, operatorAccount)

	recipient, _ := address.FromString(p.accountManager.AccountList[rand.Intn(len(p.accountManager.AccountList))].EncodedAddr)
	caller := c.Transfer(recipient, injectCfg.transferAmount).
		SetPayload(transferPayload).
		SetNonce(nonce).
		SetGasPrice(injectCfg.transferGasPrice).
		SetGasLimit(injectCfg.transferGasLimit)
	return caller, nil
}

// injectCmd represents the inject command
var injectCmd = &cobra.Command{
	Use:   "inject",
	Short: "inject actions [options : -m] (default:random)",
	Long:  `inject actions [options : -m] (default:random).`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(inject(args))
	},
}

var rawInjectCfg = struct {
	configPath          string
	serverAddr          string
	chainID             uint32
	transferGasLimit    uint64
	transferGasPrice    int64
	transferAmount      int64
	transferPayloadSize string

	contract          string
	executionAmount   int64
	executionGasLimit uint64
	executionGasPrice int64

	actionType    string
	retryNum      uint64
	retryInterval time.Duration
	duration      time.Duration
	resetInterval time.Duration
	aps           int
	arg1          int
	workers       uint64
	checkReceipt  bool
	insecure      bool

	randAccounts    int
	loadTokenAmount string
}{}

var injectCfg = struct {
	configPath       string
	chainID          uint32
	serverAddr       string
	transferGasLimit uint64
	transferGasPrice *big.Int
	transferAmount   *big.Int

	contract          string
	executionAmount   *big.Int
	executionGasLimit uint64
	executionGasPrice *big.Int

	actionType      string
	retryNum        uint64
	retryInterval   time.Duration
	duration        time.Duration
	resetInterval   time.Duration
	aps             int
	arg1            int
	workers         uint64
	checkReceipt    bool
	insecure        bool
	randAccounts    int
	loadTokenAmount *big.Int
}{}

func inject(_ []string) string {
	transferAmount := big.NewInt(rawInjectCfg.transferAmount)
	transferGasPrice := big.NewInt(rawInjectCfg.transferGasPrice)
	executionGasPrice := big.NewInt(rawInjectCfg.executionGasPrice)
	executionAmount := big.NewInt(rawInjectCfg.executionAmount)
	loadTokenAmount, ok := new(big.Int).SetString(rawInjectCfg.loadTokenAmount, 10)
	if !ok {
		return fmt.Sprint("failed to load token amount")
	}

	injectCfg.configPath = rawInjectCfg.configPath
	injectCfg.serverAddr = rawInjectCfg.serverAddr
	injectCfg.chainID = rawInjectCfg.chainID
	injectCfg.transferGasLimit = rawInjectCfg.transferGasLimit
	injectCfg.transferGasPrice = transferGasPrice
	injectCfg.transferAmount = transferAmount

	injectCfg.contract = rawInjectCfg.contract
	injectCfg.executionAmount = executionAmount
	injectCfg.executionGasLimit = rawInjectCfg.executionGasLimit
	injectCfg.executionGasPrice = executionGasPrice

	injectCfg.actionType = rawInjectCfg.actionType
	injectCfg.retryNum = rawInjectCfg.retryNum
	injectCfg.retryInterval = rawInjectCfg.retryInterval
	injectCfg.duration = rawInjectCfg.duration
	injectCfg.resetInterval = rawInjectCfg.resetInterval
	injectCfg.aps = rawInjectCfg.aps
	injectCfg.arg1 = rawInjectCfg.arg1
	injectCfg.workers = rawInjectCfg.workers
	injectCfg.checkReceipt = rawInjectCfg.checkReceipt
	injectCfg.insecure = rawInjectCfg.insecure
	injectCfg.randAccounts = rawInjectCfg.randAccounts
	injectCfg.loadTokenAmount = loadTokenAmount

	p, err := newInjectionProcessor()
	if err != nil {
		return fmt.Sprintf("failed to create injector processor: %v.", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), injectCfg.duration)
	defer cancel()
	go p.injectProcess(ctx)
	go p.syncNoncesProcess(ctx)
	<-ctx.Done()
	return ""
}

func init() {
	flag := injectCmd.Flags()
	flag.StringVar(&rawInjectCfg.configPath, "injector-config-path", "./tools/actioninjector.v2/gentsfaddrs.yaml",
		"path of config file of genesis transfer addresses")
	flag.StringVar(&rawInjectCfg.serverAddr, "addr", "api.testnet.iotex.one:443", "target ip:port for grpc connection")
	flag.Uint32Var(&rawInjectCfg.chainID, "chain-id", 2, "chain id")
	flag.Int64Var(&rawInjectCfg.transferAmount, "transfer-amount", 0, "execution amount")
	flag.Uint64Var(&rawInjectCfg.transferGasLimit, "transfer-gas-limit", 20000, "transfer gas limit")
	flag.Int64Var(&rawInjectCfg.transferGasPrice, "transfer-gas-price", 1000000000000, "transfer gas price")
	flag.StringVar(&rawInjectCfg.transferPayloadSize, "transfer-payload-size", "0", "transfer payload size")
	flag.StringVar(&rawInjectCfg.contract, "contract", "io1pmjhyksxmz2xpxn2qmz4gx9qq2kn2gdr8un4xq", "smart contract address")
	flag.Int64Var(&rawInjectCfg.executionAmount, "execution-amount", 0, "execution amount")
	flag.Uint64Var(&rawInjectCfg.executionGasLimit, "execution-gas-limit", 100000, "execution gas limit")
	flag.Int64Var(&rawInjectCfg.executionGasPrice, "execution-gas-price", 1000000000000, "execution gas price")
	flag.StringVar(&rawInjectCfg.actionType, "action-type", "transfer", "action type to inject")
	flag.Uint64Var(&rawInjectCfg.retryNum, "retry-num", 5, "maximum number of rpc retries")
	flag.DurationVar(&rawInjectCfg.retryInterval, "retry-interval", 1*time.Second, "sleep interval between two consecutive rpc retries")
	flag.DurationVar(&rawInjectCfg.duration, "duration", 60*time.Hour, "duration when the injection will run")
	flag.DurationVar(&rawInjectCfg.resetInterval, "reset-interval", 10*time.Second, "time interval to reset nonce counter")
	flag.IntVar(&rawInjectCfg.aps, "aps", 30, "actions to be injected per second")
	flag.IntVar(&rawInjectCfg.arg1, "arg1", 100, "contract arg1")
	flag.IntVar(&rawInjectCfg.randAccounts, "rand-accounts", 20, "number of accounst to use")
	flag.Uint64Var(&rawInjectCfg.workers, "workers", 10, "number of workers")
	flag.BoolVar(&rawInjectCfg.insecure, "insecure", false, "insecure network")
	flag.BoolVar(&rawInjectCfg.checkReceipt, "check-recipt", false, "check recept")
	flag.StringVar(&rawInjectCfg.loadTokenAmount, "load-token-amount", "0", "init load how much token to inject accounts")
	rootCmd.AddCommand(injectCmd)
}
