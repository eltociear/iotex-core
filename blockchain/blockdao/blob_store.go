// Copyright (c) 2024 IoTeX Foundation
// This source code is provided 'as is' and no warranties are given as to title or non-infringement, merchantability
// or fitness for purpose and, to the extent permitted by law, all liability for your use of the code is disclaimed.
// This source code is governed by Apache License 2.0 that can be found in the LICENSE file.

package blockdao

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/iotexproject/go-pkgs/byteutil"
	"github.com/iotexproject/go-pkgs/hash"
	"github.com/iotexproject/iotex-proto/golang/iotextypes"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/iotexproject/iotex-core/action"
	"github.com/iotexproject/iotex-core/blockchain/block"
	"github.com/iotexproject/iotex-core/db"
)

type (
	BlobStore interface {
		Start(context.Context) error
		Stop(context.Context) error
		GetBlob(hash.Hash256) (*types.BlobTxSidecar, error)
		GetBlobsByHeight(uint64) ([]*types.BlobTxSidecar, error)
		PutBlob(*block.Block) error
	}

	// storage for past N-day's blobs, structured as blow:
	// 1. Storage is divided by hour, with each hour's data stored in a bucket.
	//    By default N = 18 so we have 432 buckets, plus 1 extra bucket. At each
	//    new hour, the oldest bucket is found and all data inside it deleted
	//    (so we still have 18 days after deletion), and blobs in this hour are
	//    stored into it
	// 2. Inside each bucket, block height is used as the key to store the blobs
	//    in this block. Maximum number of blobs is 6 for each block, and maximum
	//    data size in the bucket is 131kB x 6 x 720 = 566MB. Maximum total size
	//    of the entire storage is 566MB x 433 = 245GB.
	// 3. A mapping of action hash --> (bucket, block) is maintained in a separate
	//    bucket as the storage index. It converts the requested blob hash to the
	//    bucket and block number, and retrieves the blob object from storage
	blobStore struct {
		kvStore db.KVStoreBasic
		indexer *blobIndexer
	}
)

func (bs *blobStore) Start(ctx context.Context) error {
	// TODO
	return bs.indexer.Start(ctx)
}

func (bs *blobStore) Stop(ctx context.Context) error {
	// TODO
	if err := bs.indexer.Stop(ctx); err != nil {
		return err
	}
	return nil
}

func (bs *blobStore) GetBlob(h hash.Hash256) (*types.BlobTxSidecar, error) {
	bucket, block, err := bs.indexer.indexByActionHash(h[:])
	if err != nil {
		return nil, err
	}
	blobs, err := bs.getBlobs(bucket, block)
	if err != nil {
		return nil, err
	}
	for _, b := range blobs {
		for _, hash := range b.BlobHashes() {
			if hash == common.BytesToHash(h[:]) {
				return b, nil
			}
		}
	}
	return nil, errors.Wrapf(db.ErrNotExist, "blob hash = %x", h)
}

func (bs *blobStore) GetBlobsByHeight(height uint64) ([]*types.BlobTxSidecar, error) {
	bucket, err := bs.indexer.bucketForGet(height)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get blobs at block = %d", height)
	}
	return bs.getBlobs(bucket, byteutil.Uint64ToBytesBigEndian(height))
}

func (bs *blobStore) PutBlob(blk *block.Block) error {
	blobs := make([]*types.BlobTxSidecar, 0)
	for _, act := range blk.Actions {
		if b := act.BlobTxSidecar(); b != nil {
			blobs = append(blobs, b)
		}
	}
	if len(blobs) == 0 {
		return nil
	}
	pb := iotextypes.BlobTxSidecars{
		BlobTxSidecars: make([]*iotextypes.BlobTxSidecar, len(blobs)),
	}
	for i := range blobs {
		pb.BlobTxSidecars[i] = action.ToProtoSideCar(blobs[i])
	}
	raw, err := proto.Marshal(&pb)
	if err != nil {
		return errors.Wrapf(err, "failed to put block = %d", blk.Height())
	}
	var (
		height       = blk.Height()
		bucket, next = bs.indexer.bucketForPut(height)
	)
	if next {
		// clear next bucket
		if err := bs.kvStore.Delete(bucket, nil); err != nil {
			return errors.Wrapf(err, "failed to put block = %d", blk.Height())
		}
	}
	return bs.kvStore.Put(bucket, byteutil.Uint64ToBytesBigEndian(height), raw)
}

func (bs *blobStore) getBlobs(bucket string, block []byte) ([]*types.BlobTxSidecar, error) {
	raw, err := bs.kvStore.Get(bucket, block)
	if err != nil {
		return nil, err
	}
	pb := iotextypes.BlobTxSidecars{}
	if err := proto.Unmarshal(raw, &pb); err != nil {
		return nil, errors.Wrapf(err, "failed to get blobs from block = %d", byteutil.BytesToUint64BigEndian(block))
	}
	var (
		pbBlobs = pb.GetBlobTxSidecars()
		blobs   = make([]*types.BlobTxSidecar, len(pbBlobs))
	)
	for i := range pbBlobs {
		blobs[i], err = action.FromProtoBlobTxSideCar(pbBlobs[i])
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get blobs from block = %d", byteutil.BytesToUint64BigEndian(block))
		}
	}
	return blobs, nil
}
