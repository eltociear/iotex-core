// Copyright (c) 2024 IoTeX Foundation
// This source code is provided 'as is' and no warranties are given as to title or non-infringement, merchantability
// or fitness for purpose and, to the extent permitted by law, all liability for your use of the code is disclaimed.
// This source code is governed by Apache License 2.0 that can be found in the LICENSE file.

package blockdao

import (
	"context"

	"github.com/iotexproject/iotex-core/pkg/util/byteutil"
)

type blobIndexer struct {
	blockPerBucket          uint64
	totalBucket, currBucket uint16
	startBlock, currBlock   uint64
}

func (bs *blobIndexer) Start(ctx context.Context) error {
	// TODO
	return nil
}

func (bs *blobIndexer) Stop(ctx context.Context) error {
	// TODO
	return nil
}

func (bs *blobIndexer) bucketForPut(height uint64) (string, bool) {
	var (
		nextBucket bool
	)
	if height == bs.startBlock+bs.blockPerBucket {
		// need to move to next bucket
		bs.currBucket++
		bs.currBucket %= bs.totalBucket
		bs.startBlock = height
		nextBucket = true
	}
	return string(byteutil.Uint16ToBytesBigEndian(bs.currBucket)), nextBucket
}

func (bs *blobIndexer) bucketForGet(height uint64) (string, error) {
	// TODO
	return "", nil
}

func (bs *blobIndexer) indexByActionHash(hash []byte) (string, []byte, error) {
	// TODO
	return "", nil, nil
}
