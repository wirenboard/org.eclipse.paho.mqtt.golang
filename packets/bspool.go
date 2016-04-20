package packets

import (
	"sync"
)

const (
	MAX_SLICE_SIZE    = 256
	MAX_POOLED_SLICES = 256
)

var byteSlicePools [MAX_SLICE_SIZE + 1]sync.Pool

type sliceHolder struct {
	slice []byte
}

type ByteSlicePool struct {
	pooledSlices    [MAX_POOLED_SLICES]*sliceHolder
	numPooledSlices int
}

var objectPools = sync.Pool{
	New: func() interface{} { return &ByteSlicePool{} },
}

// var rmmePool ByteSlicePool

func getObjectPool() *ByteSlicePool {
	return objectPools.Get().(*ByteSlicePool)
	// return &rmmePool
}

func (pool *ByteSlicePool) getByteSlice(size int) []byte {
	if size > MAX_SLICE_SIZE {
		return make([]byte, size)
	}
	if pool.numPooledSlices == MAX_POOLED_SLICES {
		return make([]byte, size)
	}
	sliceObj := byteSlicePools[size].Get()
	var holder *sliceHolder
	if sliceObj != nil {
		holder = sliceObj.(*sliceHolder)
	} else {
		holder = &sliceHolder{make([]byte, size)}
	}
	pool.pooledSlices[pool.numPooledSlices] = holder
	pool.numPooledSlices += 1
	return holder.slice
}

func (pool *ByteSlicePool) Release() {
	for i := 0; i < pool.numPooledSlices; i += 1 {
		holder := pool.pooledSlices[i]
		byteSlicePools[len(holder.slice)].Put(holder)
	}
	pool.numPooledSlices = 0
	objectPools.Put(pool)
}
