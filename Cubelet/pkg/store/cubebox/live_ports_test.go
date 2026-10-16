// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/cube/multimeta"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

func TestLivePortsDeepCopyConcurrent(t *testing.T) {
	cb := &CubeBox{}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			cb.SetLivePorts([]*cubebox.PortMapping{{ContainerPort: 3000, HostPort: 20000}}, []int32{3000}, []int32{4000})
		}()
		go func() {
			defer wg.Done()
			_ = cb.DeepCopy()
			_ = cb.LivePortMappings()
		}()
	}
	wg.Wait()
	copy := cb.DeepCopy()
	copy.PortMappings[0].HostPort = 1
	copy.DynamicPorts[0] = 1
	copy.DynamicInfraPorts[0] = 1
	require.Equal(t, int32(20000), cb.LivePortMappings()[0].HostPort)
	require.Equal(t, []int32{3000}, cb.DynamicPorts)
	require.Equal(t, []int32{4000}, cb.DynamicInfraPorts)
}

type orderedPortDB struct {
	multimeta.MetadataDBAPI
	entered      chan int
	firstRelease chan struct{}
	mu           sync.Mutex
	calls        int
	last         []byte
}

func (db *orderedPortDB) SetWithTx(_, _ string, value []byte, _ func() error) error {
	db.mu.Lock()
	db.calls++
	call := db.calls
	db.mu.Unlock()
	db.entered <- call
	if call == 1 {
		<-db.firstRelease
	}
	db.mu.Lock()
	db.last = append([]byte(nil), value...)
	db.mu.Unlock()
	return nil
}

func TestLivePortsStoreSyncCannotWriteStaleSnapshotLast(t *testing.T) {
	db := &orderedPortDB{entered: make(chan int, 2), firstRelease: make(chan struct{})}
	store := NewStore(db)
	cb := &CubeBox{Metadata: Metadata{ID: "ports"}}
	store.Add(cb)
	done := make(chan error, 2)
	go func() { done <- store.Sync(cb.ID) }()
	require.Equal(t, 1, <-db.entered)
	cb.SetLivePorts([]*cubebox.PortMapping{{ContainerPort: 3000, HostPort: 20000}}, []int32{3000}, nil)
	go func() { done <- store.Sync(cb.ID) }()
	select {
	case <-db.entered:
		t.Fatal("new sync passed the pending older write")
	case <-time.After(30 * time.Millisecond):
	}
	close(db.firstRelease)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	var recovered CubeBox
	require.NoError(t, json.Unmarshal(db.last, &recovered))
	require.Equal(t, []int32{3000}, recovered.DynamicPorts)
	require.Len(t, recovered.PortMappings, 1)
}
