// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/stretchr/testify/require"
)

type fakeExecDeleter struct {
	deletes atomic.Int32
	err     error
}

func (f *fakeExecDeleter) Delete(context.Context, ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	f.deletes.Add(1)
	return nil, f.err
}

func TestReapExecProcessDeletesAfterExit(t *testing.T) {
	ch := make(chan containerd.ExitStatus, 1)
	p := &fakeExecDeleter{}
	done := make(chan struct{})
	go func() { reapExecProcess(context.Background(), "e1", ch, p); close(done) }()
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, p.deletes.Load(), "must not delete a running exec")
	ch <- containerd.ExitStatus{}
	<-done
	require.EqualValues(t, 1, p.deletes.Load())
}

func TestReapExecProcessDeleteErrorIsNotFatal(t *testing.T) {
	ch := make(chan containerd.ExitStatus, 1)
	ch <- containerd.ExitStatus{}
	p := &fakeExecDeleter{err: errors.New("gone")}
	reapExecProcess(context.Background(), "e2", ch, p)
	require.EqualValues(t, 1, p.deletes.Load())
}

func TestReapExecProcessGivesUpOnLongRunningExec(t *testing.T) {
	old := execReapMaxWait
	execReapMaxWait = 10 * time.Millisecond
	defer func() { execReapMaxWait = old }()
	p := &fakeExecDeleter{}
	reapExecProcess(context.Background(), "e3", make(chan containerd.ExitStatus), p)
	require.Zero(t, p.deletes.Load())
}
