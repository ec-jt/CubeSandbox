// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

// B19: waiting for a cross-node import to be local before using it.
//
// import_lvol answers as soon as the destination lvol exists. Until the
// background decouple has materialised every present chunk, the volume reads
// through to the source's S3 objects. s3lvol now fetches those chunks whole
// and in parallel (rcow_import_lvol prefetch=true, the default), and reports
// progress through rcow_import_status. A cross-node restore must not mount the
// metadata disk or hand rootfs/memory to the VM until that status says done:
// otherwise every first guest read can still cost an S3 round trip, which on
// Ceph RGW took 6-28 s and failed the restore (mount killed at 3 s).
//
// The wait is bounded (default 300 s), logs progress every 5 s, and fails
// closed: a prefetch error, a cancelled decouple, or the deadline fails the
// Create, and the B18 rollback (releaseFailedCrossNodeRestore) releases the
// imports.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/internal/tomlext"
	CubeLog "github.com/tencentcloud/CubeSandbox/pkgs/CubeLog"
)

const (
	defaultImportPrefetchTimeout = 300 * time.Second
	defaultImportPrefetchPoll    = 500 * time.Millisecond
	importPrefetchProgressEvery  = 5 * time.Second
	defaultImportedMountTimeout  = 120 * time.Second
)

// Package-level knobs, set from Config during plugin init (see plugin.go) and
// overridable in tests.
var (
	importPrefetchTimeout = defaultImportPrefetchTimeout
	importPrefetchPoll    = defaultImportPrefetchPoll
	importProgressEvery   = importPrefetchProgressEvery
	importedMountTimeout  = defaultImportedMountTimeout
)

// cowImportStatuser is the engine capability behind rcow_import_status.
type cowImportStatuser interface {
	ImportStatus(name string) (string, error)
}

// importStatus mirrors the rcow_import_status reply.
type importStatus struct {
	LvolName      string `json:"lvol_name"`
	ExportUUID    string `json:"export_uuid"`
	State         string `json:"state"`
	Prefetch      bool   `json:"prefetch"`
	PresentChunks uint64 `json:"present_chunks"`
	LocalChunks   uint64 `json:"local_chunks"`
	ChunksFetched uint64 `json:"chunks_fetched"`
	FetchInflight uint32 `json:"fetch_inflight"`
	ClustersTotal uint64 `json:"clusters_total"`
	ClustersDone  uint64 `json:"clusters_done"`
	Done          bool   `json:"done"`
	Error         int32  `json:"error"`
	ErrorMsg      string `json:"error_msg"`
}

func parseImportStatus(raw string) (*importStatus, error) {
	var st importStatus
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		return nil, fmt.Errorf("decode rcow_import_status reply: %w", err)
	}
	return &st, nil
}

// errImportPrefetchTimeout marks a wait that ran out of time.
var errImportPrefetchTimeout = errors.New("import prefetch did not complete in time")

// waitImportPrefetch polls the import status of name until the volume no
// longer reads through to its export, the prefetch reports an error, or the
// deadline passes. role is only used in messages.
func waitImportPrefetch(ctx context.Context, eng cowImportStatuser, name, role string,
	timeout time.Duration) error {
	if eng == nil {
		return fmt.Errorf("wait for %s %s: engine cannot report import status", role, name)
	}
	if timeout <= 0 {
		timeout = defaultImportPrefetchTimeout
	}
	poll := importPrefetchPoll
	if poll <= 0 {
		poll = defaultImportPrefetchPoll
	}
	start := time.Now()
	deadline := start.Add(timeout)
	lastLog := start
	var last *importStatus

	for {
		raw, err := eng.ImportStatus(name)
		if err != nil {
			return fmt.Errorf("import status of %s %s: %w", role, name, err)
		}
		st, err := parseImportStatus(raw)
		if err != nil {
			return fmt.Errorf("import status of %s %s: %w", role, name, err)
		}
		last = st
		if st.Done {
			CubeLog.Infof("cross-node import %s %s is local: %d/%d chunk(s) in %s",
				role, name, st.PresentChunks, st.PresentChunks,
				time.Since(start).Round(time.Millisecond))
			return nil
		}
		switch strings.ToLower(st.State) {
		case "failed", "cancelled":
			msg := st.ErrorMsg
			if msg == "" {
				msg = fmt.Sprintf("errno %d", -st.Error)
			}
			return fmt.Errorf("prefetch of %s %s %s: %s (%d/%d chunk(s) local)",
				role, name, st.State, msg, st.LocalChunks, st.PresentChunks)
		}
		if st.Error != 0 {
			return fmt.Errorf("prefetch of %s %s failed: %s (%d/%d chunk(s) local)",
				role, name, st.ErrorMsg, st.LocalChunks, st.PresentChunks)
		}

		now := time.Now()
		if now.Sub(lastLog) >= importProgressEvery {
			CubeLog.Infof("cross-node import %s %s: %s, %d/%d chunk(s) local, %d fetched, "+
				"%d GET(s) in flight, %s elapsed", role, name, st.State,
				st.LocalChunks, st.PresentChunks, st.ChunksFetched, st.FetchInflight,
				now.Sub(start).Round(time.Second))
			lastLog = now
		}
		if !now.Before(deadline) {
			return fmt.Errorf("%w: %s %s after %s (state %s, %d/%d chunk(s) local)",
				errImportPrefetchTimeout, role, name, timeout, last.State,
				last.LocalChunks, last.PresentChunks)
		}

		wait := poll
		if rem := deadline.Sub(now); rem < wait {
			wait = rem
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %s %s: %w", role, name, ctx.Err())
		case <-time.After(wait):
		}
	}
}

// applyImportTimeouts sets the B19 wait and mount timeouts from config,
// keeping the defaults for zero values.
func applyImportTimeouts(cfg *Config) error {
	if cfg == nil {
		return nil
	}
	if d := tomlext.ToStdTime(cfg.ImportPrefetchTimeout); d < 0 {
		return fmt.Errorf("import_prefetch_timeout must be non-negative, got %v", d)
	} else if d > 0 {
		importPrefetchTimeout = d
	}
	if d := tomlext.ToStdTime(cfg.ImportedMetadataMountTimeout); d < 0 {
		return fmt.Errorf("imported_metadata_mount_timeout must be non-negative, got %v", d)
	} else if d > 0 {
		importedMountTimeout = d
	}
	return nil
}
