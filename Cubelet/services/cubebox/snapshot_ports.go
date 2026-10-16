// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/tencentcloud/CubeSandbox/Cubelet/network"
	networkruntime "github.com/tencentcloud/CubeSandbox/Cubelet/network/runtime"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/pathutil"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

const snapshotPortsFileName = "sandbox_ports.json"

type snapshotPortReader interface {
	CaptureSnapshotPorts(context.Context, string) (*networkruntime.EnsureNetworkResponse, error)
}

// Called while Pause/Commit holds sandboxLifecycleLocks, before capturing VM
// state. Runtime wins over both mirrors; failures stop the snapshot, not revert
// committed runtime operations. No-allocation legacy is safe only without any
// recorded live or dynamic mappings.
func captureSnapshotPorts(ctx context.Context, sb *cubeboxstore.CubeBox, reader snapshotPortReader, sync func() error) (*networkruntime.PortablePorts, error) {
	if sb.NetworkType != cubebox.NetworkType_tap.String() {
		return nil, nil
	}
	response, err := reader.CaptureSnapshotPorts(ctx, sb.ID)
	if err != nil {
		return nil, err
	}
	if response == nil {
		if len(sb.LivePortMappings()) != 0 || len(sb.DynamicPorts) != 0 || len(sb.DynamicInfraPorts) != 0 {
			return nil, fmt.Errorf("missing runtime/allocation for recorded live ports")
		}
		return nil, nil
	}
	if response.SandboxID != sb.ID {
		return nil, fmt.Errorf("port capture sandbox identity mismatch")
	}
	ports, err := setNetworkPortState(sb, response)
	if err != nil {
		return nil, err
	}
	if err := sync(); err != nil {
		return nil, fmt.Errorf("persist captured sandbox ports: %w", err)
	}
	return ports, nil
}

// setNetworkPortState copies runtime output, not requested intent. This matters
// on idempotent Create: Ensure may return a previously committed network.
func setNetworkPortState(sb *cubeboxstore.CubeBox, response *networkruntime.EnsureNetworkResponse) (*networkruntime.PortablePorts, error) {
	if response == nil || response.SandboxID != sb.ID {
		return nil, fmt.Errorf("network state sandbox identity mismatch")
	}
	ports, err := networkruntime.PortablePortsFromNetwork(response)
	if err != nil {
		return nil, err
	}
	mappings := make([]*cubebox.PortMapping, 0, len(response.PortMappings))
	for _, p := range response.PortMappings {
		mappings = append(mappings, &cubebox.PortMapping{ContainerPort: p.ContainerPort, HostPort: p.HostPort})
	}
	sb.SetLivePorts(mappings, ports.ClassifiedPorts("user"), ports.ClassifiedPorts("infrastructure"))
	return ports, nil
}

func (s *service) captureSnapshotPorts(ctx context.Context, sb *cubeboxstore.CubeBox) (*networkruntime.PortablePorts, error) {
	return captureSnapshotPorts(ctx, sb, network.DynamicPorts{}, func() error {
		return s.cubeboxMgr.cubeboxManger.SyncByID(ctx, sb.ID)
	})
}

func writeSnapshotPorts(dir string, ports *networkruntime.PortablePorts) error {
	path := filepath.Join(dir, snapshotPortsFileName)
	if ports == nil {
		// A reused/cloned metadata directory must not inherit another snapshot's record.
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if err := ports.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(ports)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path+".tmp", body, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

func loadSnapshotPorts(dir string) (*networkruntime.PortablePorts, error) {
	f, err := os.Open(filepath.Join(dir, snapshotPortsFileName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 8*1024*1024))
	decoder.DisallowUnknownFields()
	var ports networkruntime.PortablePorts
	if err := decoder.Decode(&ports); err != nil {
		return nil, fmt.Errorf("decode snapshot ports: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("trailing snapshot port data")
	}
	if err := ports.Validate(); err != nil {
		return nil, err
	}
	return &ports, nil
}

// Only a package read can install this context capability. An annotation with
// the same-looking name, or cube.dynamic_* metadata, is never consulted.
func applySnapshotPorts(ctx context.Context, req *cubebox.RunCubeSandboxRequest, ports *networkruntime.PortablePorts) (context.Context, error) {
	if ports == nil {
		return ctx, nil
	}
	if req.GetNetworkType() != cubebox.NetworkType_tap.String() {
		return ctx, fmt.Errorf("snapshot port intent requires tap network")
	}
	next, err := networkruntime.WithRestoredPorts(ctx, ports)
	if err != nil {
		return ctx, err
	}
	req.ExposedPorts = ports.ExposedPorts() // Includes authoritative empty, overriding thin defaults.
	return next, nil
}

// Pause and FromSnap Create both read the metadata directory selected for the
// actual VM restore. Templates retain legacy behavior when no record is present.
func restoreSnapshotPorts(ctx context.Context, req *cubebox.RunCubeSandboxRequest) (context.Context, error) {
	flow := &workflow.CreateContext{ReqInfo: req}
	if !flow.IsRetoreSnapshot() {
		return ctx, nil
	}
	id, ok := flow.GetSnapshotTemplateID()
	if !ok {
		return ctx, nil
	}
	if err := pathutil.ValidateSafeID(id); err != nil {
		return ctx, err
	}
	backend, err := storageBackendFromAnnotations(req.GetAnnotations())
	if err != nil {
		return ctx, err
	}
	var dir string
	if imp := storage.CrossNodeSandboxImport(req.GetAnnotations()); imp != nil {
		dir, err = imp.EnsureMetadata(ctx)
	} else {
		var entry *storage.SnapshotCatalogEntry
		entry, err = storage.GetLocalSnapshotFor(ctx, backend, id)
		if err == nil {
			dir = pauseSpecDir(entry)
			err = storage.MountS3MetadataAt(ctx, backend, id, dir)
		} else if errors.Is(err, storage.ErrSnapshotCatalogNotFound) && strings.HasPrefix(id, "tpl-") && strings.TrimSpace(req.GetAnnotations()[constants.MasterAnnotationRuntimeSnapshotID]) == "" && strings.TrimSpace(req.GetAnnotations()[constants.MasterAnnotationPauseSnapshotID]) == "" {
			// Historical template create can use unpacked templates without a catalog.
			return ctx, nil
		}
	}
	if err != nil {
		return ctx, fmt.Errorf("resolve snapshot port metadata: %w", err)
	}
	ports, err := loadSnapshotPorts(dir)
	if err != nil {
		return ctx, err
	}
	return applySnapshotPorts(ctx, req, ports)
}
