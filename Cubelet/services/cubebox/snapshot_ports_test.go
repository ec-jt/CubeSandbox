package cubebox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	networkruntime "github.com/tencentcloud/CubeSandbox/Cubelet/network/runtime"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

type snapshotPortsFake struct {
	response *networkruntime.EnsureNetworkResponse
	err      error
	calls    int
}

func (f *snapshotPortsFake) CaptureSnapshotPorts(context.Context, string) (*networkruntime.EnsureNetworkResponse, error) {
	f.calls++
	return f.response, f.err
}

func TestSnapshotPortsCapturePackageRoundTrip(t *testing.T) {
	sb := &cubeboxstore.CubeBox{Metadata: cubeboxstore.Metadata{ID: "sb-ports"}, NetworkType: "tap", ExposedPorts: []int64{80}, DynamicPorts: []int32{5000}}
	runtime := &snapshotPortsFake{response: &networkruntime.EnsureNetworkResponse{SandboxID: sb.ID,
		PortMappings:    []networkruntime.PortMapping{{ContainerPort: 80, HostPort: 20080}, {ContainerPort: 3000, HostPort: 23000}, {ContainerPort: 4000, HostPort: 24000}},
		PersistMetadata: map[string]string{networkruntime.DynamicPortsMetadataKey: "[3000]", networkruntime.DynamicInfraPortsMetadataKey: "[4000]"}}}
	syncs := 0
	ports, err := captureSnapshotPorts(context.Background(), sb, runtime, func() error { syncs++; return nil })
	require.NoError(t, err)
	require.Equal(t, 1, syncs)
	require.Equal(t, []int32{3000}, sb.DynamicPorts)
	require.Equal(t, []int32{4000}, sb.DynamicInfraPorts)
	require.Len(t, sb.LivePortMappings(), 3)
	dir := t.TempDir()
	require.NoError(t, writeSnapshotPorts(dir, ports))
	raw, err := os.ReadFile(filepath.Join(dir, snapshotPortsFileName))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "23000")
	require.NotContains(t, string(raw), "5000")
	packed, err := loadSnapshotPorts(dir)
	require.NoError(t, err)
	thin := &cubebox.RunCubeSandboxRequest{NetworkType: "tap", ExposedPorts: []int64{80, 5000}, Annotations: map[string]string{networkruntime.DynamicPortsMetadataKey: "[80]"}}
	ctx, err := applySnapshotPorts(context.Background(), thin, packed)
	require.NoError(t, err)
	require.Equal(t, []int64{80, 3000, 4000}, thin.ExposedPorts)
	desired := &networkruntime.EnsureNetworkRequest{}
	require.NoError(t, networkruntime.RestoredPorts(ctx).ApplyTo(desired))
	require.Equal(t, "[3000]", desired.PersistMetadata[networkruntime.DynamicPortsMetadataKey])
	for _, p := range desired.PortMappings {
		require.Zero(t, p.HostPort)
	}
}

func TestSnapshotPortsCaptureFailureBarrierAndLegacy(t *testing.T) {
	sb := &cubeboxstore.CubeBox{Metadata: cubeboxstore.Metadata{ID: "ports"}, NetworkType: "tap"}
	runtime := &snapshotPortsFake{err: errors.New("pending")}
	syncs := 0
	sync := func() error { syncs++; return errors.New("sync failure") }
	_, err := captureSnapshotPorts(context.Background(), sb, runtime, sync)
	require.ErrorContains(t, err, "pending")
	require.Zero(t, syncs)
	runtime.err = nil
	runtime.response = &networkruntime.EnsureNetworkResponse{SandboxID: sb.ID, PortMappings: []networkruntime.PortMapping{{ContainerPort: 3000}}, PersistMetadata: map[string]string{networkruntime.DynamicPortsMetadataKey: "[3000]"}}
	_, err = captureSnapshotPorts(context.Background(), sb, runtime, sync)
	require.ErrorContains(t, err, "sync failure")
	require.Equal(t, []int32{3000}, sb.DynamicPorts) // Retain committed state for repair.
	runtime.response = nil
	_, err = captureSnapshotPorts(context.Background(), sb, runtime, sync)
	require.ErrorContains(t, err, "missing runtime")
	sb.SetLivePorts(nil, nil, nil)
	ports, err := captureSnapshotPorts(context.Background(), sb, runtime, sync)
	require.NoError(t, err)
	require.Nil(t, ports)
	sb.NetworkType = "host"
	runtime.err = errors.New("must not call")
	ports, err = captureSnapshotPorts(context.Background(), sb, runtime, sync)
	require.NoError(t, err)
	require.Nil(t, ports)
}

func TestSnapshotPortsEmptyLegacyAndInvalid(t *testing.T) {
	dir := t.TempDir()
	legacy, err := loadSnapshotPorts(dir)
	require.NoError(t, err)
	require.Nil(t, legacy)
	req := &cubebox.RunCubeSandboxRequest{NetworkType: "tap", ExposedPorts: []int64{80}}
	_, err = applySnapshotPorts(context.Background(), req, legacy)
	require.NoError(t, err)
	require.Equal(t, []int64{80}, req.ExposedPorts)
	empty := &networkruntime.PortablePorts{Version: 1, Ports: []networkruntime.PortablePort{}}
	require.NoError(t, writeSnapshotPorts(dir, empty))
	ports, err := loadSnapshotPorts(dir)
	require.NoError(t, err)
	require.NotNil(t, ports)
	_, err = applySnapshotPorts(context.Background(), req, ports)
	require.NoError(t, err)
	require.Empty(t, req.ExposedPorts)
	for _, raw := range []string{`null`, `{}`, `{"version":2,"ports":[]}`, `{"version":1,"ports":null}`, `{"version":1,"ports":[],"host_port":123}`, `{"version":1,"ports":[]} {}`, `{"version":1,"ports":[{"container_port":80,"class":"static"},{"container_port":80,"class":"user"}]}`} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, snapshotPortsFileName), []byte(raw), 0600))
		_, err := loadSnapshotPorts(dir)
		require.Error(t, err, raw)
	}
	require.NoError(t, writeSnapshotPorts(dir, nil))
	legacy, err = loadSnapshotPorts(dir)
	require.NoError(t, err)
	require.Nil(t, legacy)
}

func TestSnapshotPortsPauseAndCommitRestoreSelection(t *testing.T) {
	for _, kind := range []string{storage.CatalogKindPauseSnapshot, storage.CatalogKindRuntimeSnapshot} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			id := "snap-ports-" + kind
			require.NoError(t, storage.WriteSnapshotCatalogFor("xfs", &storage.SnapshotCatalogEntry{SnapshotID: id, SnapshotPath: dir, MetaDir: dir, Kind: kind}))
			t.Cleanup(func() { storage.DeleteSnapshotCatalogFor("xfs", id) })
			req := &cubebox.RunCubeSandboxRequest{InstanceType: "cubebox", NetworkType: "tap", ExposedPorts: []int64{80, 3000}, Annotations: map[string]string{constants.MasterAnnotationStorageBackend: "xfs", constants.MasterAnnotationRuntimeSnapshotID: id}}
			if kind == storage.CatalogKindPauseSnapshot {
				req.Annotations[constants.MasterAnnotationPauseSnapshotID] = id
				packed := &cubebox.RunCubeSandboxRequest{InstanceType: "cubebox", NetworkType: "tap", Containers: []*cubebox.ContainerConfig{{Id: "container"}}, Annotations: map[string]string{constants.MasterAnnotationAppSnapshotTemplateID: "tpl-original"}}
				require.NoError(t, writePauseSandboxSpec(dir, packed))
				require.NoError(t, expandPauseSnapshotPackage(req))
				require.Equal(t, []int64{80, 3000}, req.ExposedPorts) // Legacy thin fallback still supported.
			}
			ctx, err := restoreSnapshotPorts(context.Background(), req)
			require.NoError(t, err)
			require.Nil(t, networkruntime.RestoredPorts(ctx))
			require.NoError(t, writeSnapshotPorts(dir, &networkruntime.PortablePorts{Version: 1, Ports: []networkruntime.PortablePort{}}))
			ctx, err = restoreSnapshotPorts(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, networkruntime.RestoredPorts(ctx))
			require.Empty(t, req.ExposedPorts)
		})
	}
}

func TestSnapshotPortsRequestAnnotationsCannotGrantCapability(t *testing.T) {
	req := &cubebox.RunCubeSandboxRequest{InstanceType: "cubebox", NetworkType: "tap", ExposedPorts: []int64{17300}, Annotations: map[string]string{
		networkruntime.DynamicPortsMetadataKey: "[17300]", networkruntime.DynamicInfraPortsMetadataKey: "[80]", snapshotPortsFileName: `{"version":1,"ports":[{"container_port":17300,"class":"user"}]}`,
	}}
	ctx, err := restoreSnapshotPorts(context.Background(), req)
	require.NoError(t, err)
	require.Nil(t, networkruntime.RestoredPorts(ctx))
	require.Equal(t, []int64{17300}, req.ExposedPorts)
}

func TestSnapshotPortsCreateStoreUsesRuntimeResponse(t *testing.T) {
	sb := &cubeboxstore.CubeBox{Metadata: cubeboxstore.Metadata{ID: "restored"}}
	response := &networkruntime.EnsureNetworkResponse{SandboxID: "restored", PortMappings: []networkruntime.PortMapping{{ContainerPort: 80, HostPort: 20080}, {ContainerPort: 3000, HostPort: 23000}, {ContainerPort: 4000, HostPort: 24000}}, PersistMetadata: map[string]string{networkruntime.DynamicPortsMetadataKey: "[3000]", networkruntime.DynamicInfraPortsMetadataKey: "[4000]"}}
	_, err := setNetworkPortState(sb, response)
	require.NoError(t, err)
	require.Equal(t, response.PortMappings[1].HostPort, sb.LivePortMappings()[1].HostPort)
	require.Equal(t, []int32{3000}, sb.DynamicPorts)
	require.Equal(t, []int32{4000}, sb.DynamicInfraPorts)
	response.SandboxID = "other"
	_, err = setNetworkPortState(sb, response)
	require.Error(t, err)
}
