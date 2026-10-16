package cubelet

import (
	"context"
	"errors"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	grpcpool "github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/grpc-middleware/pool"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The existing worker pool dials with its own background timeout. These rare
// control operations use a short-lived connection to keep connection setup and
// the RPC within ONE caller deadline, without changing pause/resume semantics.
func dialPortRPC(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("port RPC requires a deadline")
	}
	return grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(),
		grpc.WithInitialWindowSize(grpcpool.InitialWindowSize),
		grpc.WithInitialConnWindowSize(grpcpool.InitialConnWindowSize),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(grpcpool.MaxSendMsgSize), grpc.MaxCallRecvMsgSize(grpcpool.MaxRecvMsgSize)),
		grpc.WithUserAgent("cubemaster-"+constants.GetUA(ctx)))
}

// UpdateWithContext keeps the caller's deadline, including connection acquisition.
// Unlike the legacy helper it never replaces ctx with context.Background().
func UpdateWithContext(ctx context.Context, endpoint string, req *cubebox.UpdateCubeSandboxRequest) (*cubebox.UpdateCubeSandboxResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("update requires a deadline")
	}
	conn, err := dialPortRPC(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return cubebox.NewCubeboxMgrClient(conn).Update(ctx, req)
}

// ListWithContext queries the node within the caller's remaining RPC budget.
func ListWithContext(ctx context.Context, endpoint string, req *cubebox.ListCubeSandboxRequest) (*cubebox.ListCubeSandboxResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("list requires a deadline")
	}
	conn, err := dialPortRPC(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return cubebox.NewCubeboxMgrClient(conn).List(ctx, req)
}
