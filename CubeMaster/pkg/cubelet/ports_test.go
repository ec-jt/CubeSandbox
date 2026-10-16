package cubelet

import (
	"context"
	"net"
	"testing"
	"time"

	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"google.golang.org/grpc"
)

type blockedPortServer struct {
	cubebox.UnimplementedCubeboxMgrServer
}

func (*blockedPortServer) Update(ctx context.Context, _ *cubebox.UpdateCubeSandboxRequest) (*cubebox.UpdateCubeSandboxResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (*blockedPortServer) List(ctx context.Context, _ *cubebox.ListCubeSandboxRequest) (*cubebox.ListCubeSandboxResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestPortRPCDeadlineIncludesDialAndRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	cubebox.RegisterCubeboxMgrServer(server, &blockedPortServer{})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	// A listening socket that never speaks HTTP/2 exercises blocked dialing.
	stalled, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	for _, endpoint := range []string{listener.Addr().String(), stalled.Addr().String()} {
		for _, action := range []string{"update", "list"} {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			start := time.Now()
			if action == "update" {
				_, err = UpdateWithContext(ctx, endpoint, &cubebox.UpdateCubeSandboxRequest{})
			} else {
				_, err = ListWithContext(ctx, endpoint, &cubebox.ListCubeSandboxRequest{})
			}
			cancel()
			if err == nil || time.Since(start) > time.Second {
				t.Fatalf("%s exceeded caller budget: %v %s", action, err, time.Since(start))
			}
		}
	}
	if _, err := UpdateWithContext(context.Background(), listener.Addr().String(), nil); err == nil {
		t.Fatal("update accepted unbounded context")
	}
	if _, err := ListWithContext(context.Background(), listener.Addr().String(), nil); err == nil {
		t.Fatal("list accepted unbounded context")
	}
}
