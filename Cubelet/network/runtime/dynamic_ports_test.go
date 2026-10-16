package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

// Track both directions, including failures between map writes. No BPF loaded.
type dynamicPortAdapter struct {
	CubeVSAdapter
	local               map[[2]uint32]uint16
	remote              map[uint16][2]uint32
	failAdd, failDelete bool
	wholeDeletes        int
}

func (a *dynamicPortAdapter) AddPortMapping(index uint32, port, host uint16) error {
	key := [2]uint32{index, uint32(port)}
	if old, ok := a.remote[host]; ok && old != key {
		return errors.New("foreign remote tuple")
	}
	if old, ok := a.local[key]; ok && old != host {
		return errors.New("foreign local tuple")
	}
	a.remote[host] = key
	if a.failAdd {
		return errors.New("partial add")
	}
	a.local[key] = host
	return nil
}

func (a *dynamicPortAdapter) DelPortMapping(index uint32, port, host uint16) error {
	key := [2]uint32{index, uint32(port)}
	if a.local[key] == host {
		delete(a.local, key)
	}
	if a.failDelete {
		return errors.New("partial delete")
	}
	if a.remote[host] == key {
		delete(a.remote, host)
	}
	return nil
}

func (a *dynamicPortAdapter) DeletePortMappingsByIfindex(index uint32) error {
	a.wholeDeletes++
	for key, host := range a.local {
		if key[0] == index {
			delete(a.local, key)
			delete(a.remote, host)
		}
	}
	for host, key := range a.remote {
		if key[0] == index {
			delete(a.remote, host)
		}
	}
	return nil
}

func newDynamicTestController(t *testing.T) (*NetworkController, *dynamicPortAdapter) {
	t.Helper()
	c := newCreateTestController(t, nil)
	a := &dynamicPortAdapter{CubeVSAdapter: c.cubevsAdapter, local: map[[2]uint32]uint16{}, remote: map[uint16][2]uint32{}}
	c.cubevsAdapter = a
	_, err := c.EnsureNetwork(context.Background(), &EnsureNetworkRequest{SandboxID: "dynamic", PortMappings: []PortMapping{{Protocol: "tcp", ContainerPort: 80, HostPort: 20080}}, PersistMetadata: map[string]string{"unrelated": "preserved"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.tapAdapter.Close(c.states["dynamic"].tap.File) })
	a.wholeDeletes = 0
	return c, a
}

func assertPortOwned(t *testing.T, c *NetworkController, mapping PortMapping, owned bool) {
	t.Helper()
	_, err := c.ports.Reserve("probe", []PortMapping{mapping}, c.cfg.HostPortBindIP)
	if (err != nil) != owned {
		t.Fatalf("host %d ownership: error=%v want owned=%v", mapping.HostPort, err, owned)
	}
	if err == nil {
		c.ports.ReleaseOwnership("probe")
	}
}

func assertDynamicState(t *testing.T, c *NetworkController) {
	t.Helper()
	state := c.states["dynamic"]
	disk, err := c.store.Load("dynamic", StateFileSuccess)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(disk, &state.persistedState) {
		t.Fatalf("memory and disk differ: disk=%#v memory=%#v", disk, state.persistedState)
	}
	if !reflect.DeepEqual(state.PortMappings, state.tap.PortMappings) {
		t.Fatal("TAP mapping copy differs")
	}
	if state.PendingPort != nil {
		t.Fatal("pending transaction remains")
	}
	if state.PersistMetadata["unrelated"] != "preserved" {
		t.Fatal("unrelated metadata lost")
	}
}

func TestDynamicPortsSuccessIdempotenceAndClassification(t *testing.T) {
	c, a := newDynamicTestController(t)
	ctx := context.Background()
	for _, port := range []int32{3001, 3000} {
		if _, err := c.ExposePort(ctx, "dynamic", port, false); err != nil {
			t.Fatal(err)
		}
	}
	first, err := c.ExposePort(ctx, "dynamic", 4000, true)
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.ExposePort(ctx, "dynamic", 4000, false)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("idempotence: %v", err)
	}
	if again.PersistMetadata[DynamicPortsMetadataKey] != "[3000,3001]" || again.PersistMetadata[DynamicInfraPortsMetadataKey] != "[4000]" {
		t.Fatalf("classification: %#v", again.PersistMetadata)
	}
	for _, port := range []int32{80, 9999} {
		got, err := c.ClosePort(ctx, "dynamic", port)
		if err != nil || !reflect.DeepEqual(got, again) {
			t.Fatalf("static/absent close changed state: %v", err)
		}
	}
	static, err := c.ExposePort(ctx, "dynamic", 80, true)
	if err != nil || !reflect.DeepEqual(static, again) {
		t.Fatal("static expose changed classification")
	}
	removed := first.PortMappings[len(first.PortMappings)-1]
	if _, err := c.ClosePort(ctx, "dynamic", 4000); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ClosePort(ctx, "dynamic", 4000); err != nil {
		t.Fatal(err)
	}
	assertPortOwned(t, c, removed, false)
	for _, p := range c.states["dynamic"].PortMappings {
		assertPortOwned(t, c, p, true)
	}
	if a.wholeDeletes != 0 {
		t.Fatal("dynamic operation used whole-TAP cleanup")
	}
	assertDynamicState(t, c)
}

func TestDynamicPortsRejectInvalidInactiveAndCancelled(t *testing.T) {
	c, a := newDynamicTestController(t)
	for _, port := range []int32{-1, 0, 65536} {
		if _, err := c.ExposePort(context.Background(), "dynamic", port, false); err == nil {
			t.Fatal("invalid expose accepted")
		}
		if _, err := c.ClosePort(context.Background(), "dynamic", port); err == nil {
			t.Fatal("invalid close accepted")
		}
	}
	if _, err := c.ExposePort(context.Background(), "missing", 1234, false); !errors.Is(err, ErrNetworkNotActive) {
		t.Fatalf("inactive: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.ExposePort(ctx, "dynamic", 1234, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if len(a.remote) != 1 {
		t.Fatal("invalid request changed maps")
	}
	assertDynamicState(t, c)
}

func TestDynamicPortsPartialMapFailures(t *testing.T) {
	for _, failedCleanup := range []bool{false, true} {
		t.Run(fmt.Sprint(failedCleanup), func(t *testing.T) {
			c, a := newDynamicTestController(t)
			a.failAdd, a.failDelete = true, failedCleanup
			if _, err := c.ExposePort(context.Background(), "dynamic", 3000, false); err == nil {
				t.Fatal("partial add succeeded")
			}
			state := c.states["dynamic"]
			if failedCleanup {
				if state.PendingPort == nil {
					t.Fatal("failed cleanup lost durable intent")
				}
				assertPortOwned(t, c, *state.PendingPort, true)
			} else if len(a.remote) != 1 {
				t.Fatal("successful rollback left residue")
			}
			a.failAdd, a.failDelete = false, false
			resp, err := c.ExposePort(context.Background(), "dynamic", 3000, false)
			if err != nil {
				t.Fatal(err)
			}
			mapping := resp.PortMappings[1]
			a.failDelete = true
			if _, err := c.ClosePort(context.Background(), "dynamic", 3000); err == nil {
				t.Fatal("partial close succeeded")
			}
			assertPortOwned(t, c, mapping, true)
			a.failDelete = false
			if _, err := c.ClosePort(context.Background(), "dynamic", 3000); err != nil {
				t.Fatal(err)
			}
			assertPortOwned(t, c, mapping, false)
			if len(a.local) != 1 || len(a.remote) != 1 || a.wholeDeletes != 0 {
				t.Fatal("unrelated mapping changed or residue remains")
			}
			assertDynamicState(t, c)
		})
	}
}

// Exercise every durable boundary on both live retry and a fresh controller.
func TestDynamicPortsPersistenceBoundaries(t *testing.T) {
	for _, expose := range []bool{true, false} {
		for _, write := range []int{1, 2} {
			for _, phase := range []string{"before_rename", "after_rename"} {
				for _, restart := range []bool{false, true} {
					t.Run(fmt.Sprintf("expose=%v/write=%d/%s/restart=%v", expose, write, phase, restart), func(t *testing.T) {
						c, a := newDynamicTestController(t)
						ctx := context.Background()
						if !expose {
							if _, err := c.ExposePort(ctx, "dynamic", 3000, true); err != nil {
								t.Fatal(err)
							}
						}
						calls := 0
						c.store.rewriteHook = func(at string) error {
							if at == "before_rename" {
								calls++
							}
							if calls >= write && at == phase {
								return errors.New("injected persistence failure")
							}
							return nil
						}
						var err error
						if expose {
							_, err = c.ExposePort(ctx, "dynamic", 3000, true)
						} else {
							_, err = c.ClosePort(ctx, "dynamic", 3000)
						}
						if err == nil {
							t.Fatal("injected write succeeded")
						}
						pending := *c.states["dynamic"].PendingPort
						assertPortOwned(t, c, pending, true)
						c.store.rewriteHook = nil
						if restart {
							fresh := newRecoverTestController(t)
							fresh.store, fresh.cubevsAdapter = c.store, a
							fresh.tapAdapter.(*fakeTapDeviceAdapter).listResult = map[string]*tapDevice{c.states["dynamic"].TapName: recoverTap(&c.states["dynamic"].persistedState)}
							if err := fresh.recover(); err != nil {
								t.Fatal(err)
							}
							c = fresh
						} else {
							if _, err := c.EnsureNetwork(ctx, &EnsureNetworkRequest{SandboxID: "dynamic"}); err != nil {
								t.Fatal(err)
							}
						}
						// Pre-intent close failure keeps the old mapping. Every durable
						// close intent removes it. Only a committed expose survives.
						wantPresent := (expose && write == 2 && phase == "after_rename") || (!expose && write == 1 && phase == "before_rename")
						if (len(c.states["dynamic"].PortMappings) == 2) != wantPresent {
							t.Fatalf("unexpected recovered mappings: %#v", c.states["dynamic"].PortMappings)
						}
						assertPortOwned(t, c, pending, wantPresent)
						assertDynamicState(t, c)
						if expose {
							_, err = c.ExposePort(ctx, "dynamic", 3000, true)
						} else {
							_, err = c.ClosePort(ctx, "dynamic", 3000)
						}
						if err != nil {
							t.Fatal(err)
						}
						assertDynamicState(t, c)
					})
				}
			}
		}
	}
}

func TestDynamicPortsRecoveryFailureRetainsIntentAndOwnership(t *testing.T) {
	c, a := newDynamicTestController(t)
	a.failAdd, a.failDelete = true, true
	if _, err := c.ExposePort(context.Background(), "dynamic", 3000, false); err == nil {
		t.Fatal("expected failure")
	}
	pending := *c.states["dynamic"].PendingPort
	fresh := newRecoverTestController(t)
	fresh.store, fresh.cubevsAdapter = c.store, a
	fresh.tapAdapter.(*fakeTapDeviceAdapter).listResult = map[string]*tapDevice{c.states["dynamic"].TapName: recoverTap(&c.states["dynamic"].persistedState)}
	if err := fresh.recover(); err == nil {
		t.Fatal("recovery published failed cleanup")
	}
	if fresh.states["dynamic"] != nil {
		t.Fatal("pending network published active")
	}
	assertPortOwned(t, fresh, pending, true)
	disk, err := fresh.store.Load("dynamic", StateFileSuccess)
	if err != nil || disk.PendingPort == nil {
		t.Fatal("recovery lost durable intent")
	}
	a.failAdd, a.failDelete = false, false
	if err := fresh.recover(); err != nil {
		t.Fatal(err)
	}
	assertPortOwned(t, fresh, pending, false)
	assertDynamicState(t, fresh)
}

func TestDynamicPortsUnrelatedOwnershipAndConcurrentRetry(t *testing.T) {
	c, a := newDynamicTestController(t)
	other := PortMapping{Protocol: "tcp", ContainerPort: 5000, HostPort: 20500}
	if _, err := c.ports.Reserve("other", []PortMapping{other}, c.cfg.HostPortBindIP); err != nil {
		t.Fatal(err)
	}
	if err := a.AddPortMapping(999, 5000, 20500); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.ExposePort(context.Background(), "dynamic", 3000, false); err != nil {
				t.Error(err)
			}
			if _, err := c.ListTaps(context.Background(), nil); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(c.states["dynamic"].PortMappings) != 2 {
		t.Fatal("duplicate concurrent mappings")
	}
	a.failDelete = true
	if _, err := c.ClosePort(context.Background(), "dynamic", 3000); err == nil {
		t.Fatal("expected delete failure")
	}
	assertPortOwned(t, c, other, true)
	if a.remote[20500] != [2]uint32{999, 5000} {
		t.Fatal("foreign map removed")
	}
	a.failDelete = false
	if _, err := c.ClosePort(context.Background(), "dynamic", 3000); err != nil {
		t.Fatal(err)
	}
	assertPortOwned(t, c, other, true)
	if a.wholeDeletes != 0 {
		t.Fatal("whole-TAP cleanup used")
	}
	assertDynamicState(t, c)
}

func TestDynamicPortsFailedFinalWriteImmediatelyRemovesProvisionalMap(t *testing.T) {
	c, a := newDynamicTestController(t)
	writes := 0
	c.store.rewriteHook = func(phase string) error {
		if phase == "before_rename" {
			writes++
		}
		if writes == 2 && phase == "before_rename" {
			return errors.New("final write failure")
		}
		return nil
	}
	if _, err := c.ExposePort(context.Background(), "dynamic", 3000, false); err == nil {
		t.Fatal("expected write failure")
	}
	if len(a.remote) != 1 || len(a.local) != 1 {
		t.Fatal("provisional exposure remains")
	}
	assertDynamicState(t, c)
}

func TestDynamicPortsPendingCloseBlocksLifecycleOverwrite(t *testing.T) {
	c, a := newDynamicTestController(t)
	ctx := context.Background()
	resp, err := c.ExposePort(ctx, "dynamic", 3000, false)
	if err != nil {
		t.Fatal(err)
	}
	mapping := resp.PortMappings[1]
	a.failDelete = true
	if _, err := c.ClosePort(ctx, "dynamic", 3000); err == nil {
		t.Fatal("expected pending close")
	}
	if err := c.UpdateNetworkPolicy(ctx, &UpdateNetworkPolicyRequest{SandboxID: "dynamic"}); err == nil {
		t.Fatal("policy overwrote pending close")
	}
	released, err := c.ReleaseNetwork(ctx, &ReleaseNetworkRequest{SandboxID: "dynamic"})
	if err == nil || released.Released {
		t.Fatal("release overwrote pending close")
	}
	assertPortOwned(t, c, mapping, true)
	a.failDelete = false
	if err := c.UpdateNetworkPolicy(ctx, &UpdateNetworkPolicyRequest{SandboxID: "dynamic"}); err != nil {
		t.Fatal(err)
	}
	assertPortOwned(t, c, mapping, false)
	assertDynamicState(t, c)
}
