// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package runtime

import "testing"

func TestPortBinderReserveRollbackPreservesExistingOwnership(t *testing.T) {
	tests := []struct {
		name string
		tail []PortMapping
	}{
		{"invalid mapping", []PortMapping{{HostPort: 20082, ContainerPort: 0}}},
		{"duplicate mapping", []PortMapping{{HostPort: 20080, ContainerPort: 82}}},
		{"foreign owner", []PortMapping{{HostPort: 20083, ContainerPort: 83}}},
		{"outside managed range", []PortMapping{{HostPort: 1, ContainerPort: 84}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binder := newTestPortBinder()
			if _, err := binder.Reserve("sandbox", []PortMapping{{HostPort: 20080, ContainerPort: 80}}, ""); err != nil {
				t.Fatal(err)
			}
			if _, err := binder.Reserve("other", []PortMapping{{HostPort: 20083, ContainerPort: 83}}, ""); err != nil {
				t.Fatal(err)
			}
			request := []PortMapping{
				{HostPort: 20080, ContainerPort: 80},
				{HostPort: 20081, ContainerPort: 81},
				{ContainerPort: 85},
			}
			if _, err := binder.Reserve("sandbox", append(request, tt.tail...), ""); err == nil {
				t.Fatal("expected reservation failure")
			}
			if len(binder.assigned) != 2 || binder.assigned[20080] != "sandbox" || binder.assigned[20083] != "other" {
				t.Fatalf("rollback changed existing ownership or leaked new ports: %#v", binder.assigned)
			}
			if len(binder.owners["sandbox"]) != 1 || len(binder.owners["other"]) != 1 {
				t.Fatalf("owner index differs from reservations: %#v", binder.owners)
			}
			if _, err := binder.Reserve("other", []PortMapping{{HostPort: 20080, ContainerPort: 80}}, ""); err == nil {
				t.Fatal("another sandbox acquired the existing port")
			}
		})
	}
}

func TestPortBinderReleasePortPreservesUnrelatedOwnership(t *testing.T) {
	binder := newTestPortBinder()
	if _, err := binder.Reserve("sandbox", []PortMapping{
		{HostPort: 20080, ContainerPort: 80}, {HostPort: 20081, ContainerPort: 81},
	}, ""); err != nil {
		t.Fatal(err)
	}
	if err := binder.ReleasePort("other", 20080); err == nil {
		t.Fatal("foreign-owner release accepted")
	}
	if err := binder.ReleasePort("", 20080); err == nil {
		t.Fatal("empty-owner release accepted")
	}
	if err := binder.ReleasePort("sandbox", 0); err == nil {
		t.Fatal("zero-port release accepted")
	}
	for i := 0; i < 2; i++ {
		if err := binder.ReleasePort("sandbox", 20081); err != nil {
			t.Fatal(err)
		}
	}
	if len(binder.assigned) != 1 || binder.assigned[20080] != "sandbox" || len(binder.owners["sandbox"]) != 1 {
		t.Fatalf("unrelated port was released: assigned=%#v owners=%#v", binder.assigned, binder.owners)
	}
	if _, err := binder.Reserve("other", []PortMapping{{HostPort: 20081, ContainerPort: 81}}, ""); err != nil {
		t.Fatal(err)
	}
	if err := binder.ReleasePort("sandbox", 20081); err == nil {
		t.Fatal("stale release accepted after reassignment")
	}
	if binder.assigned[20081] != "other" {
		t.Fatal("stale release removed the new owner")
	}
	if err := binder.ReleasePort("sandbox", 20080); err != nil {
		t.Fatal(err)
	}
	if _, exists := binder.owners["sandbox"]; exists {
		t.Fatal("empty owner index was not removed")
	}
}

func TestPortBinderReserveRejectsInvalidPortsWithoutLeakingOwnership(t *testing.T) {
	tests := []struct {
		name    string
		mapping PortMapping
	}{
		{name: "zero container port", mapping: PortMapping{HostPort: 20080, ContainerPort: 0}},
		{name: "negative container port", mapping: PortMapping{HostPort: 20080, ContainerPort: -1}},
		{name: "overflow container port", mapping: PortMapping{HostPort: 20080, ContainerPort: 65536}},
		{name: "negative host port", mapping: PortMapping{HostPort: -1, ContainerPort: 80}},
		{name: "overflow host port", mapping: PortMapping{HostPort: 65536, ContainerPort: 80}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binder := newTestPortBinder()
			if _, err := binder.Reserve("sandbox", []PortMapping{
				{HostPort: 20079, ContainerPort: 79},
				tt.mapping,
			}, "127.0.0.1"); err == nil {
				t.Fatal("Reserve returned nil error")
			}
			if len(binder.assigned) != 0 {
				t.Fatalf("assigned ports leaked after validation error: %#v", binder.assigned)
			}
			if len(binder.owners) != 0 {
				t.Fatalf("owners leaked after validation error: %#v", binder.owners)
			}
		})
	}
}

func TestPortBinderReserveRejectsDuplicateResolvedHostPort(t *testing.T) {
	tests := []struct {
		name     string
		mappings []PortMapping
	}{
		{
			name: "two explicit mappings",
			mappings: []PortMapping{
				{HostPort: 20080, ContainerPort: 80},
				{HostPort: 20080, ContainerPort: 81},
			},
		},
		{
			name: "automatic then explicit mapping",
			mappings: []PortMapping{
				{HostPort: 0, ContainerPort: 80},
				{HostPort: int32(portMin), ContainerPort: 81},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binder := newTestPortBinder()
			if _, err := binder.Reserve("sandbox", tt.mappings, "127.0.0.1"); err == nil {
				t.Fatal("Reserve returned nil error")
			}
			if len(binder.assigned) != 0 {
				t.Fatalf("assigned ports leaked after duplicate error: %#v", binder.assigned)
			}
			if len(binder.owners) != 0 {
				t.Fatalf("owners leaked after duplicate error: %#v", binder.owners)
			}
		})
	}
}

func TestPortBinderReserveAllowsDistinctAutomaticPorts(t *testing.T) {
	binder := newTestPortBinder()
	mappings, err := binder.Reserve("sandbox", []PortMapping{
		{HostPort: 0, ContainerPort: 80},
		{HostPort: 0, ContainerPort: 81},
	}, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if len(mappings) != 2 || mappings[0].HostPort != int32(portMin) || mappings[1].HostPort != int32(portMin)+1 {
		t.Fatalf("automatic mappings = %#v, want host ports %d and %d", mappings, portMin, portMin+1)
	}
	if len(binder.owners["sandbox"]) != 2 {
		t.Fatalf("owners after Reserve = %#v, want two owned ports", binder.owners["sandbox"])
	}
}

func TestPortBinderAssignOwnerIsTransactional(t *testing.T) {
	binder := newTestPortBinder()
	binder.assigned[20081] = "other-sandbox"

	err := binder.AssignOwner("sandbox", []PortMapping{
		{HostPort: 20080, ContainerPort: 80},
		{HostPort: 20081, ContainerPort: 81},
	})
	if err == nil {
		t.Fatal("AssignOwner returned nil error")
	}
	if _, ok := binder.assigned[20080]; ok {
		t.Fatalf("AssignOwner partially assigned port 20080: %#v", binder.assigned)
	}
	if _, ok := binder.owners["sandbox"]; ok {
		t.Fatalf("AssignOwner left partial owner state: %#v", binder.owners["sandbox"])
	}
	if got := binder.assigned[20081]; got != "other-sandbox" {
		t.Fatalf("existing assignment changed to %q", got)
	}
}

func TestPortBinderAssignOwnerValidatesBeforeOwnership(t *testing.T) {
	binder := newTestPortBinder()
	err := binder.AssignOwner("sandbox", []PortMapping{
		{HostPort: 20080, ContainerPort: 80},
		{HostPort: 20081, ContainerPort: 65536},
	})
	if err == nil {
		t.Fatal("AssignOwner returned nil error")
	}
	if len(binder.assigned) != 0 || len(binder.owners) != 0 {
		t.Fatalf("AssignOwner committed before validation completed: assigned=%#v owners=%#v", binder.assigned, binder.owners)
	}
}

func newTestPortBinder() *PortBinder {
	return &PortBinder{
		min:      portMin,
		max:      portMax,
		next:     portMin,
		assigned: make(map[uint16]string),
		owners:   make(map[string]map[uint16]struct{}),
	}
}
