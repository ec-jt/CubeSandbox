// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/controller/runtemplate/components"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/controller/runtemplate/templatetypes"
)

// B10: templates built on v0.5.1 record component versions
// {cube-image: v0.5.1, cube-agent: v0.5.1, cube-shim: ""}. On a v0.7.1 node
// the deploy seeds component_versions/<name>/v0.5.1 from the still-installed
// v0.5.1 toolbox: cube-shim and cube-image copied verbatim, cube-agent as a
// directory whose cube-agent.ext4 is the v0.5.1 guest image (agent baked in)
// and whose version file says v0.5.1. Every v0.5.1 component must then
// resolve, and the shim component (empty version) must be skipped so the
// live (new) shim is used.
func TestEnsureTemplateComponentsResolvesV051Inventory(t *testing.T) {
	base := t.TempDir()
	files := map[string]string{
		"cube-image/v0.5.1/cube-guest-image-cpu.img":  "img-v0.5.1",
		"cube-agent/v0.5.1/cube-agent.ext4":           "img-v0.5.1",
		"cube-agent/v0.5.1/version":                   "v0.5.1\n",
		"cube-kernel-scf/sha256-5936216e3676/vmlinux": "kernel",
	}
	for rel, body := range files {
		p := filepath.Join(base, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	old := componentManagerInst
	t.Cleanup(func() { componentManagerInst = old })
	SetComponentManagerForTest(components.NewComponentManager(&components.ComponentManagerConfig{VersionedBaseDir: base}))

	local := &templatetypes.LocalRunTemplate{}
	versions := map[string]string{
		templatetypes.CubeComponentCubeImage:  "v0.5.1",
		templatetypes.CubeComponentCubeAgent:  "v0.5.1",
		templatetypes.CubeComponentCubeShim:   "",
		templatetypes.CubeComponentCubeKernel: "sha256:5936216e36768db9c8a008d8b27db274a748cbbe8f6241e74f44698bbea36687",
	}
	if err := EnsureTemplateComponents(context.Background(), local, versions); err != nil {
		t.Fatalf("v0.5.1 template components did not resolve: %v", err)
	}
	if got := local.Componts[templatetypes.CubeComponentCubeAgent].Component.Path; got != filepath.Join(base, "cube-agent/v0.5.1/cube-agent.ext4") {
		t.Fatalf("agent path %q", got)
	}
	if got := local.Componts[templatetypes.CubeComponentCubeImage].Component.Path; got != filepath.Join(base, "cube-image/v0.5.1/cube-guest-image-cpu.img") {
		t.Fatalf("image path %q", got)
	}
	if _, ok := local.Componts[templatetypes.CubeComponentCubeShim]; ok {
		t.Fatal("empty shim version must not pin a versioned shim")
	}
}

// Without the seeded v0.5.1 agent entry the same template fails, which is the
// app1 failure mode of 2026-11-09 (no component_versions at all).
func TestEnsureTemplateComponentsFailsWithoutV051Agent(t *testing.T) {
	base := t.TempDir()
	p := filepath.Join(base, "cube-image/v0.5.1/cube-guest-image-cpu.img")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := componentManagerInst
	t.Cleanup(func() { componentManagerInst = old })
	SetComponentManagerForTest(components.NewComponentManager(&components.ComponentManagerConfig{VersionedBaseDir: base}))
	err := EnsureTemplateComponents(context.Background(), &templatetypes.LocalRunTemplate{}, map[string]string{
		templatetypes.CubeComponentCubeImage: "v0.5.1",
		templatetypes.CubeComponentCubeAgent: "v0.5.1",
	})
	if err == nil {
		t.Fatal("missing component_versions/cube-agent/v0.5.1 must fail closed")
	}
}
