// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sourcefence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// legacyIncarnation reproduces the identity hash that shipped before the typed
// cohorts existed. Existing durable fences were written against it, so it must
// keep producing the same bytes.
func legacyIncarnation(id string, created int64, generation uint64, started time.Time, volumes any) string {
	b, _ := json.Marshal(struct {
		ID         string
		Created    int64
		Generation uint64
		Started    time.Time
		Volumes    any
	}{id, created, generation, started, volumes})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func TestTemplateIncarnationPreservesLegacyBytes(t *testing.T) {
	started := time.Unix(1700000000, 123456789).UTC()
	cases := []struct {
		name    string
		volumes any
	}{
		{"nil volumes", nil},
		{"empty map", map[string]any{}},
		{"populated", map[string]any{"rootfs": map[string]any{"VolumeID": "native-volume"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TemplateIncarnation("sb", 1700000000000000000, 3, started, tc.volumes)
			if err != nil {
				t.Fatalf("TemplateIncarnation: %v", err)
			}
			want := legacyIncarnation("sb", 1700000000000000000, 3, started, tc.volumes)
			if got != want {
				t.Fatalf("template incarnation changed: got %s want %s", got, want)
			}
			if len(got) != 64 {
				t.Fatalf("unexpected hash length %d", len(got))
			}
		})
	}
}

func TestTemplateIncarnationRejectsMissingIdentity(t *testing.T) {
	started := time.Unix(1700000000, 0).UTC()
	for _, tc := range []struct {
		name    string
		id      string
		created int64
		gen     uint64
		started time.Time
	}{
		{"no id", "", 1, 1, started},
		{"no created", "sb", 0, 1, started},
		{"no generation", "sb", 1, 0, started},
		{"no started", "sb", 1, 1, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := TemplateIncarnation(tc.id, tc.created, tc.gen, tc.started, nil); err == nil {
				t.Fatal("expected refusal")
			}
		})
	}
}

func TestFreshImageIncarnationIsADistinctVersionedDomain(t *testing.T) {
	started := time.Unix(1700000000, 42).UTC()
	vol := FreshImageVolume{Name: "sb-sb-root", Kind: "volume", Gen: 0, UUID: "live-uuid", Size: 1073741824}
	fresh, err := FreshImageIncarnation("sb", 1700000000000000000, 3, started, vol)
	if err != nil {
		t.Fatalf("FreshImageIncarnation: %v", err)
	}
	again, err := FreshImageIncarnation("sb", 1700000000000000000, 3, started, vol)
	if err != nil {
		t.Fatalf("FreshImageIncarnation repeat: %v", err)
	}
	if fresh != again {
		t.Fatal("fresh-image incarnation is not deterministic")
	}
	// The legacy domain must never produce the fresh-image value for the same
	// create identity, even when the volume map is chosen to look similar.
	for _, legacyVolumes := range []any{
		nil,
		map[string]any{},
		map[string]any{"rootfs": map[string]any{"VolumeID": vol.Name, "UUID": vol.UUID, "Size": vol.Size}},
	} {
		if legacyIncarnation("sb", 1700000000000000000, 3, started, legacyVolumes) == fresh {
			t.Fatal("fresh-image incarnation collides with the template domain")
		}
	}
	// The domain string is part of the hashed document, not just documentation.
	for _, mutated := range []FreshImageVolume{
		{Name: vol.Name, Kind: vol.Kind, Gen: vol.Gen, UUID: "other-uuid", Size: vol.Size},
		{Name: vol.Name, Kind: vol.Kind, Gen: vol.Gen, UUID: vol.UUID, Size: vol.Size + 1},
		{Name: "sb-sb-other", Kind: vol.Kind, Gen: vol.Gen, UUID: vol.UUID, Size: vol.Size},
	} {
		other, err := FreshImageIncarnation("sb", 1700000000000000000, 3, started, mutated)
		if err != nil {
			t.Fatalf("FreshImageIncarnation mutated: %v", err)
		}
		if other == fresh {
			t.Fatalf("fresh-image incarnation ignores %+v", mutated)
		}
	}
	if !strings.Contains(FreshImageIncarnationDomain(), "fresh-image-s3-v1") {
		t.Fatalf("unexpected domain %q", FreshImageIncarnationDomain())
	}
}

func TestFreshImageIncarnationRejectsIncompleteIdentity(t *testing.T) {
	started := time.Unix(1700000000, 0).UTC()
	good := FreshImageVolume{Name: "sb-sb-root", Kind: "volume", Gen: 0, UUID: "live-uuid", Size: 4096}
	for _, tc := range []struct {
		name string
		id   string
		gen  uint64
		vol  FreshImageVolume
	}{
		{"no sandbox", "", 1, good},
		{"no generation", "sb", 0, good},
		{"no name", "sb", 1, FreshImageVolume{Kind: "volume", UUID: "u", Size: 1}},
		{"no uuid", "sb", 1, FreshImageVolume{Name: "n", Kind: "volume", Size: 1}},
		{"no size", "sb", 1, FreshImageVolume{Name: "n", Kind: "volume", UUID: "u"}},
		{"snapshot kind", "sb", 1, FreshImageVolume{Name: "n", Kind: "snapshot", UUID: "u", Size: 1}},
		{"control chars", "sb", 1, FreshImageVolume{Name: "n\nx", Kind: "volume", UUID: "u", Size: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := FreshImageIncarnation(tc.id, 1, tc.gen, started, tc.vol); err == nil {
				t.Fatal("expected refusal")
			}
		})
	}
}
