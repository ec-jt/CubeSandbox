// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sourcefence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Source identity cohorts. A cohort is a typed property of the persisted
// records, never of the RPC request.
//
//   - CohortTemplateGenerationRoot is the original cohort: the sandbox runs on a
//     rootfs volume derived from a template/selected snapshot, so its volume
//     names follow the generation-root convention and the CubeBox carries a
//     LocalRunTemplate.
//
//   - CohortFreshImageS3 is the cohort this file adds: on an S3/cubecow backend
//     the product refuses template creation, so a sandbox created from a fresh
//     image has no LocalRunTemplate at all. Its identity must therefore be
//     derived from other authoritative records (persisted StorageInfo ownership
//     marker plus the live native volume), and it is hashed in its own versioned
//     domain so the two cohorts can never collide.
const (
	CohortTemplateGenerationRoot = "template-generation-root"
	CohortFreshImageS3           = "fresh-image-s3-v1"
)

// freshImageIncarnationDomain separates the fresh-image identity hash from the
// legacy template hash. The legacy envelope has no "domain" member at all, so
// the two marshalled documents are structurally disjoint and cannot collide
// even for identical identity inputs.
const freshImageIncarnationDomain = "cube.recovery.source.incarnation/fresh-image-s3-v1"

// FreshImageVolume is the live native rootfs identity of a fresh-image source.
// Every field is resolved on the node: the volume name from persisted
// StorageInfo, the uuid/size from the configured native service.
type FreshImageVolume struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
	Gen  uint32 `json:"gen"`
	UUID string `json:"uuid"`
	Size uint64 `json:"size"`
}

func (v FreshImageVolume) validate() error {
	if v.Name == "" || v.UUID == "" || v.Size == 0 || v.Kind != "volume" {
		return fmt.Errorf("incomplete fresh-image native volume identity")
	}
	if v.Name != strings.TrimSpace(v.Name) || strings.ContainsAny(v.Name, "\x00\r\n") {
		return fmt.Errorf("invalid fresh-image native volume name")
	}
	return nil
}

// TemplateIncarnation reproduces the original identity bytes exactly. The
// anonymous struct this replaced had the same field names, order and (absent)
// tags, so existing durable fences keep matching without migration.
func TemplateIncarnation(id string, created int64, generation uint64, started time.Time, volumes any) (string, error) {
	if id == "" || created <= 0 || generation == 0 || started.IsZero() {
		return "", fmt.Errorf("missing authoritative source incarnation")
	}
	b, err := json.Marshal(struct {
		ID         string
		Created    int64
		Generation uint64
		Started    time.Time
		Volumes    any
	}{id, created, generation, started, volumes})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// FreshImageIncarnation hashes a fresh-image source identity in its own
// versioned domain. Callers must resolve every input from authoritative
// persisted records plus the live native volume; nothing here is accepted from
// the Update request.
func FreshImageIncarnation(id string, created int64, generation uint64, started time.Time, v FreshImageVolume) (string, error) {
	if id == "" || created <= 0 || generation == 0 || started.IsZero() {
		return "", fmt.Errorf("missing authoritative source incarnation")
	}
	if err := v.validate(); err != nil {
		return "", err
	}
	b, err := json.Marshal(struct {
		Domain     string           `json:"domain"`
		Sandbox    string           `json:"sandbox"`
		Created    int64            `json:"created"`
		Generation uint64           `json:"generation"`
		Started    time.Time        `json:"started"`
		Rootfs     FreshImageVolume `json:"rootfs"`
	}{freshImageIncarnationDomain, id, created, generation, started, v})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// FreshImageIncarnationDomain exposes the domain string for evidence and tests.
func FreshImageIncarnationDomain() string { return freshImageIncarnationDomain }
