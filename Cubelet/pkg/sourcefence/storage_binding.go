package sourcefence

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

// MatchesBinding only permits explicit compatibility with an exact existing
// durable binding. It never rewrites, migrates or clears that record.
func (s *Store) MatchesBinding(b Binding) bool {
	if s == nil {
		return false
	}
	r, err := s.load(b.SandboxID)
	return err == nil && r.Binding == b
}

// BindStorage persists locally resolved evidence before stop. The retained shim
// wire record stays version 1. Evidence is not a caller receipt and does not
// change Stage. A sidecar without a main record still denies package cleanup.
// Caller holds lifecycle + Barrier. No overwrite, migration or unlock exists.
func (s *Store) BindStorage(b Binding, evidence []byte) error {
	if s == nil {
		return ErrDisabled
	}
	if err := b.Validate(); err != nil {
		return err
	}
	if len(evidence) == 0 || len(evidence) > 128<<10 || !json.Valid(evidence) {
		return fmt.Errorf("invalid native storage binding")
	}
	body, err := json.Marshal(struct {
		Binding  Binding         `json:"binding"`
		Evidence json.RawMessage `json:"native_evidence"`
	}{b, evidence})
	if err != nil {
		return err
	}
	path := s.filename(b.SandboxID) + ".storage-v1"
	f, err := os.CreateTemp(s.root, ".storage-write-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(body); err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err != nil {
		return err
	}
	if ce != nil {
		return ce
	}
	if err = os.Link(name, path); err != nil {
		if !os.IsExist(err) {
			return err
		}
		st, e := os.Lstat(path)
		if e != nil || !st.Mode().IsRegular() || st.Size() > 256<<10 {
			return ErrBinding
		}
		old, e := os.ReadFile(path)
		if e != nil || !bytes.Equal(old, body) {
			return ErrBinding
		}
	}
	return syncDir(s.root)
}
