package sourcefence

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const LocalQuiesced = "local-quiesced-not-global-fenced"

type LocalDrainResult struct {
	Version       int             `json:"version"`
	Binding       Binding         `json:"binding"`
	Stage         string          `json:"stage"`
	NativeBinding json.RawMessage `json:"native_binding"`
}

func (s *Store) LoadStorage(b Binding) ([]byte, error) {
	if s == nil {
		return nil, ErrDisabled
	}
	if err := b.Validate(); err != nil {
		return nil, err
	}
	path := s.filename(b.SandboxID) + ".storage-v1"
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > 256<<10 {
		return nil, ErrBinding
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r struct {
		Binding  Binding         `json:"binding"`
		Evidence json.RawMessage `json:"native_evidence"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return nil, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF || r.Binding != b || len(r.Evidence) == 0 {
		return nil, ErrBinding
	}
	return r.Evidence, nil
}

// LoadLocalDrain reads the durable local drain record of one sandbox. It is a
// read-only accessor for the private cross-node read path: it never creates,
// rewrites, clears or re-drains a record, and it is not an authorization to
// take the source over. Any malformed, foreign or absent record is an error.
func (s *Store) LoadLocalDrain(sandboxID string) (*LocalDrainResult, error) {
	if s == nil {
		return nil, ErrDisabled
	}
	if sandboxID == "" {
		return nil, ErrBinding
	}
	path := s.filename(sandboxID) + ".local-drain-v1"
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() == 0 || st.Size() > 256<<10 {
		return nil, ErrBinding
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var r LocalDrainResult
	if err = d.Decode(&r); err != nil {
		return nil, fmt.Errorf("%w: corrupt local drain record: %v", ErrBinding, err)
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, ErrBinding
	}
	if r.Version != 1 || r.Binding.SandboxID != sandboxID || r.Binding.Validate() != nil ||
		r.Stage != LocalQuiesced || len(r.NativeBinding) == 0 || !json.Valid(r.NativeBinding) {
		return nil, ErrBinding
	}
	return &r, nil
}

// DrainLocal is called only after Stop persisted RuntimeStopped. A failed,
// canceled or lost RPC leaves that stage and the immutable storage binding.
// Retry never recaptures membership or creates a new native token. Native
// process restart remains pending: strict owner attach is refused, not cleared.
// Callers hold lifecycle + Barrier for the entire synchronous native call.
func (s *Store) DrainLocal(ctx context.Context, b Binding, body []byte, drain func(context.Context, []byte) ([]byte, error)) (*LocalDrainResult, error) {
	if s == nil {
		return nil, ErrDisabled
	}
	r, err := s.load(b.SandboxID)
	if err != nil {
		return nil, err
	}
	if r.Binding != b || r.Stage != RuntimeStopped {
		return nil, ErrBinding
	}
	evidence, err := s.LoadStorage(b)
	if err != nil {
		return nil, err
	}
	var bound struct {
		Native     json.RawMessage `json:"native"`
		Membership json.RawMessage `json:"membership"`
	}
	if err = json.Unmarshal(evidence, &bound); err != nil {
		return nil, err
	}
	if !bytes.Equal(bound.Membership, body) {
		return nil, ErrBinding
	}
	result := &LocalDrainResult{1, b, LocalQuiesced, body}
	want, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	path := s.filename(b.SandboxID) + ".local-drain-v1"
	if st, e := os.Lstat(path); e == nil {
		if !st.Mode().IsRegular() || st.Size() > 256<<10 {
			return nil, ErrBinding
		}
		old, e := os.ReadFile(path)
		if e != nil {
			return nil, e
		}
		if !bytes.Equal(old, want) {
			return nil, ErrBinding
		}
		if e = syncDir(s.root); e != nil {
			return nil, e
		}
		return result, nil
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	got, err := drain(ctx, body)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(got, body) {
		return nil, fmt.Errorf("native local drain binding mismatch")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(s.root, ".local-drain-")
	if err != nil {
		return nil, err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(want); err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err != nil {
		return nil, err
	}
	if ce != nil {
		return nil, ce
	}
	if err = os.Link(name, path); err != nil {
		return nil, err
	}
	if err = syncDir(s.root); err != nil {
		return nil, err
	}
	return result, nil
}
