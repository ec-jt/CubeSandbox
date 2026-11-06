package sourcefence

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalDrainOrderingUncertaintyRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fences")
	s, err := Open(root)
	require.NoError(t, err)
	b := Binding{"operation", "sandbox", "node", "incarnation", "checkpoint", strings.Repeat("a", 64)}
	body := []byte(`{"token":"native"}`)
	evidence, _ := json.Marshal(map[string]json.RawMessage{"native": []byte(`{}`), "membership": body})
	called := false
	drain := func(context.Context, []byte) ([]byte, error) { called = true; return body, nil }
	_, err = s.DrainLocal(context.Background(), b, body, drain)
	require.Error(t, err)
	require.False(t, called)
	r, err := s.Stop(context.Background(), true, b, func() ([]Process, error) {
		require.NoError(t, s.BindStorage(b, evidence))
		return []Process{{99, "1", "boot", "shim"}}, nil
	}, func(context.Context, []Process) error {
		_, err := s.LoadStorage(b)
		require.NoError(t, err)
		require.ErrorIs(t, s.Check(b.SandboxID), ErrFenced)
		return nil
	})
	require.ErrorIs(t, err, ErrDrainUnsupported)
	require.Equal(t, RuntimeStopped, r.Stage)
	_, err = s.DrainLocal(context.Background(), b, body, func(context.Context, []byte) ([]byte, error) { return nil, context.DeadlineExceeded })
	require.ErrorIs(t, err, context.DeadlineExceeded)
	s, err = Open(root)
	require.NoError(t, err)
	_, err = s.DrainLocal(context.Background(), b, []byte(`{"token":"foreign"}`), drain)
	require.ErrorIs(t, err, ErrBinding)
	require.False(t, called)
	foreign := b
	foreign.OperationID = "foreign"
	_, err = s.DrainLocal(context.Background(), foreign, body, drain)
	require.ErrorIs(t, err, ErrBinding)
	_, err = s.DrainLocal(context.Background(), b, body, func(context.Context, []byte) ([]byte, error) { return []byte(`{}`), nil })
	require.Error(t, err)
	result, err := s.DrainLocal(context.Background(), b, body, drain)
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, LocalQuiesced, result.Stage)
	s, err = Open(root)
	require.NoError(t, err)
	again, err := s.DrainLocal(context.Background(), b, body, func(context.Context, []byte) ([]byte, error) {
		t.Fatal("durable receipt must not drain again")
		return nil, errors.New("unreachable")
	})
	require.NoError(t, err)
	require.Equal(t, result, again)
	require.ErrorIs(t, s.Check(b.SandboxID), ErrFenced)
}
