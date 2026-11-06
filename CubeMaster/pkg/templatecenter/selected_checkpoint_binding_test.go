package templatecenter

import "testing"

func TestSelectedCheckpointRollbackReplayBinding(t *testing.T) {
	raw := `{"request_id":"attempt-1","sandbox_id":"source-1","snapshot_id":"selected-1"}`
	if !snapshotRollbackRequestMatches(raw, "attempt-1", "source-1", "selected-1") {
		t.Fatal("exact replay must match")
	}
	for _, tc := range []struct{ raw, token, source, snapshot string }{
		{"", "attempt-1", "source-1", "selected-1"},
		{" ", "attempt-1", "source-1", "selected-1"},
		{"{", "attempt-1", "source-1", "selected-1"},
		{raw, "attempt-2", "source-1", "selected-1"},
		{raw, "attempt-1", "foreign", "selected-1"},
		{raw, "attempt-1", "source-1", "latest-pause"},
		{raw, "attempt-1", "source-1", "default-template"},
	} {
		if snapshotRollbackRequestMatches(tc.raw, tc.token, tc.source, tc.snapshot) {
			t.Fatalf("accepted different or unbound payload: %+v", tc)
		}
	}
}
