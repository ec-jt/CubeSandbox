package cubemasterclient

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestAuthenticatedLifecycleRequests(t *testing.T) {
	nonces := map[string]bool{}
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := []string{}
		for _, k := range []string{"cube_version", "cube_user_id", "cube_timestamp", "cube_nonce", "cube_sgn_method"} {
			values = append(values, r.Header.Get(k))
		}
		if values[0] != "2023" || values[1] != "test-clm" || values[4] != "sha256" {
			t.Error("incorrect authentication identity/protocol")
		}
		ts, err := strconv.ParseInt(values[2], 10, 64)
		if err != nil || time.Since(time.Unix(ts, 0)) > 5*time.Second {
			t.Error("invalid authentication timestamp")
		}
		if values[3] == "" || nonces[values[3]] {
			t.Error("missing or reused nonce")
		}
		nonces[values[3]] = true
		mac := hmac.New(sha256.New, []byte("test-secret"))
		_, _ = mac.Write([]byte(strings.Join(values, ".")))
		if r.Header.Get("cube_signature") != base64.StdEncoding.EncodeToString(mac.Sum(nil)) {
			t.Error("signature does not match Master protocol")
		}
		count++
		_, _ = w.Write([]byte(`{"ret":{"ret_code":200}}`))
	}))
	defer srv.Close()
	c := New(srv.URL, time.Second).WithAuth("test-clm", "test-secret")
	for _, fn := range []func(context.Context, string, string) error{c.Pause, c.Resume} {
		if err := fn(context.Background(), "sandbox", "cubebox"); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Kill(context.Background(), "sandbox", "cubebox", KillReasonRequest); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("requests=%d", count)
	}
}

func TestIncompleteAuthFailsBeforeNetwork(t *testing.T) {
	for _, credentials := range [][2]string{{"user", ""}, {"", "secret"}} {
		c := New("http://127.0.0.1:1", time.Second).WithAuth(credentials[0], credentials[1])
		err := c.Resume(context.Background(), "sandbox", "cubebox")
		if err == nil || !strings.Contains(err.Error(), "incomplete") {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}
