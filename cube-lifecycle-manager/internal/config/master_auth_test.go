package config

import "testing"

func TestMasterAuthConfiguration(t *testing.T) {
	for _, pair := range [][2]string{{"", ""}, {"user", ""}, {"", "secret"}, {"user", "secret"}} {
		t.Setenv("CUBE_LCM_CUBEMASTER_USER_ID", pair[0])
		t.Setenv("CUBE_LCM_CUBEMASTER_SECRET_KEY", pair[1])
		cfg, err := Load()
		if err == nil {
			err = cfg.Validate()
		}
		wantError := (pair[0] == "") != (pair[1] == "")
		if (err != nil) != wantError {
			t.Fatalf("incomplete=%v error=%v", wantError, err)
		}
		if err == nil && (cfg.CubeMasterUserID != pair[0] || cfg.CubeMasterSecretKey != pair[1]) {
			t.Fatal("credentials not loaded")
		}
	}
}
