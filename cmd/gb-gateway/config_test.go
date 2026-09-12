package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLoadConfigValidMatrixAndDefaults(t *testing.T) {
	tests := []struct {
		name  string
		input string
		codec string
		ver   string
	}{
		{"2022+h265", "gb.protocol_version=2022\ncam1.gb_codec=h265\n", "h265", "2022"},
		{"2022+h264", "gb.protocol_version=2022\ncam1.gb_codec=h264\n", "h264", "2022"},
		{"2016+h264", "gb.protocol_version=2016\ncam1.gb_codec=h264\n", "h264", "2016"},
		{"defaults", "cam1.camera_id=cam1\n", "h265", "2022"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseConfig(strings.NewReader(tt.input))
			if err != nil {
				t.Fatalf("ParseConfig() error = %v", err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if cfg.GB.ProtocolVersion != tt.ver || cfg.Cameras[0].Codec != tt.codec {
				t.Fatalf("profile = %s+%s, want %s+%s", cfg.GB.ProtocolVersion, cfg.Cameras[0].Codec, tt.ver, tt.codec)
			}
		})
	}
}

func TestLoadConfigRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"unknown key", "gb.nope=1\n", "line 1: unknown key gb.nope"},
		{"duplicate", "gb.enabled=1\ngb.enabled=0\n", "line 2: duplicate key gb.enabled"},
		{"secret in config", "gb.password=top-secret\n", "credentials"},
		{"bad version", "gb.protocol_version=2011\n", "protocol_version"},
		{"bad codec", "cam1.gb_codec=vp9\n", "gb_codec"},
		{"invalid combination", "gb.protocol_version=2016\ncam1.gb_codec=h265\n", "2016+h265"},
		{"bad GB id", "gb.server_domain=123\n", "server_domain"},
		{"bad address", "gb.server_addr=example.com:0\n", "server_addr"},
		{"bad duration", "gb.heartbeat_interval=0s\n", "heartbeat_interval"},
		{"stop grace integer overflow", "gb.stop_grace_ms=9223372036854775807\n", "stop_grace_ms"},
		{"IDR timeout integer overflow", "gb.idr_timeout_ms=9223372036854775807\n", "idr_timeout_ms"},
		{"bad AU limit", "ipc.max_au_bytes=8388609\n", "max_au_bytes"},
		{"bad camera id", "cam1.camera_id=../cam\n", "camera_id"},
		{"bad status dir", "ipc.status_dir=relative/status\n", "status_dir"},
		{"bad listen port", "gb.sip_listen=:65536\n", "sip_listen"},
		{"bad ptz mode", "cam1.ptz_mode=pelco\n", "ptz_mode"},
		{"unknown camera key", "cam1.nope=1\n", "unknown key cam1.nope"},
		{"ambiguous camera index", "cam01.gb_codec=h264\n", "unknown key cam01.gb_codec"},
		{"enabled missing address", "gb.enabled=1\n", "server_addr"},
		{"enabled missing camera", "gb.enabled=1\ngb.server_addr=127.0.0.1:5060\ngb.server_domain=34020000002000000001\ngb.local_device_id=34020000002000000002\ngb.realm=3402000000\n", "at least one camera"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseConfig(strings.NewReader(tt.input))
			if err == nil {
				err = cfg.Validate()
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
			if strings.Contains(err.Error(), "top-secret") {
				t.Fatalf("error leaked secret: %v", err)
			}
		})
	}
}

func TestLoadConfigAcceptsPTZModes(t *testing.T) {
	for _, mode := range []string{"none", "local-gb28181", "onvif", "vendor"} {
		t.Run(mode, func(t *testing.T) {
			cfg, err := ParseConfig(strings.NewReader("cam1.ptz_mode=" + mode + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLoadConfigRejectsInconsistentCameras(t *testing.T) {
	input := "gb.protocol_version=2016\ncam1.camera_id=front\ncam1.gb_codec=h264\ncam2.camera_id=front\ncam2.gb_codec=h264\n"
	_, err := ParseConfig(strings.NewReader(input))
	if err == nil || !strings.Contains(err.Error(), "line 4") || !strings.Contains(err.Error(), "camera_id") {
		t.Fatalf("ParseConfig() error = %v, want duplicate camera_id with source line", err)
	}
}

func TestLoadConfigRejectsMixedCameraProfiles(t *testing.T) {
	input := "cam1.gb_codec=h264\ncam2.gb_codec=h265\n"
	cfg, err := ParseConfig(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "one gateway profile") {
		t.Fatalf("Validate() error = %v, want mixed-profile error", err)
	}
}

func TestLoadCredentialsRequires0600AndDoesNotLeakSecret(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials")
	if err := os.WriteFile(path, []byte("sip.password=top-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	creds, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials() error = %v", err)
	}
	if got, ok := creds.Lookup("sip.password"); !ok || got != "top-secret" {
		t.Fatalf("credential = %q, %v", got, ok)
	}

	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCredentials(path); err == nil || !strings.Contains(err.Error(), "0600") {
		t.Fatalf("permissions error = %v", err)
	}
}

func TestCredentialsFormattingIsRedacted(t *testing.T) {
	creds := Credentials{values: map[string]string{"sip.password": "top-secret"}}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		got := fmt.Sprintf(format, creds)
		if strings.Contains(got, "top-secret") || !strings.Contains(got, "redacted") {
			t.Errorf("fmt.Sprintf(%q) = %q, want redacted output", format, got)
		}
	}
}

func TestLoadCredentialsRejectsFIFOWithoutBlocking(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "credentials.fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "credentials-link")
	if err := os.Symlink(fifo, link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{fifo, link} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			done := make(chan error, 1)
			go func() {
				_, err := LoadCredentials(path)
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), "regular") {
					t.Fatalf("LoadCredentials() error = %v, want non-regular error", err)
				}
			case <-time.After(time.Second):
				t.Fatal("LoadCredentials() blocked on FIFO")
			}
		})
	}
}

func TestLoadCredentialsRejectsSpecialPermissionBits(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode os.FileMode
	}{
		{"setuid", 0o600 | os.ModeSetuid},
		{"setgid", 0o600 | os.ModeSetgid},
		{"sticky", 0o600 | os.ModeSticky},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "credentials")
			if err := os.WriteFile(path, []byte("sip.password=top-secret\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, tc.mode); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadCredentials(path); err == nil || !strings.Contains(err.Error(), "0600") {
				t.Fatalf("LoadCredentials() error = %v, want strict 0600 error", err)
			}
		})
	}
}

func TestLoadConfigReadsFreshFileAfterAtomicRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gateway.conf")
	write := func(contents string) {
		t.Helper()
		tmp := filepath.Join(dir, "gateway.conf.tmp")
		if err := os.WriteFile(tmp, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
	}
	write("cam1.gb_codec=h264\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cameras[0].Codec != "h264" {
		t.Fatalf("first codec = %q", cfg.Cameras[0].Codec)
	}
	write("cam1.gb_codec=h265\n")
	cfg, err = LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cameras[0].Codec != "h265" {
		t.Fatalf("renamed codec = %q", cfg.Cameras[0].Codec)
	}
}
