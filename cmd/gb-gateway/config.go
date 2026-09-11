package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

const maxAUBytes = 8 * 1024 * 1024

const maxGatewayTimeoutMS = int64(time.Minute / time.Millisecond)

// Config is the gateway's non-secret configuration. Credentials are loaded
// separately so ordinary config exports can never contain passwords.
type Config struct {
	GB      GBConfig
	IPC     IPCConfig
	Cameras []CameraConfig
}

type GBConfig struct {
	Enabled         bool
	ProtocolVersion string
	ServerAddr      string
	ServerDomain    string
	LocalDeviceID   string
	Realm           string
	SIPListen       string
	Heartbeat       time.Duration
	RegisterExpires int
	Transport       string
	MediaTransport  string
	StopGrace       time.Duration
	IDRTimeout      time.Duration
	RecordPlayback  bool
}

type IPCConfig struct {
	ControlSocket string
	MediaSocket   string
	MaxAUBytes    int
	StatusDir     string
}

type CameraConfig struct {
	Index int
	// LocalCameraID is the stable local source identifier. The external
	// key remains camera_id for compatibility with the box design.
	LocalCameraID string
	Expose        bool
	Name          string
	Codec         string
	PTZMode       string
}

// Credentials holds secrets read from the restricted credentials file.
// Values are intentionally private; callers can request one by name without
// making accidental config/log serialization possible.
type Credentials struct {
	values map[string]string
}

// Format prevents fmt's map formatting from exposing credential values in
// logs or exports, including %#v where Stringer would not be sufficient.
func (c Credentials) Format(state fmt.State, verb rune) {
	if verb == 'v' && state.Flag('#') {
		_, _ = io.WriteString(state, "main.Credentials{values:<redacted>}")
		return
	}
	_, _ = io.WriteString(state, "Credentials{<redacted>}")
}

func (c Credentials) Lookup(key string) (string, bool) {
	v, ok := c.values[key]
	return v, ok
}

// ParseConfig reads the documented key=value format. It does not touch the
// filesystem and does not validate required startup fields until Validate.
func ParseConfig(r io.Reader) (Config, error) {
	c := defaultConfig()
	seen := make(map[string]bool)
	cameras := make(map[int]*CameraConfig)
	cameraFirstLines := make(map[int]int)
	cameraIDLines := make(map[int]int)
	s := bufio.NewScanner(r)
	lineNo := 0
	for s.Scan() {
		lineNo++
		line := strings.TrimSpace(s.Text())
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || key == "" {
			return c, configLineError(lineNo, "expected key=value")
		}
		if strings.Contains(strings.ToLower(key), "password") || strings.Contains(strings.ToLower(key), "secret") {
			return c, configLineError(lineNo, "credentials belong in a 0600 credentials file")
		}
		if seen[key] {
			return c, configLineError(lineNo, "duplicate key "+key)
		}
		seen[key] = true

		if index, field, ok := cameraKey(key); ok {
			camera := cameras[index]
			if camera == nil {
				camera = &CameraConfig{Index: index, LocalCameraID: fmt.Sprintf("cam%d", index), Expose: true, Codec: "h265", PTZMode: "none"}
				cameras[index] = camera
				cameraFirstLines[index] = lineNo
			}
			if err := parseCameraField(camera, field, value); err != nil {
				return c, configLineError(lineNo, err.Error())
			}
			if field == "camera_id" {
				cameraIDLines[index] = lineNo
			}
			continue
		}
		if err := parseTopLevelField(&c, key, value); err != nil {
			return c, configLineError(lineNo, err.Error())
		}
	}
	if err := s.Err(); err != nil {
		return c, err
	}
	for i := 1; i <= len(cameras); i++ {
		if camera, ok := cameras[i]; ok {
			c.Cameras = append(c.Cameras, *camera)
		}
	}
	if len(cameras) != len(c.Cameras) {
		return c, errors.New("camera indexes must start at cam1 and be contiguous")
	}
	seenLocalCameraIDs := make(map[string]int, len(c.Cameras))
	for _, camera := range c.Cameras {
		line := cameraIDLines[camera.Index]
		if line == 0 {
			line = cameraFirstLines[camera.Index]
		}
		if previous, ok := seenLocalCameraIDs[camera.LocalCameraID]; ok {
			return c, configLineError(line, fmt.Sprintf("camera_id %q duplicates line %d", camera.LocalCameraID, previous))
		}
		seenLocalCameraIDs[camera.LocalCameraID] = line
	}
	return c, nil
}

func LoadConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	c, err := ParseConfig(f)
	if err != nil {
		return Config{}, err
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func defaultConfig() Config {
	return Config{
		GB: GBConfig{
			ProtocolVersion: "2022",
			SIPListen:       ":5061",
			Heartbeat:       60 * time.Second,
			RegisterExpires: 3600,
			Transport:       "udp",
			MediaTransport:  "auto",
			StopGrace:       5 * time.Second,
			IDRTimeout:      3 * time.Second,
		},
		IPC: IPCConfig{
			ControlSocket: "/run/edge-gateway/control.sock",
			MediaSocket:   "/run/edge-gateway/media.sock",
			MaxAUBytes:    maxAUBytes,
			StatusDir:     "/run/edge-gateway",
		},
	}
}

func parseTopLevelField(c *Config, key, value string) error {
	switch key {
	case "gb.enabled":
		v, err := parseBool(value)
		c.GB.Enabled = v
		return withFieldError(key, err)
	case "gb.protocol_version":
		c.GB.ProtocolVersion = value
	case "gb.server_addr":
		c.GB.ServerAddr = value
	case "gb.server_domain":
		c.GB.ServerDomain = value
	case "gb.local_device_id":
		c.GB.LocalDeviceID = value
	case "gb.realm":
		c.GB.Realm = value
	case "gb.sip_listen":
		c.GB.SIPListen = value
	case "gb.heartbeat_interval":
		v, err := time.ParseDuration(value)
		c.GB.Heartbeat = v
		return withFieldError(key, err)
	case "gb.register_expires":
		v, err := strconv.Atoi(value)
		c.GB.RegisterExpires = v
		return withFieldError(key, err)
	case "gb.transport":
		c.GB.Transport = value
	case "gb.media_transport":
		c.GB.MediaTransport = value
	case "gb.stop_grace_ms":
		v, err := parseMilliseconds(value, 0, maxGatewayTimeoutMS)
		c.GB.StopGrace = v
		return withFieldError(key, err)
	case "gb.idr_timeout_ms":
		v, err := parseMilliseconds(value, 1, maxGatewayTimeoutMS)
		c.GB.IDRTimeout = v
		return withFieldError(key, err)
	case "gb.record_playback":
		v, err := parseBool(value)
		c.GB.RecordPlayback = v
		return withFieldError(key, err)
	case "ipc.control_socket":
		c.IPC.ControlSocket = value
	case "ipc.media_socket":
		c.IPC.MediaSocket = value
	case "ipc.max_au_bytes":
		v, err := strconv.Atoi(value)
		c.IPC.MaxAUBytes = v
		return withFieldError(key, err)
	case "ipc.status_dir":
		c.IPC.StatusDir = value
	default:
		return fmt.Errorf("unknown key %s", key)
	}
	return nil
}

func parseCameraField(c *CameraConfig, field, value string) error {
	switch field {
	case "camera_id":
		c.LocalCameraID = value
	case "gb_expose":
		v, err := parseBool(value)
		c.Expose = v
		return withFieldError("gb_expose", err)
	case "gb_name":
		c.Name = value
	case "gb_codec":
		c.Codec = strings.ToLower(value)
	case "ptz_mode":
		c.PTZMode = strings.ToLower(value)
	default:
		return fmt.Errorf("unknown key cam%d.%s", c.Index, field)
	}
	return nil
}

func cameraKey(key string) (int, string, bool) {
	if !strings.HasPrefix(key, "cam") {
		return 0, "", false
	}
	dot := strings.IndexByte(key[3:], '.')
	if dot < 1 {
		return 0, "", false
	}
	dot += 3
	index, err := strconv.Atoi(key[3:dot])
	if err != nil || index < 1 || (len(key[3:dot]) > 1 && key[3] == '0') {
		return 0, "", false
	}
	return index, key[dot+1:], true
}

func parseBool(value string) (bool, error) {
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("must be boolean")
	}
}

func withFieldError(key string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("invalid %s", key)
}

func parseMilliseconds(value string, min, max int64) (time.Duration, error) {
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil || ms < min || ms > max {
		return 0, errors.New("out of range")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func configLineError(line int, message string) error {
	return fmt.Errorf("line %d: %s", line, message)
}

// Validate rejects invalid values before sockets or cascade services are
// started. Empty optional fields remain valid while gb.enabled is false.
func (c Config) Validate() error {
	if c.GB.ProtocolVersion == "" {
		c.GB.ProtocolVersion = "2022"
	}
	if c.GB.ProtocolVersion != "2022" && c.GB.ProtocolVersion != "2016" {
		return fmt.Errorf("gb.protocol_version %q is unsupported", c.GB.ProtocolVersion)
	}
	if c.GB.Transport == "" {
		c.GB.Transport = "udp"
	}
	if c.GB.Transport != "udp" {
		return fmt.Errorf("gb.transport %q is unsupported; only udp is enabled", c.GB.Transport)
	}
	if c.GB.MediaTransport == "" {
		c.GB.MediaTransport = "auto"
	}
	if c.GB.MediaTransport != "auto" && c.GB.MediaTransport != "udp" && c.GB.MediaTransport != "tcp-active" {
		return fmt.Errorf("gb.media_transport %q is unsupported", c.GB.MediaTransport)
	}
	if err := validateAddr("gb.server_addr", c.GB.ServerAddr, false, c.GB.Enabled); err != nil {
		return err
	}
	if err := validateAddr("gb.sip_listen", c.GB.SIPListen, true, true); err != nil {
		return err
	}
	if err := validateID("gb.server_domain", c.GB.ServerDomain, c.GB.Enabled); err != nil {
		return err
	}
	if err := validateID("gb.local_device_id", c.GB.LocalDeviceID, c.GB.Enabled); err != nil {
		return err
	}
	if c.GB.Enabled && c.GB.Realm == "" {
		return errors.New("gb.realm is required when gb.enabled is true")
	}
	if c.GB.Heartbeat < time.Second || c.GB.Heartbeat > time.Hour {
		return fmt.Errorf("gb.heartbeat_interval %s out of range 1s-1h", c.GB.Heartbeat)
	}
	if c.GB.RegisterExpires < 1 || c.GB.RegisterExpires > 86400 {
		return fmt.Errorf("gb.register_expires %d out of range 1-86400", c.GB.RegisterExpires)
	}
	if c.GB.StopGrace < 0 || c.GB.StopGrace > time.Minute {
		return fmt.Errorf("gb.stop_grace_ms %s out of range 0-1m", c.GB.StopGrace)
	}
	if c.GB.IDRTimeout <= 0 || c.GB.IDRTimeout > time.Minute {
		return fmt.Errorf("gb.idr_timeout_ms %s out of range 1ms-1m", c.GB.IDRTimeout)
	}
	if c.IPC.MaxAUBytes < 1 || c.IPC.MaxAUBytes > maxAUBytes {
		return fmt.Errorf("ipc.max_au_bytes %d out of range 1-%d", c.IPC.MaxAUBytes, maxAUBytes)
	}
	for _, socket := range []struct{ name, value string }{{"ipc.control_socket", c.IPC.ControlSocket}, {"ipc.media_socket", c.IPC.MediaSocket}} {
		if !filepath.IsAbs(socket.value) {
			return fmt.Errorf("%s must be an absolute path", socket.name)
		}
	}
	if !filepath.IsAbs(c.IPC.StatusDir) {
		return errors.New("ipc.status_dir must be an absolute path")
	}
	if info, err := os.Stat(c.IPC.StatusDir); err == nil {
		if !info.IsDir() {
			return errors.New("ipc.status_dir must name a directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("ipc.status_dir cannot be inspected")
	}
	seenLocalCameraIDs := make(map[string]bool, len(c.Cameras))
	var profileCodec string
	for _, camera := range c.Cameras {
		if err := validateCamera(camera, c.GB.ProtocolVersion); err != nil {
			return err
		}
		if seenLocalCameraIDs[camera.LocalCameraID] {
			return fmt.Errorf("camera_id %q is duplicated", camera.LocalCameraID)
		}
		seenLocalCameraIDs[camera.LocalCameraID] = true
		if profileCodec == "" {
			profileCodec = camera.Codec
		} else if camera.Codec != profileCodec {
			return fmt.Errorf("camera codecs must use one gateway profile; got h264 and h265")
		}
	}
	if c.GB.Enabled && len(c.Cameras) == 0 {
		return errors.New("at least one camera is required when gb.enabled is true")
	}
	return nil
}

func validateCamera(camera CameraConfig, version string) error {
	if camera.LocalCameraID == "" {
		return fmt.Errorf("cam%d.camera_id is required", camera.Index)
	}
	if len(camera.LocalCameraID) > 64 || !utf8.ValidString(camera.LocalCameraID) {
		return fmt.Errorf("cam%d.camera_id must be 1-64 valid UTF-8 bytes", camera.Index)
	}
	if camera.LocalCameraID == "." || camera.LocalCameraID == ".." {
		return fmt.Errorf("cam%d.camera_id is not a path segment", camera.Index)
	}
	for _, r := range camera.LocalCameraID {
		if r == '/' || r == '\\' || r == 0 || r == '\n' || r == '\r' || r == '\t' || r < 0x20 {
			return fmt.Errorf("cam%d.camera_id contains an unsafe character", camera.Index)
		}
	}
	if camera.Codec != "h264" && camera.Codec != "h265" {
		return fmt.Errorf("cam%d.gb_codec %q is unsupported", camera.Index, camera.Codec)
	}
	if version == "2016" && camera.Codec == "h265" {
		return errors.New("2016+h265 is unsupported")
	}
	if camera.PTZMode != "none" && camera.PTZMode != "gb28181" && camera.PTZMode != "local-gb28181" && camera.PTZMode != "onvif" && camera.PTZMode != "vendor" {
		return fmt.Errorf("cam%d.ptz_mode %q is unsupported", camera.Index, camera.PTZMode)
	}
	return nil
}

func validateID(name, value string, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("%s is required", name)
		}
		return nil
	}
	if len(value) != 20 {
		return fmt.Errorf("%s must be 20 digits", name)
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return fmt.Errorf("%s must be 20 digits", name)
		}
	}
	return nil
}

func validateAddr(name, value string, allowEmptyHost, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("%s is required", name)
		}
		return nil
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil || (!allowEmptyHost && host == "") {
		return fmt.Errorf("%s must be host:port", name)
	}
	if host != "" && net.ParseIP(host) == nil {
		if _, err := netip.ParseAddr(host); err != nil && !validHostname(host) {
			return fmt.Errorf("%s host is invalid", name)
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("%s port must be 1-65535", name)
	}
	return nil
}

func validHostname(host string) bool {
	if len(host) > 253 || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' {
				return false
			}
		}
	}
	return true
}

// LoadCredentials reads key=value secrets from a regular file with exactly
// 0600 permissions. It never includes credential values in returned errors.
func LoadCredentials(path string) (Credentials, error) {
	// O_NONBLOCK makes opening a FIFO (including one reached through a
	// symlink) return immediately; regular-file checks still use this FD.
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return Credentials{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return Credentials{}, err
	}
	if !info.Mode().IsRegular() {
		return Credentials{}, fmt.Errorf("credentials file is not regular")
	}
	if info.Mode().Perm() != 0600 || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return Credentials{}, fmt.Errorf("credentials file must have 0600 permissions")
	}
	values := make(map[string]string)
	s := bufio.NewScanner(f)
	lineNo := 0
	for s.Scan() {
		lineNo++
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || key == "" || value == "" {
			return Credentials{}, fmt.Errorf("credentials line %d is invalid", lineNo)
		}
		if values[key] != "" {
			return Credentials{}, fmt.Errorf("credentials line %d duplicates key %s", lineNo, key)
		}
		values[key] = value
	}
	if err := s.Err(); err != nil {
		return Credentials{}, err
	}
	return Credentials{values: values}, nil
}
