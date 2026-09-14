package cascade

import (
	"fmt"
	"strings"
)

const (
	StatusOnline          = "ONLINE"
	StatusOffline         = "OFFLINE"
	StatusVersionMismatch = "VERSION_MISMATCH"
	StatusVersionMissing  = "VERSION_MISSING"
)

// resolveProtocolProfile normalizes the source-compatible config seam and
// enforces the profiles supported by the cascade.
func resolveProtocolProfile(version, codec string) (string, string, error) {
	version = strings.TrimSpace(version)
	if version == "" {
		version = "2022"
	}
	codec = strings.ToLower(strings.TrimSpace(codec))
	if codec == "" {
		codec = "h265"
	}
	if version != "2022" && version != "2016" {
		return "", "", fmt.Errorf("unsupported protocol version %q", version)
	}
	if codec != "h264" && codec != "h265" {
		return "", "", fmt.Errorf("unsupported video codec %q", codec)
	}
	if version == "2016" && codec == "h265" {
		return "", "", fmt.Errorf("unsupported protocol profile %s+%s", version, codec)
	}
	return version, codec, nil
}

func cameraProtocolProfile(cfg Config, cam CameraInfo) (string, string, error) {
	return resolveProtocolProfile(cfg.EffectiveProtocolVersion(), cam.Encoding)
}

func (s *Service) validateProtocolProfiles() error {
	if _, _, err := resolveProtocolProfile(s.cfg.EffectiveProtocolVersion(), "h264"); err != nil {
		return err
	}
	for _, cam := range s.src.Cameras() {
		if _, _, err := cameraProtocolProfile(s.cfg, cam); err != nil {
			return fmt.Errorf("camera %q: %w", cam.ID, err)
		}
	}
	return nil
}

func profileVersionMarker(version string) string {
	if strings.TrimSpace(version) == "2016" {
		return "2.0"
	}
	return "3.0"
}

func (s *Service) mediaVersionAllowed(u *upper, cam CameraInfo) bool {
	version, codec, err := cameraProtocolProfile(s.cfg, cam)
	if err != nil {
		return false
	}
	if u == nil || version != "2022" || codec != "h265" {
		return true
	}
	s.mu.Lock()
	seen, upstreamVersion := u.protocolVersionSeen, u.protocolVersion
	s.mu.Unlock()
	return !seen || upstreamVersion == profileVersionMarker(version)
}

func (s *Service) upperVersionMissingLocked(u *upper) bool {
	return u.protocolVersionSeen && s.requiresVersionGate() && u.protocolVersion == ""
}

func (s *Service) requiresVersionGate() bool {
	if s.cfg.EffectiveProtocolVersion() != "2022" {
		return false
	}
	for _, cam := range s.src.Cameras() {
		_, codec, err := cameraProtocolProfile(s.cfg, cam)
		if err == nil && codec == "h265" {
			return true
		}
	}
	return false
}
