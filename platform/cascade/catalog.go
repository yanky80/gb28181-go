package cascade

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/mickeyzzc/gb28181-go/manscdp"
)

// catalogItems builds the aggregated catalog: one channel per local camera,
// with GB channel IDs allocated on first sight and persisted
// (cascade_channels) so the upper platform's bindings survive restarts.
// Format: <LocalDeviceID[:10]> + "132" + 7-digit serial.
func (s *Service) catalogItems() ([]manscdp.Item, error) {
	cams := s.src.Cameras()

	alloc := map[string]string{} // cameraID → gbChannelID
	maxSerial := 0
	if s.db != nil {
		rows, err := s.db.ListCascadeChannels(context.Background())
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			if err := validateCascadeChannelID(r.GBChannelID); err != nil {
				return nil, fmt.Errorf("cascade: invalid persisted channel %q: %w", r.GBChannelID, err)
			}
			alloc[r.CameraID] = r.GBChannelID
			if ser, err := strconv.Atoi(r.GBChannelID[len(r.GBChannelID)-7:]); err == nil && ser > maxSerial {
				maxSerial = ser
			}
		}
	}

	prefix := cascadeChannelPrefix(s.cfg.LocalDeviceID)

	items := make([]manscdp.Item, 0, len(cams))
	for _, cam := range cams {
		if cam.CascadeHidden {
			// Catalog convergence: hidden cameras are not advertised. Their
			// persisted channel allocation is kept so re-enabling restores the
			// same channel code (upper-side bindings survive).
			continue
		}
		var chID string
		if s.db == nil {
			chID = s.nilStoreChannelID(cam.ID, prefix)
		}
		if s.db != nil {
			var ok bool
			chID, ok = alloc[cam.ID]
			if !ok {
				maxSerial++
				chID = fmt.Sprintf("%s%07d", prefix, maxSerial)
				if err := s.db.UpsertCascadeChannel(context.Background(), CascadeChannel{
					CameraID: cam.ID, GBChannelID: chID, Name: cam.Name, UpdatedAt: time.Now(),
				}); err != nil {
					slog.Warn("gb28181-cascade: channel allocation persist failed",
						"camera", cam.ID, "channel", chID, "error", err)
				}
			}
		}
		items = append(items, manscdp.Item{
			DeviceID:     chID,
			Name:         cam.Name,
			Parental:     0,
			Status:       s.cameraStatus(cam.ID),
			Manufacturer: orDefault(cam.Brand, s.cfg.CatalogManufacturer()),
			Model:        orDefault(cam.Model, s.cfg.CatalogModel()),
			RegisterWay:  1,
			// PTZType 3 = pan/tilt/zoom: the upper platform refuses to send
			// PTZ (404 "PTZ not supported") when this is 0. The cascade
			// forwards DeviceControl to whatever the local camera supports.
			PTZType: 3,
		})
	}
	return items, nil
}

// cameraOfChannel resolves the local camera behind an aggregated channel ID.
func (s *Service) cameraOfChannel(channelID string) (string, bool) {
	if s.db != nil {
		rows, err := s.db.ListCascadeChannels(context.Background())
		if err != nil {
			return "", false
		}
		for _, r := range rows {
			if validateCascadeChannelID(r.GBChannelID) == nil && r.GBChannelID == channelID {
				return r.CameraID, true
			}
		}
		return "", false
	}

	s.channelMu.RLock()
	cameraID, ok := s.channelBindings[channelID]
	s.channelMu.RUnlock()
	return cameraID, ok
}

func (s *Service) nilStoreChannelID(cameraID, prefix string) string {
	s.channelMu.Lock()
	defer s.channelMu.Unlock()
	if s.channelBindings == nil {
		s.channelBindings = make(map[string]string)
	}
	for channelID, id := range s.channelBindings {
		if id == cameraID {
			return channelID
		}
	}
	s.nextChannelSerial++
	channelID := fmt.Sprintf("%s%07d", prefix, s.nextChannelSerial)
	s.channelBindings[channelID] = cameraID
	return channelID
}

func cascadeChannelPrefix(localDeviceID string) string {
	if len(localDeviceID) < 10 {
		localDeviceID = fmt.Sprintf("%-10s", localDeviceID)[:10]
	}
	return localDeviceID[:10] + "132"
}

func validateCascadeChannelID(channelID string) error {
	if len(channelID) != 20 {
		return fmt.Errorf("must be a 20-digit ID")
	}
	for _, b := range []byte(channelID) {
		if b < '0' || b > '9' {
			return fmt.Errorf("must contain only digits")
		}
	}
	return nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// cameraInfo returns the source's view of one camera.
func (s *Service) cameraInfo(cameraID string) (CameraInfo, bool) {
	for _, cam := range s.src.Cameras() {
		if cam.ID == cameraID {
			return cam, true
		}
	}
	return CameraInfo{}, false
}
