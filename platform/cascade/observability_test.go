package cascade

import (
	"strings"
	"sync"
	"testing"

	"github.com/mickeyzzc/gb28181-go/metrics"
)

type eventHooks struct {
	mu     sync.Mutex
	events []metrics.GatewayEvent
}

func (h *eventHooks) ObserveGateway(event metrics.GatewayEvent) {
	h.mu.Lock()
	h.events = append(h.events, event)
	h.mu.Unlock()
}

func (h *eventHooks) RegisterAttempt()      {}
func (h *eventHooks) RegisterOK()           {}
func (h *eventHooks) RegisterFail()         {}
func (h *eventHooks) KeepaliveFail()        {}
func (h *eventHooks) InviteSessionStarted() {}
func (h *eventHooks) InviteSessionStopped() {}
func (h *eventHooks) InviteFail()           {}
func (h *eventHooks) PSBytesOut(int64)      {}

func TestMediaObservabilityReportsSuccessfulRTPAndOneLifecycle(t *testing.T) {
	hooks := &eventHooks{}
	svc := New(Config{}, nil, nil)
	svc.SetMetricsHooks(hooks)
	ms := &mediaSession{svc: svc, callID: "call", channel: "channel", generation: 4, ssrc: 9}
	ms.observeRTP(17)
	ms.observeRTP(23)
	ms.close()
	ms.close()

	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if len(hooks.events) != 4 {
		t.Fatalf("events = %+v", hooks.events)
	}
	if hooks.events[0].Name != "invite_started" || hooks.events[1].Name != "rtp_send" ||
		hooks.events[1].Value != 17 || hooks.events[2].Name != "rtp_send" ||
		hooks.events[2].Value != 23 || hooks.events[3].Name != "invite_stopped" {
		t.Fatalf("events = %+v", hooks.events)
	}
}

func TestSafeDiagnosticRedactsCredentialsAndPayloads(t *testing.T) {
	got := safeDiagnostic(assertionError("Authorization=top-secret rtsp://user:pass@camera/live payload"))
	for _, secret := range []string{"top-secret", "user:pass", "rtsp://", "payload"} {
		if strings.Contains(got, secret) {
			t.Fatalf("diagnostic leaked %q: %s", secret, got)
		}
	}
}

type assertionError string

func (e assertionError) Error() string { return string(e) }
