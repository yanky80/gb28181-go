package cascade

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	secretDiagnosticRE  = regexp.MustCompile(`(?i)(authorization|password|passwd|secret|token|credential|api[_-]?key)(\s*[:=]\s*|\s+)[^\s,;]+`)
	uriDiagnosticRE     = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s,;]+`)
	payloadDiagnosticRE = regexp.MustCompile(`(?i)\b(payload|raw\s+(?:sip|ipc|body))\b(?:\s*[:=]\s*)?[^\s,;]*`)
)

func safeErrorCode(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "unauthoriz"), strings.Contains(text, "forbidden"):
		return "unauthorized"
	case strings.Contains(text, "timeout"), strings.Contains(text, "deadline"):
		return "timeout"
	case strings.Contains(text, "closed"), strings.Contains(text, "eof"):
		return "connection_closed"
	default:
		return "operation_failed"
	}
}

// safeDiagnostic retains useful transport/store context while removing the
// values most likely to contain credentials or SIP payloads.
func safeDiagnostic(err error) string {
	if err == nil {
		return ""
	}
	text := safeText(err.Error())
	return fmt.Sprintf("%s: %s", safeErrorCode(err), text)
}

func safeText(text string) string {
	text = uriDiagnosticRE.ReplaceAllString(text, "[URL_REDACTED]")
	text = secretDiagnosticRE.ReplaceAllString(text, "[REDACTED]")
	text = payloadDiagnosticRE.ReplaceAllString(text, "[PAYLOAD_REDACTED]")
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 256 {
		text = text[:256]
	}
	return text
}
