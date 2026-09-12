package hygiene_test

import (
	"os"
	"strings"
	"testing"
)

func TestReleaseWorkflowPublishesGatewayArm64Metadata(t *testing.T) {
	workflow, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}

	raw := string(workflow)
	for _, contract := range []string{
		`if [ "${GOOS}" = "linux" ] && [ "${GOARCH}" = "arm64" ]; then`,
		`go build -trimpath -o "dist/gb-gateway" ./cmd/gb-gateway`,
		`gateway_sha256="$(sha256sum dist/gb-gateway | awk '{print $1}')"`,
		`source_commit=$(git rev-parse HEAD)`,
		`ipc_schema_version=1`,
		`config_schema_version=1`,
		`sha256sum * > SHA256SUMS`,
	} {
		if !strings.Contains(raw, contract) {
			t.Errorf("release workflow is missing %q", contract)
		}
	}
}
