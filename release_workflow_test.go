package hygiene_test

import (
	"os"
	"strings"
	"testing"
)

func TestReleaseWorkflowPublishesGatewayArm64Metadata(t *testing.T) {
	raw := readWorkflow(t, ".github/workflows/release.yml")
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

func TestReleaseAndCIUseValidatedGoToolchain(t *testing.T) {
	ci := readWorkflow(t, ".github/workflows/ci.yml")
	if !strings.Contains(ci, "go-version: '1.26.5'") || !strings.Contains(ci, "go-version: ['1.26.5', '1.27.x']") {
		t.Fatal("CI must lint on Go 1.26.5 and test both validated Go versions")
	}
	if got := strings.Count(readWorkflow(t, ".github/workflows/release.yml"), "go-version: '1.26'"); got != 1 {
		t.Fatalf("release preflight must pin its test job to Go 1.26, found %d pins", got)
	}
}

func readWorkflow(t *testing.T, path string) string {
	t.Helper()
	workflow, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(workflow)
}
