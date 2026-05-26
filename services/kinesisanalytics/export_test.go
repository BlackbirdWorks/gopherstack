package kinesisanalytics

import (
	"testing"
	"time"
)

// ApplicationCount returns the number of applications stored in the backend.
// This is exported for use in tests only.
func ApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.apps)
}

// HandlerOpsLen returns the number of operations pre-built in the handler dispatch map.
// This is exported for use in tests only.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

// CreateApp is a test helper that calls CreateApplication with the minimal (legacy) signature.
func CreateApp(b *InMemoryBackend, region, accountID, name, description, code string, tags map[string]string) (*Application, error) {
	return b.CreateApplication(region, accountID, name, description, code, "", nil, nil, nil, tags)
}

// StartAppNoConfig is a test helper that calls StartApplication with no InputConfigurations.
func StartAppNoConfig(b *InMemoryBackend, name string) error {
	return b.StartApplication(name, nil)
}

// UpdateAppCode is a test helper that calls UpdateApplication with just a code update.
func UpdateAppCode(b *InMemoryBackend, name string, versionID int64, code string) (*Application, error) {
	upd := &applicationUpdate{ApplicationCodeUpdate: code}

	return b.UpdateApplication(name, versionID, upd)
}

// WaitForStatus polls until the application reaches wantStatus or the test times out.
func WaitForStatus(t *testing.T, b *InMemoryBackend, name, wantStatus string) {
	t.Helper()

	deadline := time.Now().Add(500 * time.Millisecond)

	for time.Now().Before(deadline) {
		app, err := b.DescribeApplication(name)
		if err == nil && app.ApplicationStatus == wantStatus {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("application %q did not reach status %q within timeout", name, wantStatus)
}

// GetCancelFuncsLen returns the number of in-flight lifecycle goroutines.
func GetCancelFuncsLen(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.cancelFuncs)
}
