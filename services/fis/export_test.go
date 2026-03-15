package fis

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ExportedInMemoryBackend exposes the InMemoryBackend for tests.
type ExportedInMemoryBackend = InMemoryBackend

// ExperimentTemplateActionForTest exposes ExperimentTemplateAction for tests.
type ExperimentTemplateActionForTest = ExperimentTemplateAction

// ErrResourceNotFoundForTest exposes ErrResourceNotFound for tests.
var ErrResourceNotFoundForTest = ErrResourceNotFound

// NewTestBackend creates a new InMemoryBackend for testing.
func NewTestBackend() *InMemoryBackend {
	return NewInMemoryBackend("000000000000", "us-east-1")
}

// ParseISODurationForTest exposes parseISODuration for testing.
func ParseISODurationForTest(s string) time.Duration {
	return parseISODuration(s)
}

// ParsePercentageForTest exposes parsePercentage for testing.
func ParsePercentageForTest(s string) float64 {
	return parsePercentage(s)
}

// ParseOperationsForTest exposes parseOperations for testing.
func ParseOperationsForTest(s string) []string {
	return parseOperations(s)
}

// FaultErrorForActionForTest exposes faultErrorForAction for testing.
func FaultErrorForActionForTest(actionID string) chaos.FaultError {
	return faultErrorForAction(actionID)
}

// BuildFaultRulesForTest exposes buildFaultRules for testing.
func BuildFaultRulesForTest(action ExperimentTemplateAction) []chaos.FaultRule {
	return buildFaultRules(action)
}

// CreateTestEchoForExtract creates an echo.Context for testing ExtractOperation/ExtractResource.
func CreateTestEchoForExtract(t *testing.T, _ *Handler, method, path string) *echo.Context {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return c
}

// MockFISActionProvider is a test-only FISActionProvider that returns a configurable error.
type MockFISActionProvider struct {
	ExecErr     error
	Definitions []service.FISActionDefinition
	Calls       int
}

func (m *MockFISActionProvider) FISActions() []service.FISActionDefinition {
	return m.Definitions
}

func (m *MockFISActionProvider) ExecuteFISAction(_ context.Context, _ service.FISActionExecution) error {
	m.Calls++

	return m.ExecErr
}

// ErrMockAction is a sentinel error for mock action failures.
var ErrMockAction = errors.New("mock action failed")

// SetExperimentTerminal sets an experiment to a terminal state with a specific end time,
// cancelling the background goroutine to prevent races.
// Used only in tests.
func (b *InMemoryBackend) SetExperimentTerminal(id, status string, endTime time.Time) {
	b.mu.Lock("SetExperimentTerminal")

	exp, ok := b.experiments[id]
	if !ok {
		b.mu.Unlock()

		return
	}

	// Cancel the background goroutine before mutating so it cannot race
	// and overwrite the forced terminal values.
	if exp.cancel != nil {
		exp.cancel()
	}

	exp.Status = ExperimentStatus{Status: status}
	exp.EndTime = &endTime

	b.mu.Unlock()
}

// InjectExperiment inserts a pre-built experiment directly into the store without
// starting a background goroutine. This allows tests to create experiments in arbitrary
// terminal states without racing against the normal lifecycle goroutine.
// Used only in tests.
func (b *InMemoryBackend) InjectExperiment(exp *Experiment) {
	b.mu.Lock("InjectExperiment")
	defer b.mu.Unlock()

	b.experiments[exp.ID] = exp
}

// ExperimentCount returns the number of experiments stored.
// Used only in tests.
func (b *InMemoryBackend) ExperimentCount() int {
	b.mu.RLock("ExperimentCount")
	defer b.mu.RUnlock()

	return len(b.experiments)
}

// InjectCancel injects a cancel function for an experiment, simulating the one
// stored when StartExperiment creates a background goroutine.
// Used only in tests.
func (b *InMemoryBackend) InjectCancel(id string, cancel context.CancelFunc) {
	b.mu.Lock("InjectCancel")
	defer b.mu.Unlock()

	if exp, ok := b.experiments[id]; ok {
		exp.cancel = cancel
	}
}
