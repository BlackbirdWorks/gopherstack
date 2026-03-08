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
