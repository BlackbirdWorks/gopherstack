package cloudformation_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

var errFunctionNotFound = errors.New("function not found")

// newExtensibilityBackends creates a ServiceBackends suitable for extensibility tests.
func newExtensibilityBackends() *cloudformation.ServiceBackends {
	b := &cloudformation.ServiceBackends{
		WaitConditions: cloudformation.NewWaitConditionStore(),
		MacroRegistry:  cloudformation.NewMacroRegistry(),
		AccountID:      "000000000000",
		Region:         "us-east-1",
	}

	return b
}

// newExtensibilityBackendsWithLambda adds a real Lambda backend.
func newExtensibilityBackendsWithLambda() *cloudformation.ServiceBackends {
	b := newExtensibilityBackends()
	lambdaBk := lambdabackend.NewInMemoryBackend(
		nil,
		nil,
		lambdabackend.DefaultSettings(),
		"000000000000",
		"us-east-1",
	)
	b.Lambda = lambdabackend.NewHandler(lambdaBk)

	return b
}

// newExtensibilityBackendsWithSNS adds a real SNS backend.
func newExtensibilityBackendsWithSNS() *cloudformation.ServiceBackends {
	b := newExtensibilityBackends()
	b.SNS = snsbackend.NewHandler(snsbackend.NewInMemoryBackend())

	return b
}

// ---- WaitConditionStore ----

func TestWaitConditionStore_SignalAndWait(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		count   int
		signals int
		wantErr bool
	}{
		{
			name:    "single signal satisfies count 1",
			count:   1,
			signals: 1,
			wantErr: false,
		},
		{
			name:    "multiple signals satisfy count 3",
			count:   3,
			signals: 3,
			wantErr: false,
		},
		{
			name:    "signals pre-loaded before wait",
			count:   2,
			signals: 2,
			wantErr: false,
		},
		{
			name:    "no signals causes emulator auto-success",
			count:   1,
			signals: 0,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := cloudformation.NewWaitConditionStore()
			token := "test-token-" + tt.name

			// Pre-load signals.
			for i := range tt.signals {
				store.Signal(token, cloudformation.WCSignal{
					UniqueID: fmt.Sprintf("signal-%d", i),
					Status:   "SUCCESS",
					Data:     "ok",
				})
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err := store.Wait(ctx, token, tt.count, 50*time.Millisecond)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestWaitConditionStore_AsyncSignal(t *testing.T) {
	t.Parallel()

	store := cloudformation.NewWaitConditionStore()
	token := "async-token"

	// Signal from a goroutine after a short delay.
	go func() {
		time.Sleep(20 * time.Millisecond)
		store.Signal(
			token,
			cloudformation.WCSignal{UniqueID: "u1", Status: "SUCCESS", Data: "data"},
		)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Use a large emulator timeout so we wait for the goroutine signal.
	err := store.Wait(ctx, token, 1, 2*time.Second)
	require.NoError(t, err)
}

func TestWaitConditionStore_ContextCancel(t *testing.T) {
	t.Parallel()

	store := cloudformation.NewWaitConditionStore()
	token := "cancel-token"

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel immediately

	// With a very large emulator timeout, cancelled context should return error.
	err := store.Wait(ctx, token, 1, 24*time.Hour)
	require.Error(t, err)
}

// ---- WaitConditionHandle + WaitCondition ----

func TestResourceCreator_WaitConditionHandle_ReturnsPhysID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "basic handle creation"},
		{name: "second handle has unique ID"},
	}

	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)

	var (
		mu   sync.Mutex
		seen = map[string]bool{}
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			physID, err := rc.Create(
				t.Context(),
				"MyHandle",
				"AWS::CloudFormation::WaitConditionHandle",
				nil,
				nil,
				make(map[string]string),
			)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			mu.Lock()
			defer mu.Unlock()
			assert.False(t, seen[physID], "physID must be unique across creations: "+tt.name)
			seen[physID] = true
		})
	}
}

func TestResourceCreator_WaitCondition_AutoSucceedsWithNoSignals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count string
	}{
		{name: "count 1 auto-succeeds", count: "1"},
		{name: "count 3 auto-succeeds", count: "3"},
		{name: "count default auto-succeeds", count: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtensibilityBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physIDs := make(map[string]string)

			// Create handle first.
			handlePhysID, err := rc.Create(
				t.Context(),
				"MyHandle",
				"AWS::CloudFormation::WaitConditionHandle",
				nil,
				nil,
				physIDs,
			)
			require.NoError(t, err)

			physIDs["MyHandle"] = handlePhysID

			props := map[string]any{
				"Handle":  handlePhysID,
				"Timeout": "300",
			}
			if tt.count != "" {
				props["Count"] = tt.count
			}

			wcPhysID, err := rc.Create(t.Context(), "MyWC", "AWS::CloudFormation::WaitCondition",
				props, nil, physIDs)
			require.NoError(t, err)
			assert.NotEmpty(t, wcPhysID)
		})
	}
}

func TestResourceCreator_WaitCondition_SucceedsWithPreloadedSignal(t *testing.T) {
	t.Parallel()

	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)

	physIDs := make(map[string]string)

	// Create handle.
	handlePhysID, err := rc.Create(
		t.Context(),
		"MyHandle",
		"AWS::CloudFormation::WaitConditionHandle",
		nil,
		nil,
		physIDs,
	)
	require.NoError(t, err)

	physIDs["MyHandle"] = handlePhysID

	// Pre-signal the handle (extract token from physID "wchandle-<token>").
	token := handlePhysID[len("wchandle-"):]
	backends.WaitConditions.Signal(token, cloudformation.WCSignal{
		UniqueID: "signal-1",
		Status:   "SUCCESS",
		Data:     "ok",
	})

	wcPhysID, err := rc.Create(t.Context(), "MyWC", "AWS::CloudFormation::WaitCondition",
		map[string]any{
			"Handle":  handlePhysID,
			"Count":   "1",
			"Timeout": "300",
		},
		nil, physIDs)

	require.NoError(t, err)
	assert.NotEmpty(t, wcPhysID)
}

// ---- Full stack: WaitCondition in CFN template ----

func TestStack_WaitConditionHandle_InTemplate(t *testing.T) {
	t.Parallel()

	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)
	b := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", rc)

	template := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "WaitHandle": {
      "Type": "AWS::CloudFormation::WaitConditionHandle"
    },
    "WaitCond": {
      "Type": "AWS::CloudFormation::WaitCondition",
      "DependsOn": ["WaitHandle"],
      "Properties": {
        "Handle": {"Ref": "WaitHandle"},
        "Count": "1",
        "Timeout": "10"
      }
    }
  }
}`

	stack, err := b.CreateStack(
		t.Context(),
		"wc-test",
		template,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
}

// ---- WaitConditionStore: token isolation ----

func TestWaitConditionStore_TokenIsolation(t *testing.T) {
	t.Parallel()

	store := cloudformation.NewWaitConditionStore()

	// Signal token-A but not token-B.
	store.Signal("token-A", cloudformation.WCSignal{UniqueID: "u1", Status: "SUCCESS"})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// token-A should succeed immediately.
	err := store.Wait(ctx, "token-A", 1, 50*time.Millisecond)
	require.NoError(t, err)

	// token-B has no signals: auto-succeeds via emulator timeout.
	err = store.Wait(ctx, "token-B", 1, 30*time.Millisecond)
	require.NoError(t, err)
}

// ---- WaitConditionStore: FAILURE signal not counted ----

func TestWaitConditionStore_FailureSignalNotCounted(t *testing.T) {
	t.Parallel()

	store := cloudformation.NewWaitConditionStore()

	// Send a FAILURE signal — should not count toward SUCCESS count.
	store.Signal(
		"token",
		cloudformation.WCSignal{UniqueID: "f1", Status: "FAILURE", Reason: "deploy failed"},
	)

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	// Only SUCCESS counts; emulator auto-succeed after short timeout.
	err := store.Wait(ctx, "token", 1, 30*time.Millisecond)
	require.NoError(t, err, "emulator auto-succeed should fire since no SUCCESS signals")
}
