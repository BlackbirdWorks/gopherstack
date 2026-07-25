package ssm_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// fakeParameterPolicyNotifier is an in-memory ssm.ParameterPolicyNotifier
// double that records every call, standing in for the real EventBridge
// adapter (services/eventbridge's ssm_integration.go) so tests can assert on
// exactly what SSM would have published without any cross-service wiring.
type fakeParameterPolicyNotifier struct {
	calls []fakePolicyNotifyCall
	mu    sync.Mutex
}

type fakePolicyNotifyCall struct {
	parameterName string
	policyType    string
}

func (f *fakeParameterPolicyNotifier) NotifyParameterPolicyAction(
	_ context.Context,
	parameterName, policyType string,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, fakePolicyNotifyCall{parameterName: parameterName, policyType: policyType})

	return nil
}

func (f *fakeParameterPolicyNotifier) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.calls)
}

// TestInMemoryBackend_HistoryCap verifies that PutParameter caps history to
// MaxHistoryCap entries, evicting the oldest entries on overflow.
func TestInMemoryBackend_HistoryCap(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()

	// Insert MaxHistoryCap + 10 versions.
	total := ssm.MaxHistoryCap + 10
	for i := range total {
		_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name:      "/capped/param",
			Type:      "String",
			Value:     "v",
			Overwrite: i > 0,
		})
		require.NoError(t, err)
	}

	assert.Equal(t, ssm.MaxHistoryCap, b.HistoryLen("/capped/param"))

	// The history returned should have MaxHistoryCap entries (newest first).
	out, err := b.GetParameterHistory(context.TODO(), &ssm.GetParameterHistoryInput{Name: "/capped/param"})
	require.NoError(t, err)

	// GetParameterHistory caps at 50 by default, so just verify the newest version is present.
	assert.NotEmpty(t, out.Parameters)
	assert.Equal(t, int64(total), out.Parameters[0].Version)
}

// TestInMemoryBackend_DeleteCleansHistory verifies that deleting a parameter
// also removes its history and tags entries.
func TestInMemoryBackend_DeleteCleansHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "single_delete"},
		{name: "multi_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			paramName := "/delete/test-" + tt.name

			_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
				Name:  paramName,
				Type:  "String",
				Value: "hello",
			})
			require.NoError(t, err)
			_, err = b.PutParameter(context.TODO(), &ssm.PutParameterInput{
				Name:      paramName,
				Type:      "String",
				Value:     "world",
				Overwrite: true,
			})
			require.NoError(t, err)

			// Add a tag so we can verify it gets cleaned up on delete.
			err = b.AddTagsToResource(context.TODO(), &ssm.AddTagsToResourceInput{
				ResourceType: "Parameter",
				ResourceID:   paramName,
				Tags:         []ssm.Tag{{Key: "env", Value: "test"}},
			})
			require.NoError(t, err)

			assert.Equal(t, 2, b.HistoryLen(paramName))
			assert.True(t, b.HasTagEntry(paramName), "tag entry should exist before delete")

			if tt.name == "single_delete" {
				_, err = b.DeleteParameter(context.TODO(), &ssm.DeleteParameterInput{Name: paramName})
			} else {
				_, err = b.DeleteParameters(context.TODO(), &ssm.DeleteParametersInput{Names: []string{paramName}})
			}

			require.NoError(t, err)
			assert.Equal(t, 0, b.HistoryLen(paramName))
			assert.False(t, b.HasTagEntry(paramName), "tag entry should be removed after delete")

			// Re-create the parameter and confirm no stale tags bleed through.
			_, err = b.PutParameter(context.TODO(), &ssm.PutParameterInput{
				Name:  paramName,
				Type:  "String",
				Value: "fresh",
			})
			require.NoError(t, err)

			tagsOut, err := b.ListTagsForResource(context.TODO(), &ssm.ListTagsForResourceInput{
				ResourceType: "Parameter",
				ResourceID:   paramName,
			})
			require.NoError(t, err)
			assert.Empty(t, tagsOut.TagList, "no stale tags should appear on recreated parameter")
		})
	}
}

// TestJanitor_SweepsExpiredCommands verifies that the janitor removes commands
// whose ExpiresAfter is in the past together with their invocations.
func TestJanitor_SweepsExpiredCommands(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()

	// AWS-RunShellScript is pre-registered as a default document.
	out1, err := b.SendCommand(context.TODO(), &ssm.SendCommandInput{
		DocumentName: "AWS-RunShellScript",
		InstanceIDs:  []string{"i-1111"},
	})
	require.NoError(t, err)

	out2, err := b.SendCommand(context.TODO(), &ssm.SendCommandInput{
		DocumentName: "AWS-RunShellScript",
		InstanceIDs:  []string{"i-2222"},
	})
	require.NoError(t, err)

	// Force the first command into the past.
	b.SetCommandExpiresAfter(out1.Command.CommandID, float64(time.Now().Add(-1*time.Second).Unix()))

	assert.Equal(t, 2, b.CommandCount())
	assert.Equal(t, 2, b.CommandInvocationCount())

	// Run the janitor once with a very short interval so it fires quickly.
	j := ssm.NewJanitor(b, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	go j.Run(ctx)

	// Wait until the expired command is gone.
	require.Eventually(t, func() bool {
		return b.CommandCount() == 1
	}, 2*time.Second, 20*time.Millisecond)

	assert.Equal(t, 1, b.CommandInvocationCount())

	// The non-expired command must still exist.
	listOut, err := b.ListCommands(context.TODO(), &ssm.ListCommandsInput{CommandID: out2.Command.CommandID})
	require.NoError(t, err)
	require.Len(t, listOut.Commands, 1)
}

// TestHandler_WithJanitor_StartWorker verifies that StartWorker can be called
// on a handler that has a janitor attached without error.
func TestHandler_WithJanitor_StartWorker(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	h := ssm.NewHandler(b).WithJanitor(10 * time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err)
}

// TestInMemoryBackend_ResetRestoresDefaultDocuments verifies that Reset() clears user
// state but re-registers the built-in AWS documents so they remain available.
func TestInMemoryBackend_ResetRestoresDefaultDocuments(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()

	// Create a parameter and a user document to verify both are cleared by Reset.
	_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "my-param",
		Value: "value",
		Type:  "String",
	})
	require.NoError(t, err)

	_, err = b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "MyUserDoc",
		Content: "--- user",
	})
	require.NoError(t, err)

	// Reset should clear user data.
	b.Reset()

	// Parameter must be gone.
	_, err = b.GetParameter(context.TODO(), &ssm.GetParameterInput{Name: "my-param"})
	require.Error(t, err)

	// User document must be gone.
	_, err = b.GetDocument(context.TODO(), &ssm.GetDocumentInput{Name: "MyUserDoc"})
	require.Error(t, err)

	// Default AWS document must still be present so SendCommand works.
	_, err = b.SendCommand(context.TODO(), &ssm.SendCommandInput{
		DocumentName: "AWS-RunShellScript",
		InstanceIDs:  []string{"i-1234567890abcdef0"},
	})
	require.NoError(t, err, "default document AWS-RunShellScript must be available after Reset")
}

// TestInMemoryBackend_DocumentVersionCap verifies that UpdateDocument caps the
// stored version list to MaxDocumentVersionCap entries, evicting the oldest.
func TestInMemoryBackend_DocumentVersionCap(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()

	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "CapTestDoc",
		Content: "--- init",
	})
	require.NoError(t, err)

	// Apply MaxDocumentVersionCap + 5 updates so the cap must evict old versions.
	total := ssm.MaxDocumentVersionCap + 5
	for i := range total {
		_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
			Name:    "CapTestDoc",
			Content: fmt.Sprintf("--- version: %d", i),
		})
		require.NoError(t, err)
	}

	assert.Equal(t, ssm.MaxDocumentVersionCap, b.DocumentVersionCount("CapTestDoc"))

	// The most recent version should be retrievable via ListDocumentVersions.
	out, err := b.ListDocumentVersions(context.TODO(), &ssm.ListDocumentVersionsInput{Name: "CapTestDoc"})
	require.NoError(t, err)
	assert.NotEmpty(t, out.DocumentVersions)
}

// TestSSMJanitor_TaskTimeout_WithJanitor verifies that WithJanitor propagates
// the variadic taskTimeout into the janitor's TaskTimeout field.
func TestSSMJanitor_TaskTimeout_WithJanitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskTimeout time.Duration
		want        time.Duration
	}{
		{
			name:        "no_timeout_zero",
			taskTimeout: 0,
			want:        0,
		},
		{
			name:        "with_30s_timeout",
			taskTimeout: 30 * time.Second,
			want:        30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			h := ssm.NewHandler(b).WithJanitor(time.Minute, tt.taskTimeout)

			assert.Equal(t, tt.want, h.GetJanitorTaskTimeout())
		})
	}
}

// TestSSMJanitor_SweepOnce_EvictsExpiredCommands verifies that SweepOnce removes
// expired commands without requiring the janitor loop to run.
func TestSSMJanitor_SweepOnce_EvictsExpiredCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		expiredCount   int
		unexpiredCount int
	}{
		{
			name:           "single_expired",
			expiredCount:   1,
			unexpiredCount: 0,
		},
		{
			name:           "mixed_expired_and_alive",
			expiredCount:   2,
			unexpiredCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()

			for range tt.expiredCount {
				out, err := b.SendCommand(context.TODO(), &ssm.SendCommandInput{
					DocumentName: "AWS-RunShellScript",
					InstanceIDs:  []string{"i-1111"},
				})
				require.NoError(t, err)
				b.SetCommandExpiresAfter(out.Command.CommandID, float64(time.Now().Add(-time.Second).Unix()))
			}

			for range tt.unexpiredCount {
				_, err := b.SendCommand(context.TODO(), &ssm.SendCommandInput{
					DocumentName: "AWS-RunShellScript",
					InstanceIDs:  []string{"i-2222"},
				})
				require.NoError(t, err)
			}

			j := ssm.NewJanitor(b, time.Minute)
			j.SweepOnce(t.Context())

			assert.Equal(t, tt.unexpiredCount, b.CommandCount())
		})
	}
}

// TestSSMJanitor_DefaultInterval verifies that a zero interval in WithJanitor
// results in the default interval being used.
func TestSSMJanitor_DefaultInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{
			name:     "zero_uses_default",
			interval: 0,
			want:     ssm.DefaultJanitorInterval,
		},
		{
			name:     "custom_interval_propagated",
			interval: 5 * time.Minute,
			want:     5 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ssm.NewHandler(ssm.NewInMemoryBackend())
			h.WithJanitor(tt.interval)

			assert.Equal(t, tt.want, h.GetJanitorInterval())
		})
	}
}

// TestSSMJanitor_ParameterPolicyNotifications proves the previously-entirely-
// unevaluated NoChangeNotification/ExpirationNotification parameter policies
// (PARITY.md gap, bd tracked as part of the parameter-store family) now
// really do drive a ParameterPolicyNotifier call once a policy becomes due,
// with correct dedupe/reset/cascade-cleanup semantics -- not just that the
// Policies string round-trips.
//
// "Due" is engineered without waiting real wall-clock hours/days by using a
// zero "After"/"Before" amount (fires the instant LastModifiedDate --
// respectively a just-in-the-future Expiration timestamp -- is in the past),
// matching real AWS's own periodic best-effort scan semantics, not a fixed
// polling delay this test would otherwise have to sleep through.
func TestSSMJanitor_ParameterPolicyNotifications(t *testing.T) {
	t.Parallel()

	putAdvanced := func(t *testing.T, b *ssm.InMemoryBackend, name, policies string, overwrite bool) {
		t.Helper()

		_, err := b.PutParameter(context.Background(), &ssm.PutParameterInput{
			Name:      name,
			Type:      "String",
			Value:     "v",
			Tier:      "Advanced",
			Policies:  policies,
			Overwrite: overwrite,
		})
		require.NoError(t, err)
	}

	noChangeDuePolicy := `[{"Type":"NoChangeNotification","Version":"1.0","Attributes":{"After":"0","Unit":"Hours"}}]`
	noChangeNotDuePolicy := `[{"Type":"NoChangeNotification","Version":"1.0","Attributes":{"After":"9999","Unit":"Days"}}]`
	expirationNotificationDue := func() string {
		expiresAt := time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339)

		return `[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":"` + expiresAt + `"}},` +
			`{"Type":"ExpirationNotification","Version":"1.0","Attributes":{"Before":"1","Unit":"Hours"}}]`
	}

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "no_change_notification_fires_when_due",
			run: func(t *testing.T) {
				t.Helper()

				b := ssm.NewInMemoryBackend()
				notifier := &fakeParameterPolicyNotifier{}
				b.SetParameterPolicyNotifier(notifier)

				putAdvanced(t, b, "/app/no-change", noChangeDuePolicy, false)

				j := ssm.NewJanitor(b, time.Minute)
				j.SweepOnce(t.Context())

				require.Equal(t, 1, notifier.callCount())
				assert.Equal(t, "/app/no-change", notifier.calls[0].parameterName)
				assert.Equal(t, "NoChangeNotification", notifier.calls[0].policyType)
			},
		},
		{
			name: "expiration_notification_fires_when_due",
			run: func(t *testing.T) {
				t.Helper()

				b := ssm.NewInMemoryBackend()
				notifier := &fakeParameterPolicyNotifier{}
				b.SetParameterPolicyNotifier(notifier)

				putAdvanced(t, b, "/app/expiring", expirationNotificationDue(), false)

				j := ssm.NewJanitor(b, time.Minute)
				j.SweepOnce(t.Context())

				require.Equal(t, 1, notifier.callCount())
				assert.Equal(t, "/app/expiring", notifier.calls[0].parameterName)
				assert.Equal(t, "ExpirationNotification", notifier.calls[0].policyType)

				// The parameter must still exist -- it isn't due to actually
				// expire (and be deleted by the separate Expiration sweep)
				// for a while yet, only the advance-notice threshold fired.
				_, err := b.GetParameter(context.Background(), &ssm.GetParameterInput{Name: "/app/expiring"})
				require.NoError(t, err)
			},
		},
		{
			name: "not_yet_due_never_fires",
			run: func(t *testing.T) {
				t.Helper()

				b := ssm.NewInMemoryBackend()
				notifier := &fakeParameterPolicyNotifier{}
				b.SetParameterPolicyNotifier(notifier)

				putAdvanced(t, b, "/app/far-future", noChangeNotDuePolicy, false)

				j := ssm.NewJanitor(b, time.Minute)
				j.SweepOnce(t.Context())

				assert.Equal(t, 0, notifier.callCount())
			},
		},
		{
			name: "no_notifier_configured_is_a_safe_noop",
			run: func(t *testing.T) {
				t.Helper()

				b := ssm.NewInMemoryBackend()
				// Deliberately never call SetParameterPolicyNotifier.
				putAdvanced(t, b, "/app/unconfigured", noChangeDuePolicy, false)

				j := ssm.NewJanitor(b, time.Minute)
				require.NotPanics(t, func() { j.SweepOnce(t.Context()) })
			},
		},
		{
			name: "dedup_does_not_refire_on_second_sweep",
			run: func(t *testing.T) {
				t.Helper()

				b := ssm.NewInMemoryBackend()
				notifier := &fakeParameterPolicyNotifier{}
				b.SetParameterPolicyNotifier(notifier)

				putAdvanced(t, b, "/app/dedup", noChangeDuePolicy, false)

				j := ssm.NewJanitor(b, time.Minute)
				j.SweepOnce(t.Context())
				j.SweepOnce(t.Context())
				j.SweepOnce(t.Context())

				assert.Equal(t, 1, notifier.callCount(),
					"a policy instance must notify at most once until the parameter is re-written")
			},
		},
		{
			name: "put_parameter_resets_dedupe_state",
			run: func(t *testing.T) {
				t.Helper()

				b := ssm.NewInMemoryBackend()
				notifier := &fakeParameterPolicyNotifier{}
				b.SetParameterPolicyNotifier(notifier)

				putAdvanced(t, b, "/app/rewrite", noChangeDuePolicy, false)

				j := ssm.NewJanitor(b, time.Minute)
				j.SweepOnce(t.Context())
				require.Equal(t, 1, notifier.callCount())

				// Real AWS: "If you change or edit a parameter, the system
				// resets the notification time period." A fresh write must
				// make the (still-due, After=0) policy eligible again.
				putAdvanced(t, b, "/app/rewrite", noChangeDuePolicy, true)
				j.SweepOnce(t.Context())

				assert.Equal(t, 2, notifier.callCount())
			},
		},
		{
			name: "delete_then_recreate_leaves_no_ghost_dedupe_state",
			run: func(t *testing.T) {
				t.Helper()

				b := ssm.NewInMemoryBackend()
				notifier := &fakeParameterPolicyNotifier{}
				b.SetParameterPolicyNotifier(notifier)

				putAdvanced(t, b, "/app/recreate", noChangeDuePolicy, false)

				j := ssm.NewJanitor(b, time.Minute)
				j.SweepOnce(t.Context())
				require.Equal(t, 1, notifier.callCount())

				_, err := b.DeleteParameter(context.Background(), &ssm.DeleteParameterInput{Name: "/app/recreate"})
				require.NoError(t, err)

				putAdvanced(t, b, "/app/recreate", noChangeDuePolicy, false)
				j.SweepOnce(t.Context())

				assert.Equal(t, 2, notifier.callCount(),
					"deleting and recreating a parameter must not leave stale notified-state behind")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t)
		})
	}
}
