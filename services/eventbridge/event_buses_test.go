package eventbridge_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestCreateEventBus_EnforcesLimit(t *testing.T) {
	t.Parallel()
	b := newBackend()

	const limit = 200
	for i := range limit {
		_, err := b.CreateEventBus(
			context.Background(),
			eventbridge.CreateEventBusParams{Name: fmt.Sprintf("bus-%d", i)},
		)
		require.NoError(t, err, "bus %d should be created", i)
	}

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "bus-overflow"})
	require.ErrorIs(t, err, eventbridge.ErrResourceLimitExceeded)
}

func TestTarget_RetryPolicyStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "r", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
			RetryPolicy: &eventbridge.RetryPolicy{
				MaximumRetryAttempts:     5,
				MaximumEventAgeInSeconds: 600,
			},
		},
	})
	require.NoError(t, err)

	targets, _, err := b.ListTargetsByRule(context.Background(), "r", "", "", 0)
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.NotNil(t, targets[0].RetryPolicy)
	assert.Equal(t, 5, targets[0].RetryPolicy.MaximumRetryAttempts)
	assert.Equal(t, 600, targets[0].RetryPolicy.MaximumEventAgeInSeconds)
}

func TestPutPermission_StoresStatement(t *testing.T) {
	t.Parallel()
	b := newBackend()

	err := b.PutPermission(context.Background(), eventbridge.PutPermissionInput{
		StatementID: "allow-account-123",
		Action:      "events:PutEvents",
		Principal:   "123456789013",
	})
	require.NoError(t, err)

	policy, err := b.GetEventBusPolicy(context.Background(), "")
	require.NoError(t, err)
	assert.Contains(t, policy, "allow-account-123")
}

func TestRemovePermission_RemovesStatement(t *testing.T) {
	t.Parallel()
	b := newBackend()

	require.NoError(t, b.PutPermission(context.Background(), eventbridge.PutPermissionInput{
		StatementID: "stmt-1",
		Action:      "events:PutEvents",
		Principal:   "111111111111",
	}))
	require.NoError(t, b.PutPermission(context.Background(), eventbridge.PutPermissionInput{
		StatementID: "stmt-2",
		Action:      "events:PutEvents",
		Principal:   "222222222222",
	}))

	require.NoError(
		t,
		b.RemovePermission(context.Background(), eventbridge.RemovePermissionInput{StatementID: "stmt-1"}),
	)

	policy, err := b.GetEventBusPolicy(context.Background(), "")
	require.NoError(t, err)
	assert.NotContains(t, policy, "stmt-1")
	assert.Contains(t, policy, "stmt-2")
}

func TestRemovePermission_RemoveAll(t *testing.T) {
	t.Parallel()
	b := newBackend()

	require.NoError(t, b.PutPermission(context.Background(), eventbridge.PutPermissionInput{
		StatementID: "stmt-1",
		Action:      "events:PutEvents",
		Principal:   "111111111111",
	}))

	require.NoError(t, b.RemovePermission(context.Background(), eventbridge.RemovePermissionInput{
		RemoveAllPermissions: true,
	}))

	policy, err := b.GetEventBusPolicy(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, policy)
}

func TestGetEventBusPolicy_BusNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.GetEventBusPolicy(context.Background(), "nonexistent")
	require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)
}

func TestPutEventBusPolicy_ReplacePolicy(t *testing.T) {
	t.Parallel()
	b := newBackend()

	policyJSON := `[{"Sid":"s1","Effect":"Allow","Action":"events:PutEvents","Principal":"123"}]`
	err := b.PutEventBusPolicy(context.Background(), eventbridge.PutEventBusPolicyInput{Policy: policyJSON})
	require.NoError(t, err)

	policy, err := b.GetEventBusPolicy(context.Background(), "")
	require.NoError(t, err)
	assert.Contains(t, policy, "s1")
}

func TestPutPermission_BusNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	err := b.PutPermission(context.Background(), eventbridge.PutPermissionInput{
		EventBusName: "no-such-bus",
		StatementID:  "s1",
		Action:       "events:PutEvents",
		Principal:    "123",
	})
	require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)
}

func TestTags_EventBus(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/default"

	rec := auditMakeRequest(t, h, e, "TagResource", map[string]any{
		"ResourceARN": busARN,
		"Tags":        []map[string]string{{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "platform"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": busARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "env")
	assert.Contains(t, rec.Body.String(), "platform")

	rec = auditMakeRequest(t, h, e, "UntagResource", map[string]any{
		"ResourceARN": busARN,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": busARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "\"env\"")
	assert.Contains(t, rec.Body.String(), "platform")
}

func TestEventBus_DefaultAlwaysPresent(t *testing.T) {
	t.Parallel()
	b := newBackend()

	bus, err := b.DescribeEventBus(context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, "default", bus.Name)
}

func TestEventBus_CannotDeleteDefault(t *testing.T) {
	t.Parallel()
	b := newBackend()

	err := b.DeleteEventBus(context.Background(), "default")
	require.ErrorIs(t, err, eventbridge.ErrCannotDeleteDefaultBus)
}

func TestEventBus_UpdateDescription(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(
		context.Background(),
		eventbridge.CreateEventBusParams{Name: "update-me", Description: "original"},
	)
	require.NoError(t, err)

	updated, err := b.UpdateEventBus(context.Background(), eventbridge.UpdateEventBusInput{
		Name:        "update-me",
		Description: "new description",
	})
	require.NoError(t, err)
	assert.Equal(t, "new description", updated.Description)
}

func TestEventBus_ListPagination(t *testing.T) {
	t.Parallel()
	b := newBackend()

	for i := range 5 {
		_, err := b.CreateEventBus(
			context.Background(),
			eventbridge.CreateEventBusParams{Name: fmt.Sprintf("page-bus-%d", i)},
		)
		require.NoError(t, err)
	}

	buses, _, err := b.ListEventBuses(context.Background(), "page-bus-", "", 0)
	require.NoError(t, err)
	assert.Len(t, buses, 5)
}

func TestEventBus_DeleteCleansUpRulesAndTargets(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "to-delete"})
	require.NoError(t, err)

	_, err = b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "orphan-rule",
		EventBusName: "to-delete",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "orphan-rule", "to-delete", []eventbridge.Target{
		{ID: "t1", Arn: "arn:aws:sqs:us-east-1:123456789012:q"},
	})
	require.NoError(t, err)

	err = b.DeleteEventBus(context.Background(), "to-delete")
	require.NoError(t, err)

	_, err = b.DescribeEventBus(context.Background(), "to-delete")
	require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)

	// Rules on deleted bus should also be gone.
	rules, _, err := b.ListRules(context.Background(), "to-delete", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, rules)
}

func TestCreateEventBus_RejectsAWSPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		busName string
	}{
		{
			name:    "aws.partner prefix rejected",
			busName: "aws.partner.saas.us-east-1.12345678",
			wantErr: eventbridge.ErrInvalidParameter,
		},
		{
			name:    "aws.events prefix rejected",
			busName: "aws.events",
			wantErr: eventbridge.ErrInvalidParameter,
		},
		{
			name:    "custom bus allowed",
			busName: "my-bus",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: tt.busName})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateEventBus_RejectsLongName(t *testing.T) {
	t.Parallel()
	b := newBackend()
	longName := make([]byte, 257)
	for i := range longName {
		longName[i] = 'x'
	}
	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: string(longName)})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

// TestPutTargets_RetryPolicyBounds proves PutTargets enforces
// AWS's documented RetryPolicy bounds (MaximumRetryAttempts 0-185,
// MaximumEventAgeInSeconds 60-86400 when set), which the client SDK does not
// validate locally (no smithy range trait on these fields), so the backend
// must enforce them itself to reject out-of-range values the way the real
// service does.
func TestPutTargets_RetryPolicyBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		policy  eventbridge.RetryPolicy
		wantErr bool
	}{
		{"zero attempts and unset age ok", eventbridge.RetryPolicy{MaximumRetryAttempts: 0}, false},
		{"max attempts ok", eventbridge.RetryPolicy{MaximumRetryAttempts: 185}, false},
		{"attempts over max rejected", eventbridge.RetryPolicy{MaximumRetryAttempts: 186}, true},
		{"negative attempts rejected", eventbridge.RetryPolicy{MaximumRetryAttempts: -1}, true},
		{"min age ok", eventbridge.RetryPolicy{MaximumEventAgeInSeconds: 60}, false},
		{"max age ok", eventbridge.RetryPolicy{MaximumEventAgeInSeconds: 86400}, false},
		{"age under min rejected", eventbridge.RetryPolicy{MaximumEventAgeInSeconds: 59}, true},
		{"age over max rejected", eventbridge.RetryPolicy{MaximumEventAgeInSeconds: 86401}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name: "r", EventPattern: `{"source":["x"]}`,
			})
			require.NoError(t, err)

			policy := tt.policy
			failed, err := b.PutTargets(context.Background(), "r", "", []eventbridge.Target{
				{ID: "t1", Arn: "arn:aws:sqs:us-east-1:123456789012:q", RetryPolicy: &policy},
			})
			require.NoError(t, err)
			if tt.wantErr {
				require.Len(t, failed, 1)
				assert.Equal(t, "InvalidParameter", failed[0].ErrorCode)

				return
			}
			assert.Empty(t, failed)
		})
	}
}

func TestUpdateEventBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    eventbridge.UpdateEventBusInput
		wantErr  error
		wantDesc string
	}{
		{
			name:    "not found returns error",
			input:   eventbridge.UpdateEventBusInput{Name: "no-such"},
			wantErr: eventbridge.ErrEventBusNotFound,
		},
		{
			name:     "updates description on default bus",
			input:    eventbridge.UpdateEventBusInput{Name: "", Description: "new"},
			wantDesc: "new",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			got, err := b.UpdateEventBus(context.Background(), tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, got.Description)
		})
	}
}

func TestPutRemovePermission_NoOp(t *testing.T) {
	t.Parallel()
	b := newBackend()
	require.NoError(t, b.PutPermission(context.Background(), eventbridge.PutPermissionInput{StatementID: "s1"}))
	require.NoError(t, b.RemovePermission(context.Background(), eventbridge.RemovePermissionInput{StatementID: "s1"}))
}

// TestListEventBuses_RespectsLimit verifies that the Limit field in
// ListEventBuses requests controls the page size. Real AWS honors this field.
func TestListEventBuses_RespectsLimit(t *testing.T) {
	t.Parallel()

	// Note: NewInMemoryBackendWithConfig pre-creates the default bus, so total = busCount+1.
	tests := []struct {
		name      string
		busCount  int
		limit     int
		wantCount int
		wantToken bool
	}{
		{
			name:      "limit_smaller_than_total",
			busCount:  5,
			limit:     3,
			wantCount: 3, // 6 total (5 custom + default), page size 3
			wantToken: true,
		},
		{
			name:      "limit_equals_total",
			busCount:  5,
			limit:     6,
			wantCount: 6, // exactly 6 total — no next token
			wantToken: false,
		},
		{
			name:      "limit_zero_uses_default",
			busCount:  5,
			limit:     0,
			wantCount: 6, // default limit 100 covers all 6
			wantToken: false,
		},
		{
			name:      "limit_larger_than_total",
			busCount:  2,
			limit:     10,
			wantCount: 3, // 3 total (2 custom + default) — all fit
			wantToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			for i := range tt.busCount {
				_, err := b.CreateEventBus(
					context.Background(),
					eventbridge.CreateEventBusParams{Name: strings.Repeat("a", i+1) + "-bus"},
				)
				require.NoError(t, err)
			}

			page, token, err := b.ListEventBuses(context.Background(), "", "", tt.limit)
			require.NoError(t, err)
			assert.Len(t, page, tt.wantCount)
			if tt.wantToken {
				assert.NotEmpty(t, token, "expected a next-page token")
			} else {
				assert.Empty(t, token, "expected no next-page token")
			}
		})
	}
}

// TestListEventBuses_TokenIsOpaque verifies that next-page tokens
// are base64-encoded (opaque). Real AWS tokens are not plain integers.
func TestListEventBuses_TokenIsOpaque(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	for i := range 5 {
		_, err := b.CreateEventBus(
			context.Background(),
			eventbridge.CreateEventBusParams{Name: strings.Repeat("z", i+1) + "-bus"},
		)
		require.NoError(t, err)
	}

	_, token, err := b.ListEventBuses(context.Background(), "", "", 3)
	require.NoError(t, err)
	require.NotEmpty(t, token, "expected a pagination token")

	// Token must be valid base64.
	decoded, decodeErr := base64.StdEncoding.DecodeString(token)
	require.NoError(t, decodeErr, "next-page token must be base64-encoded")

	// The decoded value should be a non-negative integer (offset).
	assert.NotEmpty(t, string(decoded))
}

// TestListEventBuses_PaginationFollowsToken verifies that providing the
// token from page 1 returns the correct page 2.
func TestListEventBuses_PaginationFollowsToken(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// Create 5 buses; default bus makes 6 total.
	for i := range 5 {
		_, err := b.CreateEventBus(
			context.Background(),
			eventbridge.CreateEventBusParams{Name: strings.Repeat("b", i+1) + "-bus"},
		)
		require.NoError(t, err)
	}

	page1, token, err := b.ListEventBuses(context.Background(), "", "", 4)
	require.NoError(t, err)
	require.NotEmpty(t, token)
	assert.Len(t, page1, 4)

	page2, token2, err := b.ListEventBuses(context.Background(), "", token, 4)
	require.NoError(t, err)
	assert.Empty(t, token2)
	assert.Len(t, page2, 2) // 6 total - 4 on page 1 = 2 remaining

	// Pages must not overlap.
	page1Names := make(map[string]bool, len(page1))
	for _, bus := range page1 {
		page1Names[bus.Name] = true
	}
	for _, bus := range page2 {
		assert.False(t, page1Names[bus.Name], "page 2 item %q appeared on page 1", bus.Name)
	}
}

// TestCreateEventBus_QuotaIsPerAccount verifies that the 200-bus limit
// applies across all regions for the same account. Real AWS enforces per-account limits.
func TestCreateEventBus_QuotaIsPerAccount(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	// Fill up to the limit (200) using buses spread across two regions.
	eastCtx := regionCtx("us-east-1")
	westCtx := regionCtx("us-west-2")

	for i := range 100 {
		_, err := b.CreateEventBus(eastCtx, eventbridge.CreateEventBusParams{Name: strings.Repeat("e", i+1) + "-east"})
		require.NoError(t, err)
	}
	for i := range 100 {
		_, err := b.CreateEventBus(westCtx, eventbridge.CreateEventBusParams{Name: strings.Repeat("w", i+1) + "-west"})
		require.NoError(t, err)
	}

	// 201st bus in any region must fail — quota is per-account.
	_, err := b.CreateEventBus(eastCtx, eventbridge.CreateEventBusParams{Name: "one-too-many"})
	require.Error(t, err)
	assert.ErrorIs(t, err, eventbridge.ErrResourceLimitExceeded)
}
