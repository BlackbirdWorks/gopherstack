package stepfunctions_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// ─── CreateStateMachine Idempotency ──────────────────────────────────────────

func TestAudit2_CreateStateMachine_Idempotent_SameParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		smType string
	}{
		{name: "standard_type", smType: "STANDARD"},
		{name: "express_type", smType: "EXPRESS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			sm1, err := b.CreateStateMachine(context.Background(), "idem-sm-"+tt.name, minimalDefinition, validRoleARN, tt.smType)
			require.NoError(t, err)

			// Same name+def+roleArn+type → idempotent, returns same ARN.
			sm2, err := b.CreateStateMachine(context.Background(), "idem-sm-"+tt.name, minimalDefinition, validRoleARN, tt.smType)
			require.NoError(t, err)
			assert.Equal(t, sm1.StateMachineArn, sm2.StateMachineArn)

			// Only one state machine stored.
			assert.Equal(t, 1, b.StateMachineCount())
		})
	}
}

func TestAudit2_CreateStateMachine_Idempotent_ViaHandler(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	body, err := json.Marshal(map[string]any{
		"name":       "idem-handler-sm",
		"definition": sfnPassDefinition,
		"roleArn":    "arn:aws:iam::123456789012:role/role",
	})
	require.NoError(t, err)

	rec1 := sfnPost(ctx, t, h, e, "CreateStateMachine", string(body))
	require.Equal(t, http.StatusOK, rec1.Code)
	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	rec2 := sfnPost(ctx, t, h, e, "CreateStateMachine", string(body))
	require.Equal(t, http.StatusOK, rec2.Code)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	assert.Equal(t, resp1["stateMachineArn"], resp2["stateMachineArn"])
}

func TestAudit2_CreateStateMachine_SameName_DiffDef_Errors(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.CreateStateMachine(context.Background(), "conflict-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	altDef := `{"StartAt":"T","States":{"T":{"Type":"Succeed"}}}`
	_, err = b.CreateStateMachine(context.Background(), "conflict-sm", altDef, validRoleARN, "STANDARD")
	require.ErrorIs(t, err, stepfunctions.ErrStateMachineAlreadyExists)
}

func TestAudit2_CreateStateMachine_SameName_DiffRole_Errors(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.CreateStateMachine(context.Background(), "role-conflict-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	altRole := "arn:aws:iam::000000000000:role/other-role"
	_, err = b.CreateStateMachine(context.Background(), "role-conflict-sm", minimalDefinition, altRole, "STANDARD")
	require.ErrorIs(t, err, stepfunctions.ErrStateMachineAlreadyExists)
}

func TestAudit2_CreateStateMachine_SameName_DiffType_Errors(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.CreateStateMachine(context.Background(), "type-conflict-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	_, err = b.CreateStateMachine(context.Background(), "type-conflict-sm", minimalDefinition, validRoleARN, "EXPRESS")
	require.ErrorIs(t, err, stepfunctions.ErrStateMachineAlreadyExists)
}

// ─── Alias Routing Weight Validation ─────────────────────────────────────────

func TestAudit2_CreateStateMachineAlias_RoutingWeightsMustSum100(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		routing []stepfunctions.AliasRoutingConfig
		wantErr bool
	}{
		{
			name: "single_entry_weight_100_ok",
			routing: []stepfunctions.AliasRoutingConfig{
				{StateMachineVersionArn: "v1-arn", Weight: 100},
			},
			wantErr: false,
		},
		{
			name: "two_entries_sum_100_ok",
			routing: []stepfunctions.AliasRoutingConfig{
				{StateMachineVersionArn: "v1-arn", Weight: 80},
				{StateMachineVersionArn: "v2-arn", Weight: 20},
			},
			wantErr: false,
		},
		{
			name: "single_entry_weight_50_error",
			routing: []stepfunctions.AliasRoutingConfig{
				{StateMachineVersionArn: "v1-arn", Weight: 50},
			},
			wantErr: true,
		},
		{
			name: "two_entries_sum_not_100_error",
			routing: []stepfunctions.AliasRoutingConfig{
				{StateMachineVersionArn: "v1-arn", Weight: 60},
				{StateMachineVersionArn: "v2-arn", Weight: 60},
			},
			wantErr: true,
		},
		{
			name:    "empty_routing_error",
			routing: []stepfunctions.AliasRoutingConfig{},
			wantErr: true,
		},
		{
			name: "three_entries_error",
			routing: []stepfunctions.AliasRoutingConfig{
				{StateMachineVersionArn: "v1-arn", Weight: 50},
				{StateMachineVersionArn: "v2-arn", Weight: 30},
				{StateMachineVersionArn: "v3-arn", Weight: 20},
			},
			wantErr: true,
		},
		{
			name: "weight_over_100_error",
			routing: []stepfunctions.AliasRoutingConfig{
				{StateMachineVersionArn: "v1-arn", Weight: 101},
			},
			wantErr: true,
		},
		{
			name: "negative_weight_error",
			routing: []stepfunctions.AliasRoutingConfig{
				{StateMachineVersionArn: "v1-arn", Weight: -1},
				{StateMachineVersionArn: "v2-arn", Weight: 101},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			sm, err := b.CreateStateMachine(
				context.Background(),
				"alias-weight-sm-"+tt.name[:min(len(tt.name), 20)],
				minimalDefinition,
				validRoleARN,
				"STANDARD",
			)
			require.NoError(t, err)

			_, err = b.CreateStateMachineAlias(
				sm.StateMachineArn,
				"my-alias-"+tt.name[:min(len(tt.name), 10)],
				"",
				tt.routing,
			)
			if tt.wantErr {
				require.ErrorIs(t, err, stepfunctions.ErrInvalidRoutingConfiguration)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAudit2_UpdateStateMachineAlias_RoutingWeightsMustSum100(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "alias-update-weight-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	v, err := b.PublishStateMachineVersion(sm.StateMachineArn, "v1", "")
	require.NoError(t, err)

	alias, err := b.CreateStateMachineAlias(sm.StateMachineArn, "stable", "", []stepfunctions.AliasRoutingConfig{
		{StateMachineVersionArn: v.StateMachineVersionArn, Weight: 100},
	})
	require.NoError(t, err)

	// Update with weights that don't sum to 100 → error.
	_, err = b.UpdateStateMachineAlias(alias.StateMachineAliasArn, "", []stepfunctions.AliasRoutingConfig{
		{StateMachineVersionArn: v.StateMachineVersionArn, Weight: 50},
	})
	require.ErrorIs(t, err, stepfunctions.ErrInvalidRoutingConfiguration)

	// Update with valid weights → succeeds.
	_, err = b.UpdateStateMachineAlias(alias.StateMachineAliasArn, "", []stepfunctions.AliasRoutingConfig{
		{StateMachineVersionArn: v.StateMachineVersionArn, Weight: 100},
	})
	require.NoError(t, err)
}

func TestAudit2_CreateStateMachineAlias_RoutingViaHandler(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "alias-routing-handler-sm")

	// Create alias with weights not summing to 100 → 400.
	body, err := json.Marshal(map[string]any{
		"stateMachineArn": smARN,
		"name":            "bad-alias",
		"routingConfiguration": []map[string]any{
			{"stateMachineVersionArn": "arn:fake:version:1", "weight": 50},
		},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "CreateStateMachineAlias", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidRoutingConfiguration", resp["__type"])
}

// ─── Tag Validation ───────────────────────────────────────────────────────────

func TestAudit2_TagResource_KeyTooLong_Error(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "tag-key-len-sm")

	longKey := strings.Repeat("k", 129)
	body, err := json.Marshal(map[string]any{
		"resourceArn": smARN,
		"tags":        map[string]string{longKey: "val"},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TagPolicyViolation", resp["__type"])
}

func TestAudit2_TagResource_EmptyKey_Error(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "tag-empty-key-sm")

	body, err := json.Marshal(map[string]any{
		"resourceArn": smARN,
		"tags":        map[string]string{"": "val"},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TagPolicyViolation", resp["__type"])
}

func TestAudit2_TagResource_ValueTooLong_Error(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "tag-val-len-sm")

	longVal := strings.Repeat("v", 257)
	body, err := json.Marshal(map[string]any{
		"resourceArn": smARN,
		"tags":        map[string]string{"mykey": longVal},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TagPolicyViolation", resp["__type"])
}

func TestAudit2_TagResource_MaxTagsExceeded_Error(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "tag-max-count-sm")

	// Add 50 tags in batches of 10.
	for i := range 5 {
		batch := make(map[string]string, 10)
		for j := range 10 {
			batch["key-"+string(rune('a'+i))+string(rune('0'+j))] = "val"
		}
		body, err := json.Marshal(map[string]any{
			"resourceArn": smARN,
			"tags":        batch,
		})
		require.NoError(t, err)
		rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Adding one more tag should fail.
	body, err := json.Marshal(map[string]any{
		"resourceArn": smARN,
		"tags":        map[string]string{"overflow-key": "val"},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TagPolicyViolation", resp["__type"])
}

func TestAudit2_TagResource_ValidTagsAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "max_key_length", key: strings.Repeat("k", 128), value: "val"},
		{name: "max_value_length", key: "k", value: strings.Repeat("v", 256)},
		{name: "empty_value_ok", key: "kk", value: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)
			smARN := createSM(ctx, t, h, e, "tag-valid-sm-"+tt.name[:min(len(tt.name), 15)])

			body, err := json.Marshal(map[string]any{
				"resourceArn": smARN,
				"tags":        map[string]string{tt.key: tt.value},
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// ─── ListExecutions Sort Order ────────────────────────────────────────────────

func TestAudit2_ListExecutions_OrderedByStartDateDesc(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	sm, err := b.CreateStateMachine(context.Background(), "order-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	names := []string{"exec-a", "exec-b", "exec-c"}
	startDates := make([]float64, len(names))

	for i, name := range names {
		exec, execErr := b.StartExecution(sm.StateMachineArn, name, "{}")
		require.NoError(t, execErr)

		startDates[i] = exec.StartDate

		// Small sleep to ensure distinct start timestamps.
		time.Sleep(5 * time.Millisecond)
	}

	execs, _, err := b.ListExecutions(sm.StateMachineArn, "", "", 10)
	require.NoError(t, err)
	require.Len(t, execs, 3)

	// Most recent first.
	for i := 1; i < len(execs); i++ {
		assert.GreaterOrEqual(t, execs[i-1].StartDate, execs[i].StartDate,
			"expected descending startDate order at index %d", i)
	}

	// First result should be the last started.
	assert.Equal(t, names[2], execs[0].Name)
}

func TestAudit2_ListExecutions_OrderedByStartDateDesc_ViaHandler(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "order-handler-sm")

	execNames := []string{"exec-z", "exec-a", "exec-m"}
	for _, name := range execNames {
		body, err := json.Marshal(map[string]string{
			"stateMachineArn": smARN,
			"name":            name,
			"input":           "{}",
		})
		require.NoError(t, err)

		rec := sfnPost(ctx, t, h, e, "StartExecution", string(body))
		require.Equal(t, http.StatusOK, rec.Code)
		time.Sleep(5 * time.Millisecond)
	}

	listBody, err := json.Marshal(map[string]string{"stateMachineArn": smARN})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "ListExecutions", string(listBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rawExecs, _ := resp["executions"].([]any)
	require.Len(t, rawExecs, 3)

	// exec-m started last, should appear first.
	first, _ := rawExecs[0].(map[string]any)
	assert.Equal(t, "exec-m", first["name"])
}
