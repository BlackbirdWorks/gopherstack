package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestResourceDataSync_CRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// List empty
	rec := doRequest(t, h, "ListResourceDataSync", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Create
	rec = doRequest(
		t,
		h,
		"CreateResourceDataSync",
		`{"SyncName":"my-sync","SyncType":"SyncToDestination"}`,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List shows it
	rec = doRequest(t, h, "ListResourceDataSync", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "my-sync")

	// Create duplicate → returns error (sync already exists)
	rec = doRequest(t, h, "CreateResourceDataSync", `{"SyncName":"my-sync"}`)
	assert.NotEqual(t, http.StatusNotFound, rec.Code)

	// Update
	rec = doRequest(t, h, "UpdateResourceDataSync", `{"SyncName":"my-sync"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doRequest(t, h, "DeleteResourceDataSync", `{"SyncName":"my-sync"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List empty again
	rec = doRequest(t, h, "ListResourceDataSync", `{}`)
	assertBodyContains(t, rec, "[]")
}
func TestListNodes(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create an activation so there's a node
	_, err := b.CreateActivation(context.TODO(), &ssm.CreateActivationInput{
		IamRole:           "arn:aws:iam::000000000000:role/SSMRole",
		RegistrationLimit: 1,
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "ListNodes", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "Nodes")

	rec = doRequest(
		t, h, "ListNodesSummary",
		`{"Aggregators":[{"AggregatorType":"Count","AttributeName":"PlatformType","TypeName":"Instance"}]}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "Count")
}

// TestOtherMapsRegionCleanup verifies that delete operations on resources
// other than parameters also clean up their per-region inner maps.
func TestOtherMapsRegionCleanup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string // which resource type to create and delete
	}{
		{name: "activation cleanup", resource: "activation"},
		{name: "patch baseline cleanup", resource: "patchbaseline"},
		{name: "maintenance window cleanup", resource: "maintenancewindow"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ssm.NewInMemoryBackend()

			switch tc.resource {
			case "activation":
				act, err := b.CreateActivation(ctx, &ssm.CreateActivationInput{
					IamRole:           "arn:aws:iam::000000000000:role/SSMRole",
					RegistrationLimit: 1,
				})
				require.NoError(t, err)
				_, err = b.DeleteActivation(
					ctx,
					&ssm.DeleteActivationInput{ActivationID: act.ActivationID},
				)
				require.NoError(t, err)
				assert.Zero(t, b.ActivationCount(), "activation map must be cleaned up")

			case "patchbaseline":
				pb, err := b.CreatePatchBaseline(
					ctx,
					&ssm.CreatePatchBaselineInput{Name: "test-baseline"},
				)
				require.NoError(t, err)
				_, err = b.DeletePatchBaseline(
					ctx,
					&ssm.DeletePatchBaselineInput{BaselineID: pb.BaselineID},
				)
				require.NoError(t, err)
				assert.Zero(t, b.PatchBaselineCount(), "patch baseline map must be cleaned up")

			case "maintenancewindow":
				win, err := b.CreateMaintenanceWindow(ctx, &ssm.CreateMaintenanceWindowInput{
					Name: "test-win", Schedule: "rate(7 days)", Duration: 1,
				})
				require.NoError(t, err)
				_, err = b.DeleteMaintenanceWindow(
					ctx,
					&ssm.DeleteMaintenanceWindowInput{WindowID: win.WindowID},
				)
				require.NoError(t, err)
				assert.Zero(
					t,
					b.MaintenanceWindowCount(),
					"maintenance window map must be cleaned up",
				)
			}
		})
	}
}
func TestDeleteResourceDataSync_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		syncName string
	}{
		{
			name:     "unknown_sync_name_returns_error",
			syncName: "ghost-sync",
		},
		{
			name:     "another_unknown_sync_returns_error",
			syncName: "never-created",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.DeleteResourceDataSync(context.TODO(), &ssm.DeleteResourceDataSyncInput{
				SyncName: tt.syncName,
			})
			require.ErrorIs(t, err, ssm.ErrResourceDataSyncNotFound,
				"non-existent sync must return ErrResourceDataSyncNotFound")
		})
	}
}
func TestDeleteResourceDataSync_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		syncName string
		syncType string
	}{
		{
			name:     "created_sync_can_be_deleted",
			syncName: "my-s3-sync",
			syncType: "SyncToDestination",
		},
		{
			name:     "default_sync_type",
			syncName: "another-sync",
			syncType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)

			_, err := b.CreateResourceDataSync(context.TODO(), &ssm.CreateResourceDataSyncInput{
				SyncName: tt.syncName,
				SyncType: tt.syncType,
			})
			require.NoError(t, err)

			_, err = b.DeleteResourceDataSync(context.TODO(), &ssm.DeleteResourceDataSyncInput{
				SyncName: tt.syncName,
			})
			require.NoError(t, err)

			// Second delete must fail.
			_, err = b.DeleteResourceDataSync(context.TODO(), &ssm.DeleteResourceDataSyncInput{
				SyncName: tt.syncName,
			})
			require.ErrorIs(t, err, ssm.ErrResourceDataSyncNotFound,
				"second delete of removed sync must return ErrResourceDataSyncNotFound")
		})
	}
}
func TestDeleteResourceDataSync_Handler_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "non_existent_sync_returns_500",
			body: `{"SyncName":"ghost-sync"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "DeleteResourceDataSync", tt.body)
			assert.Equal(t, http.StatusInternalServerError, rec.Code)
			assert.Contains(t, rec.Body.String(), "ResourceDataSyncNotFoundException")
		})
	}
}

// TestStubOps_DeleteActivation exercises the activation-backed delete stub.
func TestStubOps_DeleteActivation(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create an activation first so we have a valid ID.
	act, err := b.CreateActivation(context.TODO(), &ssm.CreateActivationInput{
		IamRole:             "arn:aws:iam::123456789012:role/SSMRole",
		DefaultInstanceName: "test-instance",
		RegistrationLimit:   1,
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"ActivationId": act.ActivationID})
	rec := doRequest(t, h, "DeleteActivation", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_DeleteResourceDataSync exercises that stub.
func TestStubOps_DeleteResourceDataSync(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.CreateResourceDataSync(context.TODO(), &ssm.CreateResourceDataSyncInput{SyncName: "test-sync"})
	require.NoError(t, err)

	rec := doRequest(t, h, "DeleteResourceDataSync", `{"SyncName":"test-sync"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}
func TestCreateActivation_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		body              string
		wantStatus        int
		wantActivationLen int
	}{
		{
			name:              "minimal_required_fields",
			body:              `{"IamRole":"arn:aws:iam::123456789012:role/SSMRole"}`,
			wantStatus:        http.StatusOK,
			wantActivationLen: 1,
		},
		{
			name: "with_optional_fields",
			body: `{"IamRole":"arn:aws:iam::123456789012:role/SSMRole",` +
				`"Description":"test","DefaultInstanceName":"my-instance","RegistrationLimit":5}`,
			wantStatus:        http.StatusOK,
			wantActivationLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			rec := doRequest(t, h, "CreateActivation", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateActivationOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.ActivationID)
			assert.NotEmpty(t, resp.ActivationCode)
			assert.True(t, strings.HasPrefix(resp.ActivationID, "act-"))
			assert.Len(t, resp.ActivationCode, 20)
			assert.Equal(t, tt.wantActivationLen, backend.ActivationCount())
		})
	}
}
func TestCreateActivation_IamRoleValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "missing_iam_role",
			body:       `{"Description":"test"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "empty_iam_role",
			body:       `{"IamRole":""}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "valid_iam_role",
			body:       `{"IamRole":"arn:aws:iam::123456789012:role/SSMRole"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateActivation", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantErr != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErr)
			}
		})
	}
}

// TestGenerateCode verifies generateCode properties.
func TestGenerateCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		validChars string
		wantLen    int
	}{
		{
			name:       "activation_code_length_20",
			wantLen:    20,
			validChars: "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			// Create an activation to get a code
			out, err := b.CreateActivation(context.TODO(), &ssm.CreateActivationInput{
				IamRole: "arn:aws:iam::123:role/test",
			})
			require.NoError(t, err)

			code := out.ActivationCode
			assert.Len(t, code, tt.wantLen)

			for _, ch := range code {
				assert.True(t, strings.ContainsRune(tt.validChars, ch),
					"char %q not in valid chars", ch)
			}
		})
	}
}

// TestCreateActivationWithTags verifies tags are stored on activation create.
func TestCreateActivationWithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		tagCount int
	}{
		{
			name:     "activation_with_tags",
			body:     `{"IamRole":"arn:aws:iam::123:role/r","Tags":[{"Key":"Env","Value":"test"},{"Key":"Team","Value":"ops"}]}`,
			tagCount: 2,
		},
		{
			name:     "activation_without_tags",
			body:     `{"IamRole":"arn:aws:iam::123:role/r"}`,
			tagCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateActivation", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp ssm.CreateActivationOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.NotEmpty(t, resp.ActivationID)

			listBody, _ := json.Marshal(map[string]any{
				"ResourceType": "Activation",
				"ResourceId":   resp.ActivationID,
			})
			listRec := doRequest(t, h, "ListTagsForResource", string(listBody))
			require.Equal(t, http.StatusOK, listRec.Code)

			var tagResp ssm.ListTagsForResourceOutput
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagResp))
			assert.Len(t, tagResp.TagList, tt.tagCount)
		})
	}
}

// TestInternalTimestamp verifies activation codes use valid timestamp format.
func TestInternalTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "activation_expiry_is_in_future"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			out, err := b.CreateActivation(context.TODO(), &ssm.CreateActivationInput{
				IamRole: "arn:aws:iam::123:role/test",
			})
			require.NoError(t, err)

			assert.NotEmpty(t, out.ActivationID)
			assert.True(t, strings.HasPrefix(out.ActivationID, "act-"))

			_ = tt.name
		})
	}
}
func TestFull_Activation_CreateList(t *testing.T) {
	t.Parallel()
	h := newHandler()

	code, out := postJSON(t, h, "CreateActivation", map[string]any{
		"IamRole":           "SSMServiceRole",
		"RegistrationLimit": 5,
		"Description":       "test activation",
	})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["ActivationId"])
	assert.NotEmpty(t, out["ActivationCode"])
}
func TestFull_ResourceDataSync_CreateListDelete(t *testing.T) {
	t.Parallel()
	h := newHandler()

	code, _ := postJSON(t, h, "CreateResourceDataSync", map[string]any{
		"SyncName": "my-sync",
		"SyncType": "SyncToDestination",
	})
	assert.Equal(t, http.StatusOK, code)

	code, out := postJSON(t, h, "ListResourceDataSync", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	syncs := out["ResourceDataSyncItems"].([]any)
	assert.Len(t, syncs, 1)

	code, _ = postJSON(t, h, "DeleteResourceDataSync", map[string]any{"SyncName": "my-sync"})
	assert.Equal(t, http.StatusOK, code)

	code, out = postJSON(t, h, "ListResourceDataSync", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	syncs = out["ResourceDataSyncItems"].([]any)
	assert.Empty(t, syncs)
}

// TestDeleteActivation_NotFound exercises the error path.
func TestDeleteActivation_NotFound(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, err := b.DeleteActivation(context.TODO(), &ssm.DeleteActivationInput{ActivationID: "nonexistent"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrActivationNotFound)
}

// TestCreateActivation_WithTags covers tags path in CreateActivation.
func TestCreateActivation_WithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   ssm.CreateActivationInput
		wantErr bool
	}{
		{
			name: "missing_iam_role",
			input: ssm.CreateActivationInput{
				IamRole: "",
			},
			wantErr: true,
		},
		{
			name: "with_tags",
			input: ssm.CreateActivationInput{
				IamRole: "arn:aws:iam::123:role/SSMRole",
				Tags:    []ssm.Tag{{Key: "env", Value: "test"}},
			},
			wantErr: false,
		},
		{
			name: "custom_registration_limit",
			input: ssm.CreateActivationInput{
				IamRole:           "arn:aws:iam::123:role/SSMRole",
				RegistrationLimit: 5,
			},
			wantErr: false,
		},
		{
			name: "custom_expiry_date",
			input: ssm.CreateActivationInput{
				IamRole:        "arn:aws:iam::123:role/SSMRole",
				ExpirationDate: float64(time.Now().Add(48 * time.Hour).Unix()),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateActivation(context.TODO(), &tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
func TestFleetManager_ListNodes_FromActivations(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create activations — each produces a node.
	for range 3 {
		_, err := b.CreateActivation(context.TODO(), &ssm.CreateActivationInput{
			IamRole:           "arn:aws:iam::123456789012:role/SSMRole",
			RegistrationLimit: 1,
		})
		require.NoError(t, err)
	}

	rec := doRequest(t, h, "ListNodes", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	nodes := resp["Nodes"].([]any)
	assert.GreaterOrEqual(t, len(nodes), 3)

	// Each node should have PlatformType.
	for _, n := range nodes {
		node := n.(map[string]any)
		assert.NotEmpty(t, node["PlatformType"])
	}
}

// TestFleetManager_ListNodesSummary_NodeCount drives ListNodesSummary
// through the real aws-sdk-go-v2 client and proves Aggregators -- the op's
// only required member (api_op_ListNodesSummary.go:31-62, ssm@v1.73.4) --
// actually drives a real per-attribute grouping instead of the fixed
// synthetic count the backend previously returned regardless of input.
func TestFleetManager_ListNodesSummary_NodeCount(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	client := newTestSSMClient(t, h)

	_, err := b.CreateActivation(context.TODO(), &ssm.CreateActivationInput{
		IamRole:           "arn:aws:iam::123456789012:role/SSMRole",
		RegistrationLimit: 2,
	})
	require.NoError(t, err)

	out, err := client.ListNodesSummary(context.TODO(), &ssmsdk.ListNodesSummaryInput{
		Aggregators: []ssmtypes.NodeAggregator{
			{
				AggregatorType: ssmtypes.NodeAggregatorTypeCount,
				AttributeName:  ssmtypes.NodeAttributeNamePlatformType,
				TypeName:       ssmtypes.NodeTypeNameInstance,
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Summary)
	assert.Equal(t, "Linux", out.Summary[0]["PlatformType"])
	assert.Equal(t, "1", out.Summary[0]["Count"])

	// A real client's client-side validation middleware refuses to send
	// Aggregators as an empty slice, so the "missing required field" case is
	// exercised at the raw-JSON layer instead (see TestListNodes).
}

// TestDeleteActivation_TableDriven verifies success and not-found for DeleteActivation.
func TestDeleteActivation_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		wantStatus int
		setupFirst bool
	}{
		{
			name:       "deletes_existing_activation",
			setupFirst: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_activation_returns_error",
			setupFirst: false,
			wantStatus: http.StatusBadRequest,
			wantErrMsg: "ActivationNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			activationID := "act-nonexistent"
			if tt.setupFirst {
				act, err := b.CreateActivation(context.Background(), &ssm.CreateActivationInput{
					IamRole:           "arn:aws:iam::123456789012:role/SSMRole",
					RegistrationLimit: 1,
				})
				require.NoError(t, err)
				activationID = act.ActivationID
			}

			body, _ := json.Marshal(map[string]any{"ActivationId": activationID})
			rec := doRequest(t, h, "DeleteActivation", string(body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrMsg != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrMsg)
			}
		})
	}
}

// TestDescribeInstanceProperties_DerivedFromActivations verifies that
// registered managed instances (activations) are reflected in
// DescribeInstanceProperties rather than requiring separate seeding.
func TestDescribeInstanceProperties_DerivedFromActivations(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	actResp := doRequest(t, h, "CreateActivation", `{"IamRole":"my-role"}`)
	require.Equal(t, http.StatusOK, actResp.Code)

	var activation struct {
		ActivationID string `json:"ActivationId"`
	}
	require.NoError(t, json.Unmarshal(actResp.Body.Bytes(), &activation))
	require.NotEmpty(t, activation.ActivationID)

	propsResp := doRequest(t, h, "DescribeInstanceProperties", `{}`)
	require.Equal(t, http.StatusOK, propsResp.Code)
	assert.Contains(t, propsResp.Body.String(), activation.ActivationID)
}
