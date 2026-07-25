package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func azureConfig(tenantID string, subIDs ...string) ssm.CloudConnectorConfiguration {
	subs := make([]ssm.AzureSubscription, 0, len(subIDs))
	for _, id := range subIDs {
		subs = append(subs, ssm.AzureSubscription{ID: id})
	}

	var targets *ssm.ConfigurationTargets
	if len(subs) > 0 {
		targets = &ssm.ConfigurationTargets{Subscriptions: subs}
	}

	return ssm.CloudConnectorConfiguration{
		AzureConfiguration: &ssm.AzureConfiguration{
			ApplicationID: "app-1",
			TenantID:      tenantID,
			Targets:       targets,
		},
	}
}

func Test_CreateCloudConnector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *ssm.CreateCloudConnectorInput
		name    string
		wantErr string
	}{
		{
			name: "valid_azure_connector_succeeds",
			input: &ssm.CreateCloudConnectorInput{
				ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc1",
				Configuration:      azureConfig("tenant-1", "sub-1", "sub-2"),
				DisplayName:        "my-connector",
				RoleArn:            "arn:aws:iam::123456789012:role/ssm-cloud-connector",
				Tags:               []ssm.Tag{{Key: "env", Value: "prod"}},
			},
		},
		{
			name: "missing_display_name_rejected",
			input: &ssm.CreateCloudConnectorInput{
				ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc1",
				Configuration:      azureConfig("tenant-1"),
				RoleArn:            "arn:aws:iam::123456789012:role/ssm-cloud-connector",
			},
			wantErr: "ValidationException",
		},
		{
			name: "missing_role_arn_rejected",
			input: &ssm.CreateCloudConnectorInput{
				ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc1",
				Configuration:      azureConfig("tenant-1"),
				DisplayName:        "my-connector",
			},
			wantErr: "ValidationException",
		},
		{
			name: "missing_azure_configuration_rejected",
			input: &ssm.CreateCloudConnectorInput{
				ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc1",
				DisplayName:        "my-connector",
				RoleArn:            "arn:aws:iam::123456789012:role/ssm-cloud-connector",
			},
			wantErr: "ValidationException",
		},
		{
			name: "missing_tenant_id_rejected",
			input: &ssm.CreateCloudConnectorInput{
				ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc1",
				Configuration: ssm.CloudConnectorConfiguration{
					AzureConfiguration: &ssm.AzureConfiguration{ApplicationID: "app-1"},
				},
				DisplayName: "my-connector",
				RoleArn:     "arn:aws:iam::123456789012:role/ssm-cloud-connector",
			},
			wantErr: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			out, err := b.CreateCloudConnector(context.Background(), tt.input)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, out.CloudConnectorID)
			assert.True(t, strings.HasPrefix(out.CloudConnectorID, "cc-"))

			get, err := b.GetCloudConnector(context.Background(), &ssm.GetCloudConnectorInput{
				CloudConnectorID: out.CloudConnectorID,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.input.DisplayName, get.DisplayName)
			assert.Equal(t, tt.input.RoleArn, get.RoleArn)
			assert.Equal(t, tt.input.ConfigConnectorArn, get.ConfigConnectorArn)
			assert.Contains(t, get.CloudConnectorArn, "cloudconnector/"+out.CloudConnectorID)
			assert.InDelta(t, get.CreatedAt, get.UpdatedAt, 0)
			assert.Positive(t, get.CreatedAt)

			if len(tt.input.Tags) > 0 {
				tagsOut, tagErr := b.ListTagsForResource(context.Background(), &ssm.ListTagsForResourceInput{
					ResourceType: "CloudConnector",
					ResourceID:   out.CloudConnectorID,
				})
				require.NoError(t, tagErr)
				require.Len(t, tagsOut.TagList, 1)
				assert.Equal(t, "env", tagsOut.TagList[0].Key)
			}
		})
	}
}

func Test_GetCloudConnector_NotFound(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, err := b.GetCloudConnector(context.Background(), &ssm.GetCloudConnectorInput{
		CloudConnectorID: "cc-does-not-exist",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}

func Test_DeleteCloudConnector(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	created, err := b.CreateCloudConnector(context.Background(), &ssm.CreateCloudConnectorInput{
		ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc1",
		Configuration:      azureConfig("tenant-1"),
		DisplayName:        "my-connector",
		RoleArn:            "arn:aws:iam::123456789012:role/ssm-cloud-connector",
		Tags:               []ssm.Tag{{Key: "k", Value: "v"}},
	})
	require.NoError(t, err)

	del, err := b.DeleteCloudConnector(context.Background(), &ssm.DeleteCloudConnectorInput{
		CloudConnectorID: created.CloudConnectorID,
	})
	require.NoError(t, err)
	assert.Equal(t, created.CloudConnectorID, del.CloudConnectorID)

	_, err = b.GetCloudConnector(context.Background(), &ssm.GetCloudConnectorInput{
		CloudConnectorID: created.CloudConnectorID,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")

	// Deleting again is a not-found, not a silent success.
	_, err = b.DeleteCloudConnector(context.Background(), &ssm.DeleteCloudConnectorInput{
		CloudConnectorID: created.CloudConnectorID,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")

	// Tags attached at creation must not leak past deletion.
	tagsOut, err := b.ListTagsForResource(context.Background(), &ssm.ListTagsForResourceInput{
		ResourceType: "CloudConnector",
		ResourceID:   created.CloudConnectorID,
	})
	require.NoError(t, err)
	assert.Empty(t, tagsOut.TagList)
}

func Test_UpdateCloudConnector(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	created, err := b.CreateCloudConnector(context.Background(), &ssm.CreateCloudConnectorInput{
		ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc1",
		Configuration:      azureConfig("tenant-1"),
		DisplayName:        "old-name",
		RoleArn:            "arn:aws:iam::123456789012:role/ssm-cloud-connector",
	})
	require.NoError(t, err)

	before, err := b.GetCloudConnector(context.Background(), &ssm.GetCloudConnectorInput{
		CloudConnectorID: created.CloudConnectorID,
	})
	require.NoError(t, err)

	upd, err := b.UpdateCloudConnector(context.Background(), &ssm.UpdateCloudConnectorInput{
		CloudConnectorID: created.CloudConnectorID,
		DisplayName:      "new-name",
	})
	require.NoError(t, err)
	assert.Equal(t, created.CloudConnectorID, upd.CloudConnectorID)

	after, err := b.GetCloudConnector(context.Background(), &ssm.GetCloudConnectorInput{
		CloudConnectorID: created.CloudConnectorID,
	})
	require.NoError(t, err)
	assert.Equal(t, "new-name", after.DisplayName)
	// RoleArn untouched by a partial update that didn't set it.
	assert.Equal(t, before.RoleArn, after.RoleArn)
	assert.GreaterOrEqual(t, after.UpdatedAt, before.UpdatedAt)
	assert.InDelta(t, before.CreatedAt, after.CreatedAt, 0)

	_, err = b.UpdateCloudConnector(context.Background(), &ssm.UpdateCloudConnectorInput{
		CloudConnectorID: "cc-nope",
		DisplayName:      "x",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}

func Test_ListCloudConnectors(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()

	tenantOnly, setupErr := b.CreateCloudConnector(context.Background(), &ssm.CreateCloudConnectorInput{
		ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc1",
		Configuration:      azureConfig("tenant-A"),
		DisplayName:        "tenant-level",
		RoleArn:            "arn:aws:iam::123456789012:role/r",
	})
	require.NoError(t, setupErr)

	withSub, setupErr := b.CreateCloudConnector(context.Background(), &ssm.CreateCloudConnectorInput{
		ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc2",
		Configuration:      azureConfig("tenant-B", "sub-99"),
		DisplayName:        "sub-scoped",
		RoleArn:            "arn:aws:iam::123456789012:role/r",
	})
	require.NoError(t, setupErr)

	t.Run("no_filters_returns_all", func(t *testing.T) {
		t.Parallel()

		out, err := b.ListCloudConnectors(context.Background(), &ssm.ListCloudConnectorsInput{})
		require.NoError(t, err)
		assert.Len(t, out.CloudConnectors, 2)
		assert.Empty(t, out.NextToken)
	})

	t.Run("filter_by_tenant_id", func(t *testing.T) {
		t.Parallel()

		out, err := b.ListCloudConnectors(context.Background(), &ssm.ListCloudConnectorsInput{
			Filters: []ssm.CloudConnectorFilter{{FilterKey: "TenantId", FilterValues: []string{"tenant-B"}}},
		})
		require.NoError(t, err)
		require.Len(t, out.CloudConnectors, 1)
		assert.Equal(t, withSub.CloudConnectorID, out.CloudConnectors[0].CloudConnectorID)
	})

	t.Run("filter_by_subscription_id", func(t *testing.T) {
		t.Parallel()

		out, err := b.ListCloudConnectors(context.Background(), &ssm.ListCloudConnectorsInput{
			Filters: []ssm.CloudConnectorFilter{
				{FilterKey: "SubscriptionId", FilterValues: []string{"sub-99"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.CloudConnectors, 1)
		assert.Equal(t, withSub.CloudConnectorID, out.CloudConnectors[0].CloudConnectorID)
	})

	t.Run("filter_by_subscription_id_none_returns_tenant_level_only", func(t *testing.T) {
		t.Parallel()

		out, err := b.ListCloudConnectors(context.Background(), &ssm.ListCloudConnectorsInput{
			Filters: []ssm.CloudConnectorFilter{
				{FilterKey: "SubscriptionId", FilterValues: []string{"NONE"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.CloudConnectors, 1)
		assert.Equal(t, tenantOnly.CloudConnectorID, out.CloudConnectors[0].CloudConnectorID)
	})

	t.Run("pagination_respects_max_results_and_next_token", func(t *testing.T) {
		t.Parallel()

		page1, err := b.ListCloudConnectors(context.Background(), &ssm.ListCloudConnectorsInput{
			MaxResults: new(int64(1)),
		})
		require.NoError(t, err)
		require.Len(t, page1.CloudConnectors, 1)
		require.NotEmpty(t, page1.NextToken)

		page2, err := b.ListCloudConnectors(context.Background(), &ssm.ListCloudConnectorsInput{
			MaxResults: new(int64(1)),
			NextToken:  page1.NextToken,
		})
		require.NoError(t, err)
		require.Len(t, page2.CloudConnectors, 1)
		assert.Empty(t, page2.NextToken)
		assert.NotEqual(t, page1.CloudConnectors[0].CloudConnectorID, page2.CloudConnectors[0].CloudConnectorID)
	})
}

func Test_ValidateCloudConnector(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	created, err := b.CreateCloudConnector(context.Background(), &ssm.CreateCloudConnectorInput{
		ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc1",
		Configuration:      azureConfig("tenant-1", "sub-1", "sub-2"),
		DisplayName:        "my-connector",
		RoleArn:            "arn:aws:iam::123456789012:role/r",
	})
	require.NoError(t, err)

	out, err := b.ValidateCloudConnector(context.Background(), &ssm.ValidateCloudConnectorInput{
		CloudConnectorID: created.CloudConnectorID,
	})
	require.NoError(t, err)
	require.Len(t, out.ValidationFindings, 3) // 1 tenant + 2 subscriptions

	assert.Equal(t, "TenantSummary", out.ValidationFindings[0].Code)
	assert.Equal(t, "INFO", out.ValidationFindings[0].Type)
	require.NotNil(t, out.ValidationFindings[0].Scope)
	assert.Equal(t, "tenant-1", out.ValidationFindings[0].Scope.ID)

	subCodes := map[string]bool{}
	for _, f := range out.ValidationFindings[1:] {
		assert.Equal(t, "SubscriptionAccessible", f.Code)
		subCodes[f.Scope.ID] = true
	}
	assert.True(t, subCodes["sub-1"])
	assert.True(t, subCodes["sub-2"])

	_, err = b.ValidateCloudConnector(context.Background(), &ssm.ValidateCloudConnectorInput{
		CloudConnectorID: "cc-nope",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}

func Test_CloudConnector_HandlerDispatch(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	createBody, _ := json.Marshal(map[string]any{
		"ConfigConnectorArn": "arn:aws:config::123456789012:config-connector/cc1",
		"Configuration": map[string]any{
			"AzureConfiguration": map[string]any{
				"ApplicationId": "app-1",
				"TenantId":      "tenant-1",
			},
		},
		"DisplayName": "my-connector",
		"RoleArn":     "arn:aws:iam::123456789012:role/r",
	})

	rec := doRequest(t, h, "CreateCloudConnector", string(createBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		CloudConnectorID string `json:"CloudConnectorId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	require.NotEmpty(t, createResp.CloudConnectorID)

	getBody, _ := json.Marshal(map[string]any{"CloudConnectorId": createResp.CloudConnectorID})
	rec = doRequest(t, h, "GetCloudConnector", string(getBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "my-connector", getResp["DisplayName"])
	// CreatedAt/UpdatedAt must serialize as epoch-seconds numbers, not ISO8601 strings.
	_, isNumber := getResp["CreatedAt"].(float64)
	assert.True(t, isNumber, "CreatedAt must be a JSON number (epoch seconds), got %T", getResp["CreatedAt"])

	rec = doRequest(t, h, "GetCloudConnector", `{"CloudConnectorId":"cc-unknown"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp.Type)

	deleteBody, _ := json.Marshal(map[string]any{"CloudConnectorId": createResp.CloudConnectorID})
	rec = doRequest(t, h, "DeleteCloudConnector", string(deleteBody))
	require.Equal(t, http.StatusOK, rec.Code)
}

func Test_CloudConnector_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	created, err := b.CreateCloudConnector(context.Background(), &ssm.CreateCloudConnectorInput{
		ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc1",
		Configuration:      azureConfig("tenant-1", "sub-1"),
		DisplayName:        "my-connector",
		RoleArn:            "arn:aws:iam::123456789012:role/r",
		Tags:               []ssm.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)

	snap := b.Snapshot(context.Background())
	require.NotEmpty(t, snap)

	restored := ssm.NewInMemoryBackend()
	require.NoError(t, restored.Restore(context.Background(), snap))

	get, err := restored.GetCloudConnector(context.Background(), &ssm.GetCloudConnectorInput{
		CloudConnectorID: created.CloudConnectorID,
	})
	require.NoError(t, err)
	assert.Equal(t, "my-connector", get.DisplayName)
	require.NotNil(t, get.Configuration.AzureConfiguration)
	assert.Equal(t, "tenant-1", get.Configuration.AzureConfiguration.TenantID)

	tagsOut, err := restored.ListTagsForResource(context.Background(), &ssm.ListTagsForResourceInput{
		ResourceType: "CloudConnector",
		ResourceID:   created.CloudConnectorID,
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1)
	assert.Equal(t, "env", tagsOut.TagList[0].Key)
}

// Test_CloudConnector_MaxResultsBounds locks in ListCloudConnectors/
// ValidateCloudConnector's now-confirmed real MaxResults bounds ("Valid
// Range: Minimum value of 0. Maximum value of 10" and "...Maximum value of
// 75" respectively, confirmed 2026-07-24 via
// API_ListCloudConnectors.html/API_ValidateCloudConnector.html -- this
// AWS-hosted page was not discoverable at the time of the prior audit pass,
// which is why the bound was previously recorded as an unconfirmed guess).
// A caller-supplied value outside [0, bound] must be rejected with
// ValidationException rather than silently accepted or silently clamped.
func Test_CloudConnector_MaxResultsBounds(t *testing.T) {
	t.Parallel()

	newConnector := func(t *testing.T, b *ssm.InMemoryBackend) string {
		t.Helper()

		created, err := b.CreateCloudConnector(context.Background(), &ssm.CreateCloudConnectorInput{
			ConfigConnectorArn: "arn:aws:config::123456789012:config-connector/cc1",
			Configuration:      azureConfig("tenant-1"),
			DisplayName:        "bounds-test",
			RoleArn:            "arn:aws:iam::123456789012:role/r",
		})
		require.NoError(t, err)

		return created.CloudConnectorID
	}

	tests := []struct {
		name       string
		op         string
		maxResults int64
		wantErr    bool
	}{
		{name: "list_at_documented_max_succeeds", op: "List", maxResults: 10, wantErr: false},
		{name: "list_above_documented_max_fails", op: "List", maxResults: 11, wantErr: true},
		{name: "list_explicit_zero_is_valid_per_documented_minimum", op: "List", maxResults: 0, wantErr: false},
		{name: "validate_at_documented_max_succeeds", op: "Validate", maxResults: 75, wantErr: false},
		{name: "validate_above_documented_max_fails", op: "Validate", maxResults: 76, wantErr: true},
		{name: "validate_explicit_zero_is_valid_per_documented_minimum", op: "Validate", maxResults: 0, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			mr := tt.maxResults

			var err error

			switch tt.op {
			case "List":
				newConnector(t, b)
				_, err = b.ListCloudConnectors(context.Background(), &ssm.ListCloudConnectorsInput{MaxResults: &mr})
			case "Validate":
				id := newConnector(t, b)
				_, err = b.ValidateCloudConnector(context.Background(), &ssm.ValidateCloudConnectorInput{
					CloudConnectorID: id,
					MaxResults:       &mr,
				})
			}

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrValidationException)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
