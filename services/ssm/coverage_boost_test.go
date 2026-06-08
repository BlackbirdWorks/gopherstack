package ssm_test

// coverage_boost_test.go adds table-driven tests targeting specific uncovered
// branches to push total coverage above 90%.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestWithCommandTTL exercises the WithCommandTTL path where d > 0.
func TestWithCommandTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ttl     time.Duration
		wantTTL float64
	}{
		{
			name:    "positive_ttl_applied",
			ttl:     30 * time.Minute,
			wantTTL: (30 * time.Minute).Seconds(),
		},
		{
			name:    "zero_ttl_ignored",
			ttl:     0,
			wantTTL: ssm.DefaultCommandExpirySecs,
		},
		{
			name:    "negative_ttl_ignored",
			ttl:     -5 * time.Second,
			wantTTL: ssm.DefaultCommandExpirySecs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend().WithCommandTTL(tt.ttl)
			assert.InDelta(t, tt.wantTTL, b.GetCommandExpirySecs(), 0.0001)
		})
	}
}

// TestPutParameter_Validation exercises validation branches in PutParameter.
func TestPutParameter_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		name    string
		input   ssm.PutParameterInput
		wantErr bool
	}{
		{
			name: "invalid_data_type",
			input: ssm.PutParameterInput{
				Name:     "/valid/param",
				Type:     "String",
				Value:    "v",
				DataType: "badtype",
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "invalid_allowed_pattern",
			input: ssm.PutParameterInput{
				Name:           "/valid/param",
				Type:           "String",
				Value:          "hello world",
				AllowedPattern: `^\d+$`,
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "bad_regex_pattern",
			input: ssm.PutParameterInput{
				Name:           "/valid/param",
				Type:           "String",
				Value:          "v",
				AllowedPattern: `[invalid`,
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "tier_too_large_standard",
			input: ssm.PutParameterInput{
				Name:  "/valid/param",
				Type:  "String",
				Value: string(make([]byte, 5000)), // > 4096 bytes
				Tier:  "Standard",
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "tier_too_large_advanced",
			input: ssm.PutParameterInput{
				Name:  "/valid/param",
				Type:  "String",
				Value: string(make([]byte, 9000)), // > 8192 bytes
				Tier:  "Advanced",
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "invalid_tier_string",
			input: ssm.PutParameterInput{
				Name:  "/valid/param",
				Type:  "String",
				Value: "v",
				Tier:  "UnknownTier",
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "valid_ec2_image_datatype",
			input: ssm.PutParameterInput{
				Name:     "/valid/ec2",
				Type:     "String",
				Value:    "ami-12345678",
				DataType: "aws:ec2:image",
			},
			wantErr: false,
		},
		{
			name: "valid_intelligent_tiering",
			input: ssm.PutParameterInput{
				Name:  "/valid/tiering",
				Type:  "String",
				Value: "v",
				Tier:  "Intelligent-Tiering",
			},
			wantErr: false,
		},
		{
			name: "param_name_too_long",
			input: ssm.PutParameterInput{
				Name:  "/" + string(make([]byte, 2048)),
				Type:  "String",
				Value: "v",
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.PutParameter(context.TODO(), &tt.input)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestListCommands_Filtered exercises ListCommands filtering and pagination.
func TestListCommands_Filtered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults *int64
		name       string
		wantCount  int
		filterByID bool
	}{
		{
			name:       "no_filter_all_returned",
			filterByID: false,
			wantCount:  2,
		},
		{
			name:       "filter_by_command_id",
			filterByID: true,
			wantCount:  1,
		},
		{
			name:       "pagination_max_results_1",
			filterByID: false,
			maxResults: func() *int64 {
				v := int64(1)

				return &v
			}(),
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			// Create a document for commands
			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:    "AWS-RunShellScript",
				Content: `{"schemaVersion":"2.2"}`,
			})
			// AWS-RunShellScript is a default doc, error is expected
			_ = err

			out1, err := b.SendCommand(context.TODO(), &ssm.SendCommandInput{
				DocumentName: "AWS-RunShellScript",
				InstanceIDs:  []string{"i-1111"},
			})
			require.NoError(t, err)

			_, err = b.SendCommand(context.TODO(), &ssm.SendCommandInput{
				DocumentName: "AWS-RunShellScript",
				InstanceIDs:  []string{"i-2222"},
			})
			require.NoError(t, err)

			var cmdID string
			if tt.filterByID {
				cmdID = out1.Command.CommandID
			}

			listInput := &ssm.ListCommandsInput{
				CommandID:  cmdID,
				MaxResults: tt.maxResults,
			}
			listOut, err := b.ListCommands(context.TODO(), listInput)
			require.NoError(t, err)
			assert.Len(t, listOut.Commands, tt.wantCount)
		})
	}
}

// TestListCommandInvocations_Filtered exercises ListCommandInvocations branches.
func TestListCommandInvocations_Filtered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		commandID  string
		instanceID string
		wantCount  int
	}{
		{
			name:      "all_invocations",
			wantCount: 2,
		},
		{
			name:       "filter_by_instance",
			instanceID: "i-1111",
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			out, err := b.SendCommand(context.TODO(), &ssm.SendCommandInput{
				DocumentName: "AWS-RunShellScript",
				InstanceIDs:  []string{"i-1111", "i-2222"},
			})
			require.NoError(t, err)

			listOut, err := b.ListCommandInvocations(context.TODO(), &ssm.ListCommandInvocationsInput{
				CommandID:  out.Command.CommandID,
				InstanceID: tt.instanceID,
			})
			require.NoError(t, err)
			assert.Len(t, listOut.CommandInvocations, tt.wantCount)
		})
	}
}

// TestDocumentMatchesFilters exercises the documentMatchesFilters branches.
func TestDocumentMatchesFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filters    []ssm.DocumentFilter
		wantResult int
	}{
		{
			name:       "no_filters_all_returned",
			filters:    nil,
			wantResult: 3, // 2 defaults + 1 created
		},
		{
			name: "filter_by_document_type",
			filters: []ssm.DocumentFilter{
				{Key: "DocumentType", Values: []string{"Command"}},
			},
			wantResult: 3, // default docs are Command type too
		},
		{
			name: "filter_by_name",
			filters: []ssm.DocumentFilter{
				{Key: "Name", Values: []string{"TestDoc"}},
			},
			wantResult: 1,
		},
		{
			name: "filter_no_match",
			filters: []ssm.DocumentFilter{
				{Key: "Name", Values: []string{"NonExistentDoc"}},
			},
			wantResult: 0,
		},
		{
			name: "unknown_filter_key_ignored",
			filters: []ssm.DocumentFilter{
				{Key: "UnknownKey", Values: []string{"anything"}},
			},
			wantResult: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:    "TestDoc",
				Content: `{"schemaVersion":"2.2"}`,
			})
			require.NoError(t, err)

			out, err := b.ListDocuments(context.TODO(), &ssm.ListDocumentsInput{
				Filters: tt.filters,
			})
			require.NoError(t, err)
			assert.Len(t, out.DocumentIdentifiers, tt.wantResult)
		})
	}
}

// TestCopyAssocTargets exercises the copyAssocTargets nil/non-nil paths.
func TestCopyAssocTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		targets []ssm.AssociationTarget
	}{
		{
			name:    "nil_targets",
			targets: nil,
		},
		{
			name: "non_nil_targets",
			targets: []ssm.AssociationTarget{
				{Key: "tag:Env", Values: []string{"prod"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			// CreateAssociationBatch exercises copyAssocTargets internally
			out, err := b.CreateAssociationBatch(context.TODO(), &ssm.CreateAssociationBatchInput{
				Entries: []ssm.CreateAssociationBatchRequestEntry{
					{
						Name:    "AWS-RunShellScript",
						Targets: tt.targets,
					},
				},
			})
			require.NoError(t, err)
			assert.Len(t, out.Successful, 1)
		})
	}
}

// TestCreateMaintenanceWindow_Validation covers MW duration/cutoff validation.
func TestCreateMaintenanceWindow_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    ssm.CreateMaintenanceWindowInput
		wantErr  bool
		wantCode int
	}{
		{
			name: "duration_zero",
			input: ssm.CreateMaintenanceWindowInput{
				Name:     "mw-test",
				Duration: 0,
				Cutoff:   0,
				Schedule: "cron(0 2 * * ? *)",
			},
			wantErr: true,
		},
		{
			name: "duration_too_large",
			input: ssm.CreateMaintenanceWindowInput{
				Name:     "mw-test",
				Duration: 25,
				Cutoff:   1,
				Schedule: "cron(0 2 * * ? *)",
			},
			wantErr: true,
		},
		{
			name: "cutoff_equals_duration",
			input: ssm.CreateMaintenanceWindowInput{
				Name:     "mw-test",
				Duration: 4,
				Cutoff:   4,
				Schedule: "cron(0 2 * * ? *)",
			},
			wantErr: true,
		},
		{
			name: "name_empty",
			input: ssm.CreateMaintenanceWindowInput{
				Name:     "",
				Duration: 4,
				Cutoff:   1,
				Schedule: "cron(0 2 * * ? *)",
			},
			wantErr: true,
		},
		{
			name: "valid_window",
			input: ssm.CreateMaintenanceWindowInput{
				Name:     "mw-valid",
				Duration: 4,
				Cutoff:   1,
				Schedule: "cron(0 2 * * ? *)",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateMaintenanceWindow(context.TODO(), &tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestCreateOpsItem_Validation covers Title and Source validation.
func TestCreateOpsItem_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   ssm.CreateOpsItemInput
		wantErr bool
	}{
		{
			name: "missing_title",
			input: ssm.CreateOpsItemInput{
				Title:  "",
				Source: "my-service",
			},
			wantErr: true,
		},
		{
			name: "missing_source",
			input: ssm.CreateOpsItemInput{
				Title:  "Some issue",
				Source: "",
			},
			wantErr: true,
		},
		{
			name: "valid_ops_item_with_tags",
			input: ssm.CreateOpsItemInput{
				Title:  "Valid OpsItem",
				Source: "myapp",
				Tags:   []ssm.Tag{{Key: "env", Value: "prod"}},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateOpsItem(context.TODO(), &tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrValidationException)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestCreatePatchBaseline_Validation covers Name and OS default branches.
func TestCreatePatchBaseline_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantOS        string
		input         ssm.CreatePatchBaselineInput
		wantErr       bool
		expectTagsSet bool
	}{
		{
			name: "missing_name",
			input: ssm.CreatePatchBaselineInput{
				Name: "",
			},
			wantErr: true,
		},
		{
			name: "default_os_windows",
			input: ssm.CreatePatchBaselineInput{
				Name:            "my-baseline",
				OperatingSystem: "",
			},
			wantOS:  "WINDOWS",
			wantErr: false,
		},
		{
			name: "explicit_os_linux",
			input: ssm.CreatePatchBaselineInput{
				Name:            "linux-baseline",
				OperatingSystem: "AMAZON_LINUX",
			},
			wantOS:  "AMAZON_LINUX",
			wantErr: false,
		},
		{
			name: "with_tags",
			input: ssm.CreatePatchBaselineInput{
				Name: "tagged-baseline",
				Tags: []ssm.Tag{{Key: "env", Value: "prod"}},
			},
			wantOS:        "WINDOWS",
			wantErr:       false,
			expectTagsSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			out, err := b.CreatePatchBaseline(context.TODO(), &tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				bl := b.GetPatchBaselineInternal(out.BaselineID)
				assert.Equal(t, tt.wantOS, bl.OperatingSystem)
			}
		})
	}
}

// TestDeleteActivation_NotFound exercises the error path.
func TestDeleteActivation_NotFound(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, err := b.DeleteActivation(context.TODO(), &ssm.DeleteActivationInput{ActivationID: "nonexistent"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrActivationNotFound)
}

// TestDeleteAssociation_NotFound exercises the error path.
func TestDeleteAssociation_NotFound(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, err := b.DeleteAssociation(context.TODO(), &ssm.DeleteAssociationInput{AssociationID: "nonexistent"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrAssociationNotFound)
}

// TestUpdateAssociation covers update branches including nil vs non-nil targets.
func TestUpdateAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		update  ssm.UpdateAssociationInput
		wantErr bool
	}{
		{
			name: "not_found",
			update: ssm.UpdateAssociationInput{
				AssociationID: "does-not-exist",
			},
			wantErr: true,
		},
		{
			name: "update_name_and_version",
			update: ssm.UpdateAssociationInput{
				AssociationName: "updated-name",
				DocumentVersion: "$LATEST",
			},
			wantErr: false,
		},
		{
			name: "update_with_targets",
			update: ssm.UpdateAssociationInput{
				Targets: []ssm.AssociationTarget{
					{Key: "tag:Env", Values: []string{"staging"}},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()

			if !tt.wantErr {
				// Create an association first
				out, err := b.CreateAssociation(context.TODO(), &ssm.CreateAssociationInput{
					Name: "AWS-RunShellScript",
				})
				require.NoError(t, err)
				tt.update.AssociationID = out.AssociationDescription.AssociationID
			}

			_, err := b.UpdateAssociation(context.TODO(), &tt.update)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrAssociationNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestUpdateMaintenanceWindow covers all field update branches.
func TestUpdateMaintenanceWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		update  ssm.UpdateMaintenanceWindowInput
		wantErr bool
	}{
		{
			name: "not_found",
			update: ssm.UpdateMaintenanceWindowInput{
				WindowID: "mw-does-not-exist",
				Name:     "newname",
			},
			wantErr: true,
		},
		{
			name: "update_all_fields",
			update: ssm.UpdateMaintenanceWindowInput{
				Name:        "new-name",
				Description: "new-desc",
				Schedule:    "cron(0 3 * * ? *)",
				Duration:    6,
				Cutoff:      1,
				Enabled: func() *bool {
					v := false

					return &v
				}(),
			},
			wantErr: false,
		},
		{
			name:    "update_no_changes",
			update:  ssm.UpdateMaintenanceWindowInput{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()

			if !tt.wantErr {
				out, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
					Name:     "my-window",
					Duration: 4,
					Cutoff:   1,
					Schedule: "cron(0 2 * * ? *)",
				})
				require.NoError(t, err)
				tt.update.WindowID = out.WindowID
			}

			_, err := b.UpdateMaintenanceWindow(context.TODO(), &tt.update)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrMaintenanceWindowNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestUpdateOpsItem_Branches covers all conditional update paths.
func TestUpdateOpsItem_Branches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		update  ssm.UpdateOpsItemInput
		wantErr bool
	}{
		{
			name: "not_found",
			update: ssm.UpdateOpsItemInput{
				OpsItemID: "oi-does-not-exist",
				Title:     "new title",
			},
			wantErr: true,
		},
		{
			name: "update_all_fields",
			update: ssm.UpdateOpsItemInput{
				Title:       "Updated Title",
				Description: "Updated Description",
				Status:      "Resolved",
				Severity:    "2",
				Category:    "Security",
				OperationalData: map[string]ssm.OpsItemDataValue{
					"key": {Value: "val", Type: "SearchableString"},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()

			if !tt.wantErr {
				out, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
					Title:  "Initial Title",
					Source: "test",
				})
				require.NoError(t, err)
				tt.update.OpsItemID = out.OpsItemID
			}

			_, err := b.UpdateOpsItem(context.TODO(), &tt.update)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrOpsItemNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestPatchBaselineMatchesFilters covers the patchBaselineMatchesFilters branches.
func TestPatchBaselineMatchesFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filters   []ssm.PatchBaselineFilter
		wantCount int
	}{
		{
			name:      "no_filters",
			filters:   nil,
			wantCount: 1,
		},
		{
			name: "filter_by_os_match",
			filters: []ssm.PatchBaselineFilter{
				{Key: "OPERATING_SYSTEM", Values: []string{"AMAZON_LINUX"}},
			},
			wantCount: 1,
		},
		{
			name: "filter_by_os_no_match",
			filters: []ssm.PatchBaselineFilter{
				{Key: "OPERATING_SYSTEM", Values: []string{"WINDOWS"}},
			},
			wantCount: 0,
		},
		{
			name: "filter_by_name_prefix",
			filters: []ssm.PatchBaselineFilter{
				{Key: "NAME_PREFIX", Values: []string{"my-"}},
			},
			wantCount: 1,
		},
		{
			name: "filter_by_unknown_key",
			filters: []ssm.PatchBaselineFilter{
				{Key: "UNKNOWN_KEY", Values: []string{"anything"}},
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
				Name:            "my-baseline",
				OperatingSystem: "AMAZON_LINUX",
			})
			require.NoError(t, err)

			out, err := b.DescribePatchBaselines(context.TODO(), &ssm.DescribePatchBaselinesInput{
				Filters: tt.filters,
			})
			require.NoError(t, err)
			assert.Len(t, out.BaselineIdentities, tt.wantCount)
		})
	}
}

// TestOpsItemMatchesFilters covers the opsItemMatchesFilters branches.
func TestOpsItemMatchesFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filters   []ssm.OpsItemFilter
		wantCount int
	}{
		{
			name:      "no_filters",
			filters:   nil,
			wantCount: 2,
		},
		{
			name: "filter_by_status",
			filters: []ssm.OpsItemFilter{
				{Key: "Status", Operator: "Equal", Values: []string{"Open"}},
			},
			wantCount: 2,
		},
		{
			name: "filter_by_title",
			filters: []ssm.OpsItemFilter{
				{Key: "Title", Operator: "Equal", Values: []string{"Alpha Issue"}},
			},
			wantCount: 1,
		},
		{
			name: "filter_by_source",
			filters: []ssm.OpsItemFilter{
				{Key: "Source", Operator: "Equal", Values: []string{"source-a"}},
			},
			wantCount: 1,
		},
		{
			name: "unknown_filter_key",
			filters: []ssm.OpsItemFilter{
				{Key: "UnknownKey", Operator: "Equal", Values: []string{"v"}},
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
				Title:  "Alpha Issue",
				Source: "source-a",
			})
			require.NoError(t, err)
			_, err = b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
				Title:  "Beta Issue",
				Source: "source-b",
			})
			require.NoError(t, err)

			out, err := b.DescribeOpsItems(context.TODO(), &ssm.DescribeOpsItemsInput{
				OpsItemFilters: tt.filters,
			})
			require.NoError(t, err)
			assert.Len(t, out.OpsItemSummaries, tt.wantCount)
		})
	}
}

// TestClassifySSMErrorExtended covers the extended error classification paths.
func TestClassifySSMErrorExtended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *ssm.InMemoryBackend)
		name        string
		action      string
		body        string
		wantErrType string
		wantCode    int
	}{
		{
			name:        "activation_not_found_via_delete",
			action:      "DeleteActivation",
			body:        `{"ActivationId":"nonexistent"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "ActivationNotFound",
		},
		{
			name:        "association_not_found_via_delete",
			action:      "DeleteAssociation",
			body:        `{"AssociationId":"nonexistent"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "AssociationDoesNotExist",
		},
		{
			name:        "ops_item_not_found",
			action:      "GetOpsItem",
			body:        `{"OpsItemId":"oi-nonexistent"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "OpsItemNotFoundException",
		},
		{
			name:        "ops_metadata_not_found",
			action:      "GetOpsMetadata",
			body:        `{"OpsMetadataArn":"arn:aws:ssm:us-east-1:123:opsmetadata/nonexistent"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "OpsMetadataNotFoundException",
		},
		{
			name:        "patch_baseline_not_found",
			action:      "GetPatchBaseline",
			body:        `{"BaselineId":"pb-nonexistent"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "DoesNotExistException",
		},
		{
			name:        "ops_metadata_already_exists",
			action:      "CreateOpsMetadata",
			body:        `{"ResourceId":"my-resource"}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "OpsMetadataAlreadyExistsException",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
					ResourceID: "my-resource",
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantErrType, resp["__type"])
		})
	}
}

// TestGetDocumentVersion exercises GetDocument with version branches.
func TestGetDocumentVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		documentVersion string
		wantErr         bool
	}{
		{
			name:            "latest_version",
			documentVersion: "$LATEST",
			wantErr:         false,
		},
		{
			name:            "default_version",
			documentVersion: "$DEFAULT",
			wantErr:         false,
		},
		{
			name:            "explicit_version_1",
			documentVersion: "1",
			wantErr:         false,
		},
		{
			name:            "nonexistent_version",
			documentVersion: "99",
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:    "TestVersionDoc",
				Content: `{"schemaVersion":"2.2"}`,
			})
			require.NoError(t, err)

			_, err = b.GetDocument(context.TODO(), &ssm.GetDocumentInput{
				Name:            "TestVersionDoc",
				DocumentVersion: tt.documentVersion,
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrInvalidDocumentVersion)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestListDocumentVersions_Pagination exercises ListDocumentVersions pagination.
func TestListDocumentVersions_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults *int64
		nextToken  string
		wantCount  int
		wantToken  bool
	}{
		{
			name:      "all_versions",
			wantCount: 3,
			wantToken: false,
		},
		{
			name: "paginate_2",
			maxResults: func() *int64 {
				v := int64(2)

				return &v
			}(),
			wantCount: 2,
			wantToken: true,
		},
		{
			name:      "beyond_end",
			nextToken: "999",
			wantCount: 0,
			wantToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:    "PagDoc",
				Content: `{"schemaVersion":"2.2","v":"1"}`,
			})
			require.NoError(t, err)
			_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
				Name:    "PagDoc",
				Content: `{"schemaVersion":"2.2","v":"2"}`,
			})
			require.NoError(t, err)
			_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
				Name:    "PagDoc",
				Content: `{"schemaVersion":"2.2","v":"3"}`,
			})
			require.NoError(t, err)

			out, err := b.ListDocumentVersions(context.TODO(), &ssm.ListDocumentVersionsInput{
				Name:       "PagDoc",
				MaxResults: tt.maxResults,
				NextToken:  tt.nextToken,
			})
			require.NoError(t, err)
			assert.Len(t, out.DocumentVersions, tt.wantCount)
			if tt.wantToken {
				assert.NotEmpty(t, out.NextToken)
			}
		})
	}
}

// TestAddTagsToResource_NonParameter exercises tag ops for non-parameter resources.
func TestAddTagsToResource_NonParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
		wantErr      bool
	}{
		{
			name:         "maintenance_window_tags",
			resourceType: "MaintenanceWindow",
			wantErr:      false,
		},
		{
			name:         "empty_resource_type_parameter_path",
			resourceType: "Parameter",
			wantErr:      true, // because param doesn't exist
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			err := b.AddTagsToResource(context.TODO(), &ssm.AddTagsToResourceInput{
				ResourceType: tt.resourceType,
				ResourceID:   "my-resource",
				Tags:         []ssm.Tag{{Key: "k", Value: "v"}},
			})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Verify list tags works for non-parameter
				out, err2 := b.ListTagsForResource(context.TODO(), &ssm.ListTagsForResourceInput{
					ResourceType: tt.resourceType,
					ResourceID:   "my-resource",
				})
				require.NoError(t, err2)
				assert.NotEmpty(t, out.TagList)
			}
		})
	}
}

// TestRemoveTagsFromResource_NonParameter exercises remove tags for non-parameter.
func TestRemoveTagsFromResource_NonParameter(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	// Add tags first
	err := b.AddTagsToResource(context.TODO(), &ssm.AddTagsToResourceInput{
		ResourceType: "MaintenanceWindow",
		ResourceID:   "mw-1234",
		Tags:         []ssm.Tag{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}},
	})
	require.NoError(t, err)

	// Remove one tag
	err = b.RemoveTagsFromResource(context.TODO(), &ssm.RemoveTagsFromResourceInput{
		ResourceType: "MaintenanceWindow",
		ResourceID:   "mw-1234",
		TagKeys:      []string{"k1"},
	})
	require.NoError(t, err)

	// Remove from non-existent resource (should not error)
	err = b.RemoveTagsFromResource(context.TODO(), &ssm.RemoveTagsFromResourceInput{
		ResourceType: "MaintenanceWindow",
		ResourceID:   "mw-nonexistent",
		TagKeys:      []string{"k1"},
	})
	require.NoError(t, err)
}

// TestGetParameterHistory_Pagination exercises pagination with nextToken.
func TestGetParameterHistory_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nextToken string
		wantEmpty bool
	}{
		{
			name:      "beyond_end_next_token",
			nextToken: "999",
			wantEmpty: true,
		},
		{
			name:      "valid_next_token",
			nextToken: "1",
			wantEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: "/paged", Type: "String", Value: "v1"})
			_, _ = b.PutParameter(
				context.TODO(),
				&ssm.PutParameterInput{Name: "/paged", Type: "String", Value: "v2", Overwrite: true},
			)
			_, _ = b.PutParameter(
				context.TODO(),
				&ssm.PutParameterInput{Name: "/paged", Type: "String", Value: "v3", Overwrite: true},
			)

			out, err := b.GetParameterHistory(context.TODO(), &ssm.GetParameterHistoryInput{
				Name:      "/paged",
				NextToken: tt.nextToken,
			})
			require.NoError(t, err)
			if tt.wantEmpty {
				assert.Empty(t, out.Parameters)
			} else {
				assert.NotEmpty(t, out.Parameters)
			}
		})
	}
}

// TestProvider_NilContext exercises the nil-context error path.
func TestProvider_NilContext(t *testing.T) {
	t.Parallel()

	p := &ssm.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrNilAppContext)
}

// TestHandlerReset exercises the handler Reset method.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(h *ssm.Handler, b *ssm.InMemoryBackend)
		name  string
	}{
		{
			name: "reset_clears_parameters",
			setup: func(_ *ssm.Handler, b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{
					Name: "/before-reset", Type: "String", Value: "v",
				})
			},
		},
		{
			name:  "reset_on_empty_backend",
			setup: func(_ *ssm.Handler, _ *ssm.InMemoryBackend) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			tt.setup(h, b)
			h.Reset()
			assert.Empty(t, b.ListAll(context.TODO()))
		})
	}
}

// TestUpdateDocument_Version exercises UpdateDocument version validation.
func TestUpdateDocument_Version(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		documentVersion string
		wantErr         bool
	}{
		{
			name:            "no_version",
			documentVersion: "",
			wantErr:         false,
		},
		{
			name:            "latest_version",
			documentVersion: "$LATEST",
			wantErr:         false,
		},
		{
			name:            "default_version",
			documentVersion: "$DEFAULT",
			wantErr:         false,
		},
		{
			name:            "explicit_current_version",
			documentVersion: "1",
			wantErr:         false,
		},
		{
			name:            "wrong_version",
			documentVersion: "99",
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:    "UpdDoc",
				Content: `{"schemaVersion":"2.2"}`,
			})
			require.NoError(t, err)

			_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
				Name:            "UpdDoc",
				Content:         `{"schemaVersion":"2.2","v":"2"}`,
				DocumentVersion: tt.documentVersion,
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrInvalidDocumentVersion)
			} else {
				require.NoError(t, err)
			}
		})
	}
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

// TestCreateMaintenanceWindow_WithTags covers the tags path.
func TestCreateMaintenanceWindow_WithTags(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	out, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "tagged-window",
		Duration: 4,
		Cutoff:   1,
		Schedule: "cron(0 2 * * ? *)",
		Tags:     []ssm.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.WindowID)
}

// TestListCommands_EmptyWithPagination covers the empty-result pagination branch.
func TestListCommands_EmptyWithPagination(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	out, err := b.ListCommands(context.TODO(), &ssm.ListCommandsInput{
		NextToken: "999",
	})
	require.NoError(t, err)
	assert.Empty(t, out.Commands)
}

// TestListCommandInvocations_EmptyPagination covers the empty-result pagination path.
func TestListCommandInvocations_EmptyPagination(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	out, err := b.ListCommandInvocations(context.TODO(), &ssm.ListCommandInvocationsInput{
		NextToken: "999",
	})
	require.NoError(t, err)
	assert.Empty(t, out.CommandInvocations)
}

// TestGetParametersByPath_StartBeyondEnd exercises the beyond-end pagination branch.
func TestGetParametersByPath_StartBeyondEnd(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: "/app/x", Type: "String", Value: "v"})

	out, err := b.GetParametersByPath(context.TODO(), &ssm.GetParametersByPathInput{
		Path:      "/app",
		Recursive: true,
		NextToken: "999",
	})
	require.NoError(t, err)
	assert.Empty(t, out.Parameters)
}

// TestListDocuments_Pagination exercises ListDocuments pagination.
func TestListDocuments_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults *int64
		nextToken  string
		wantEmpty  bool
	}{
		{
			name: "paginate_1",
			maxResults: func() *int64 {
				v := int64(1)

				return &v
			}(),
			wantEmpty: false,
		},
		{
			name:      "beyond_end",
			nextToken: "999",
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			// There are already 2 default docs
			out, err := b.ListDocuments(context.TODO(), &ssm.ListDocumentsInput{
				MaxResults: tt.maxResults,
				NextToken:  tt.nextToken,
			})
			require.NoError(t, err)
			if tt.wantEmpty {
				assert.Empty(t, out.DocumentIdentifiers)
			} else {
				assert.NotEmpty(t, out.DocumentIdentifiers)
				assert.NotEmpty(t, out.NextToken)
			}
		})
	}
}
