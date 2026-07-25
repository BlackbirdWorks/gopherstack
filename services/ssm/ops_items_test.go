package ssm_test

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestStubOps_DeleteOpsItem exercises the opsitem-backed delete stub.
func TestStubOps_DeleteOpsItem(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	item, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
		Title:       "test-item",
		Description: "test",
		Source:      "test",
		Severity:    "2",
		Category:    "Performance",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
	rec := doRequest(t, h, "DeleteOpsItem", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_DescribeOpsItems exercises that stub.
func TestStubOps_DescribeOpsItems(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := doRequest(t, h, "DescribeOpsItems", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_GetOpsItem exercises that stub.
func TestStubOps_GetOpsItem(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	item, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
		Title:       "test-item-2",
		Description: "test",
		Source:      "test",
		Severity:    "2",
		Category:    "Performance",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
	rec := doRequest(t, h, "GetOpsItem", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_UpdateOpsItem exercises that stub.
func TestStubOps_UpdateOpsItem(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	item, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
		Title:       "test-item-3",
		Description: "test",
		Source:      "test",
		Severity:    "2",
		Category:    "Performance",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"OpsItemId": item.OpsItemID, "Title": "updated-item"})
	rec := doRequest(t, h, "UpdateOpsItem", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}
func TestCreateOpsItem_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "valid_ops_item",
			body: `{"Title":"My OpsItem","Source":"my-app",` +
				`"Description":"test issue","Severity":"2","Category":"Availability"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "minimal_ops_item",
			body:       `{"Title":"Minimal","Source":"svc"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			rec := doRequest(t, h, "CreateOpsItem", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreateOpsItemOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.OpsItemID)
			assert.NotEmpty(t, resp.OpsItemArn)
			assert.True(t, strings.HasPrefix(resp.OpsItemID, "oi-"))
			assert.Equal(t, 1, backend.OpsItemCount())
		})
	}
}
func TestCreateOpsItem_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "missing_title",
			body:       `{"Source":"my-app"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
		{
			name:       "missing_source",
			body:       `{"Title":"My Title"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "CreateOpsItem", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)
		})
	}
}
func TestAssociateOpsItemRelatedItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantErr    string
		wantStatus int
		createItem bool
	}{
		{
			name:       "success",
			createItem: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "ops_item_not_found",
			createItem: false,
			body: `{"OpsItemId":"oi-nonexistent","AssociationType":"RelatesTo",` +
				`"ResourceType":"AWS::EC2::Instance",` +
				`"ResourceUri":"arn:aws:ec2:us-east-1:123:instance/i-abc"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "OpsItemNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			body := tt.body
			if tt.createItem {
				rec := doRequest(t, h, "CreateOpsItem", `{"Title":"item","Source":"src"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				var createResp ssm.CreateOpsItemOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

				body = `{"OpsItemId":"` + createResp.OpsItemID +
					`","AssociationType":"RelatesTo","ResourceType":"AWS::EC2::Instance",` +
					`"ResourceUri":"arn:aws:ec2:us-east-1:123:instance/i-abc"}`
			}

			rec := doRequest(t, h, "AssociateOpsItemRelatedItem", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErr != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErr)
			} else {
				var resp ssm.AssociateOpsItemRelatedItemOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.AssociationID)
			}
		})
	}
}

// TestAccountID_Region verifies AccountID and Region methods.
func TestAccountID_Region(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantAcct   string
		wantRegion string
	}{
		{
			name:       "default_values",
			wantAcct:   "123456789012",
			wantRegion: "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			assert.Equal(t, tt.wantAcct, b.AccountID())
			assert.Equal(t, tt.wantRegion, b.Region())
		})
	}
}
func TestSSMBounds_ListOpsItemRelatedItems(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.ListOpsItemRelatedItems(ctx, &ssm.ListOpsItemRelatedItemsInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}
func TestSSMBounds_ListOpsItemEvents(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.ListOpsItemEvents(ctx, &ssm.ListOpsItemEventsInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}
func TestBackendOps_GetOpsItem(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestOpsItem(t, b)

	out, err := b.GetOpsItem(context.TODO(), &ssm.GetOpsItemInput{OpsItemID: id})
	require.NoError(t, err)
	assert.Equal(t, id, out.OpsItem.OpsItemID)
}
func TestBackendOps_DeleteOpsItem(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestOpsItem(t, b)

	out, err := b.DeleteOpsItem(context.TODO(), &ssm.DeleteOpsItemInput{OpsItemID: id})
	require.NoError(t, err)
	assert.NotNil(t, out)

	// Should be gone.
	_, err = b.GetOpsItem(context.TODO(), &ssm.GetOpsItemInput{OpsItemID: id})
	require.Error(t, err)
}
func TestBackendOps_DescribeOpsItems(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	createTestOpsItem(t, b)
	createTestOpsItem(t, b)

	out, err := b.DescribeOpsItems(context.TODO(), &ssm.DescribeOpsItemsInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.OpsItemSummaries), 2)
}
func TestBackendOps_UpdateOpsItem(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestOpsItem(t, b)

	out, err := b.UpdateOpsItem(context.TODO(), &ssm.UpdateOpsItemInput{
		OpsItemID: id,
		Title:     "updated-title",
		Status:    "Resolved",
	})
	require.NoError(t, err)
	assert.NotNil(t, out)

	// Verify update took effect.
	getOut, err := b.GetOpsItem(context.TODO(), &ssm.GetOpsItemInput{OpsItemID: id})
	require.NoError(t, err)
	assert.Equal(t, "updated-title", getOut.OpsItem.Title)
	assert.Equal(t, "Resolved", getOut.OpsItem.Status)
}

// TestOpsItem_PriorityRoundTrip locks in the Priority field (confirmed
// against aws-sdk-go-v2/service/ssm@v1.71.0's api_op_CreateOpsItem.go/
// api_op_UpdateOpsItem.go -- "the importance of this OpsItem in relation to
// other OpsItems"), which was previously entirely absent from
// CreateOpsItemInput/UpdateOpsItemInput/OpsItem/OpsItemSummary and silently
// discarded even when a client sent it.
func TestOpsItem_PriorityRoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	ctx := context.TODO()

	created, err := b.CreateOpsItem(ctx, &ssm.CreateOpsItemInput{
		Title: "prioritized", Source: "test", Priority: 1,
	})
	require.NoError(t, err)

	got, err := b.GetOpsItem(ctx, &ssm.GetOpsItemInput{OpsItemID: created.OpsItemID})
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.OpsItem.Priority)

	described, err := b.DescribeOpsItems(ctx, &ssm.DescribeOpsItemsInput{})
	require.NoError(t, err)
	require.Len(t, described.OpsItemSummaries, 1)
	assert.EqualValues(t, 1, described.OpsItemSummaries[0].Priority)

	newPriority := int32(5)
	_, err = b.UpdateOpsItem(ctx, &ssm.UpdateOpsItemInput{
		OpsItemID: created.OpsItemID, Priority: &newPriority,
	})
	require.NoError(t, err)

	got, err = b.GetOpsItem(ctx, &ssm.GetOpsItemInput{OpsItemID: created.OpsItemID})
	require.NoError(t, err)
	assert.EqualValues(t, 5, got.OpsItem.Priority)
}

// TestOpsItem_ChangeManagerFieldsRoundTrip locks in the previously-missing
// CreateOpsItemInput/UpdateOpsItemInput fields (bd gopherstack-iq4m):
// AccountId/ActualStartTime/ActualEndTime/Notifications/PlannedStartTime/
// PlannedEndTime/RelatedOpsItems, confirmed present in aws-sdk-go-v2
// v1.71.0's api_op_CreateOpsItem.go/api_op_UpdateOpsItem.go.
func TestOpsItem_ChangeManagerFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	plannedStart := float64(1700000000)
	plannedEnd := float64(1700003600)
	actualStart := float64(1700000100)
	actualEnd := float64(1700003700)

	cases := []struct {
		update *ssm.UpdateOpsItemInput
		check  func(t *testing.T, item ssm.OpsItem)
		name   string
		create ssm.CreateOpsItemInput
	}{
		{
			name: "create_populates_change_manager_fields",
			create: ssm.CreateOpsItemInput{
				Title:            "change request",
				Source:           "test",
				OpsItemType:      "/aws/changerequest",
				AccountID:        "123456789012",
				PlannedStartTime: &plannedStart,
				PlannedEndTime:   &plannedEnd,
				Notifications:    []ssm.OpsItemNotification{{Arn: "arn:aws:sns:us-east-1:123456789012:topic"}},
				RelatedOpsItems:  []ssm.RelatedOpsItemRef{{OpsItemID: "oi-other"}},
			},
			check: func(t *testing.T, item ssm.OpsItem) {
				t.Helper()
				assert.Equal(t, "123456789012", item.AccountID)
				require.NotNil(t, item.PlannedStartTime)
				assert.InDelta(t, plannedStart, *item.PlannedStartTime, 0)
				require.NotNil(t, item.PlannedEndTime)
				assert.InDelta(t, plannedEnd, *item.PlannedEndTime, 0)
				require.Len(t, item.Notifications, 1)
				assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:topic", item.Notifications[0].Arn)
				require.Len(t, item.RelatedOpsItems, 1)
				assert.Equal(t, "oi-other", item.RelatedOpsItems[0].OpsItemID)
			},
		},
		{
			name: "update_sets_actual_times_and_related_items",
			create: ssm.CreateOpsItemInput{
				Title:       "change request 2",
				Source:      "test",
				OpsItemType: "/aws/changerequest",
			},
			update: &ssm.UpdateOpsItemInput{
				ActualStartTime: &actualStart,
				ActualEndTime:   &actualEnd,
				RelatedOpsItems: []ssm.RelatedOpsItemRef{{OpsItemID: "oi-related-1"}, {OpsItemID: "oi-related-2"}},
			},
			check: func(t *testing.T, item ssm.OpsItem) {
				t.Helper()
				require.NotNil(t, item.ActualStartTime)
				assert.InDelta(t, actualStart, *item.ActualStartTime, 0)
				require.NotNil(t, item.ActualEndTime)
				assert.InDelta(t, actualEnd, *item.ActualEndTime, 0)
				require.Len(t, item.RelatedOpsItems, 2)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			ctx := context.Background()

			created, err := b.CreateOpsItem(ctx, &tc.create)
			require.NoError(t, err)

			if tc.update != nil {
				tc.update.OpsItemID = created.OpsItemID
				_, err = b.UpdateOpsItem(ctx, tc.update)
				require.NoError(t, err)
			}

			got, err := b.GetOpsItem(ctx, &ssm.GetOpsItemInput{OpsItemID: created.OpsItemID})
			require.NoError(t, err)
			tc.check(t, got.OpsItem)
		})
	}
}

func TestBackendOps_DisassociateOpsItemRelatedItem(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestOpsItem(t, b)

	// Associate a related item first.
	assocOut, err := b.AssociateOpsItemRelatedItem(context.TODO(), &ssm.AssociateOpsItemRelatedItemInput{
		OpsItemID:       id,
		AssociationType: "IsRelatedTo",
		ResourceType:    "AWS::EC2::Instance",
		ResourceURI:     "arn:aws:ec2:us-east-1:123456789012:instance/i-test",
	})
	require.NoError(t, err)

	// Disassociate.
	out, err := b.DisassociateOpsItemRelatedItem(context.TODO(), &ssm.DisassociateOpsItemRelatedItemInput{
		OpsItemID:     id,
		AssociationID: assocOut.AssociationID,
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
func TestBackendOps_ListOpsItemRelatedItems(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestOpsItem(t, b)

	_, err := b.AssociateOpsItemRelatedItem(context.TODO(), &ssm.AssociateOpsItemRelatedItemInput{
		OpsItemID:       id,
		AssociationType: "IsRelatedTo",
		ResourceType:    "AWS::EC2::Instance",
		ResourceURI:     "arn:aws:ec2:us-east-1:123456789012:instance/i-related",
	})
	require.NoError(t, err)

	out, err := b.ListOpsItemRelatedItems(context.TODO(), &ssm.ListOpsItemRelatedItemsInput{OpsItemID: id})
	require.NoError(t, err)
	assert.Len(t, out.Summaries, 1)
}
func TestBackendOps_ListOpsItemEvents(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.ListOpsItemEvents(context.TODO(), &ssm.ListOpsItemEventsInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.Summaries)
}
func TestFull_OpsItem_FullLifecycle(t *testing.T) {
	t.Parallel()
	h := newHandler()

	// Create
	code, out := postJSON(t, h, "CreateOpsItem", map[string]any{
		"Title":       "High CPU Usage",
		"Source":      "CloudWatch",
		"Severity":    "2",
		"Category":    "Performance",
		"Description": "CPU exceeded 90% for 15 minutes",
	})
	assert.Equal(t, http.StatusOK, code)
	opsItemID := out["OpsItemId"].(string)
	assert.NotEmpty(t, opsItemID)

	// Get
	code, out = postJSON(t, h, "GetOpsItem", map[string]any{"OpsItemId": opsItemID})
	assert.Equal(t, http.StatusOK, code)
	item := out["OpsItem"].(map[string]any)
	assert.Equal(t, "High CPU Usage", item["Title"])
	assert.Equal(t, "Open", item["Status"])

	// Describe
	code, out = postJSON(t, h, "DescribeOpsItems", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	items := out["OpsItemSummaries"].([]any)
	assert.NotEmpty(t, items)

	// Update
	code, _ = postJSON(t, h, "UpdateOpsItem", map[string]any{
		"OpsItemId": opsItemID,
		"Status":    "InProgress",
		"Title":     "High CPU - Under Investigation",
	})
	assert.Equal(t, http.StatusOK, code)

	// Verify update
	code, out = postJSON(t, h, "GetOpsItem", map[string]any{"OpsItemId": opsItemID})
	assert.Equal(t, http.StatusOK, code)
	item = out["OpsItem"].(map[string]any)
	assert.Equal(t, "InProgress", item["Status"])
	assert.Equal(t, "High CPU - Under Investigation", item["Title"])

	// AssociateRelatedItem
	code, out = postJSON(t, h, "AssociateOpsItemRelatedItem", map[string]any{
		"OpsItemId":       opsItemID,
		"AssociationType": "IsParentOf",
		"ResourceType":    "AWS::EC2::Instance",
		"ResourceUri":     "arn:aws:ec2:us-east-1:123:instance/i-abc",
	})
	assert.Equal(t, http.StatusOK, code)
	assocID := out["AssociationId"].(string)
	assert.NotEmpty(t, assocID)

	// ListRelatedItems
	code, out = postJSON(t, h, "ListOpsItemRelatedItems", map[string]any{"OpsItemId": opsItemID})
	assert.Equal(t, http.StatusOK, code)
	relatedItems := out["Summaries"].([]any)
	assert.Len(t, relatedItems, 1)

	// DisassociateRelatedItem
	code, _ = postJSON(t, h, "DisassociateOpsItemRelatedItem", map[string]any{
		"OpsItemId":     opsItemID,
		"AssociationId": assocID,
	})
	assert.Equal(t, http.StatusOK, code)

	// Delete
	code, _ = postJSON(t, h, "DeleteOpsItem", map[string]any{"OpsItemId": opsItemID})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = postJSON(t, h, "GetOpsItem", map[string]any{"OpsItemId": opsItemID})
	assert.Equal(t, http.StatusBadRequest, code)
}
func TestFull_OpsItem_FilterByStatus(t *testing.T) {
	t.Parallel()
	h := newHandler()

	_, out1 := postJSON(t, h, "CreateOpsItem", map[string]any{
		"Title":  "Item A",
		"Source": "manual",
	})
	id1 := out1["OpsItemId"].(string)

	postJSON(t, h, "CreateOpsItem", map[string]any{
		"Title":  "Item B",
		"Source": "manual",
	})

	// Close item A
	postJSON(t, h, "UpdateOpsItem", map[string]any{
		"OpsItemId": id1,
		"Status":    "Resolved",
	})

	code, out := postJSON(t, h, "DescribeOpsItems", map[string]any{
		"OpsItemFilters": []map[string]any{
			{"Key": "Status", "Values": []string{"Open"}, "Operator": "Equal"},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	items := out["OpsItemSummaries"].([]any)
	assert.Len(t, items, 1)
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
func TestOpsItem_OperationalData_CreateAndUpdate(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create with initial OperationalData.
	item, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
		Title:  "DB Connection Failure",
		Source: "ec2",
		OperationalData: map[string]ssm.OpsItemDataValue{
			"/aws/resources": {
				Type:  "SearchableString",
				Value: `[{"arn":"arn:aws:ec2:us-east-1:123456789012:instance/i-abc"}]`,
			},
		},
	})
	require.NoError(t, err)

	// GetOpsItem should have initial OperationalData.
	body, _ := json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
	rec := doRequest(t, h, "GetOpsItem", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "/aws/resources")

	// Update with additional OperationalData key.
	body, _ = json.Marshal(map[string]any{
		"OpsItemId": item.OpsItemID,
		"OperationalData": map[string]any{
			"/aws/automations": map[string]any{
				"Type":  "SearchableString",
				"Value": `[{"automationId":"auto-123"}]`,
			},
		},
	})
	rec = doRequest(t, h, "UpdateOpsItem", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// GetOpsItem should have both keys.
	body, _ = json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
	rec = doRequest(t, h, "GetOpsItem", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "/aws/resources")
	assert.Contains(t, rec.Body.String(), "/aws/automations")
}
func TestOpsItem_OperationalData_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialData   map[string]ssm.OpsItemDataValue
		updateData    map[string]ssm.OpsItemDataValue
		wantAfterKeys []string
	}{
		{
			name: "merge_new_key",
			initialData: map[string]ssm.OpsItemDataValue{
				"key1": {Value: "val1"},
			},
			updateData: map[string]ssm.OpsItemDataValue{
				"key2": {Value: "val2"},
			},
			wantAfterKeys: []string{"key1", "key2"},
		},
		{
			name: "overwrite_existing_key",
			initialData: map[string]ssm.OpsItemDataValue{
				"key1": {Value: "old"},
			},
			updateData: map[string]ssm.OpsItemDataValue{
				"key1": {Value: "new"},
			},
			wantAfterKeys: []string{"key1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			item, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
				Title:           "Test Item",
				Source:          "test",
				OperationalData: tt.initialData,
			})
			require.NoError(t, err)

			body, _ := json.Marshal(map[string]any{
				"OpsItemId":       item.OpsItemID,
				"OperationalData": tt.updateData,
			})
			rec := doRequest(t, h, "UpdateOpsItem", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			body, _ = json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
			rec = doRequest(t, h, "GetOpsItem", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			for _, key := range tt.wantAfterKeys {
				assert.Contains(t, rec.Body.String(), key)
			}
		})
	}
}
func TestOpsItemEvents_TrackCreates(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	item, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
		Title:  "Tracked Item",
		Source: "automation",
	})
	require.NoError(t, err)

	// ListOpsItemEvents for this item should return at least one event.
	body, _ := json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
	rec := doRequest(t, h, "ListOpsItemEvents", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["Summaries"].([]any)
	assert.NotEmpty(t, summaries, "should have at least one event after create")
}
func TestOpsItemEvents_TrackUpdates(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	item, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
		Title:  "Updated Item",
		Source: "ec2",
	})
	require.NoError(t, err)

	// Update the item.
	body, _ := json.Marshal(map[string]any{
		"OpsItemId":   item.OpsItemID,
		"Description": "Updated description",
	})
	doRequest(t, h, "UpdateOpsItem", string(body))

	// ListOpsItemEvents should have multiple events now.
	body, _ = json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
	rec := doRequest(t, h, "ListOpsItemEvents", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["Summaries"].([]any)
	assert.GreaterOrEqual(t, len(summaries), 2, "create + update should each produce an event")
}
func TestOpsItemEvents_FilterByOpsItemID(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	item1, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{Title: "Item 1", Source: "ec2"})
	require.NoError(t, err)
	item2, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{Title: "Item 2", Source: "rds"})
	require.NoError(t, err)

	// Filter by item1 — should only get item1 events.
	body, _ := json.Marshal(map[string]any{"OpsItemId": item1.OpsItemID})
	rec := doRequest(t, h, "ListOpsItemEvents", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), item1.OpsItemID)
	assert.NotContains(t, rec.Body.String(), item2.OpsItemID)
}

// TestUpdateOpsItem_TableDriven verifies UpdateOpsItem with multiple field updates.
func TestUpdateOpsItem_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update     map[string]any
		name       string
		wantErrMsg string
		wantStatus int
		setupFirst bool
	}{
		{
			name:       "updates_title_and_status",
			setupFirst: true,
			update:     map[string]any{"Title": "Updated Title", "Status": "Resolved"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_ops_item_returns_error",
			setupFirst: false,
			update:     map[string]any{"Title": "Should Fail"},
			wantStatus: http.StatusBadRequest,
			wantErrMsg: "OpsItemNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			opsItemID := "oi-nonexistent"
			if tt.setupFirst {
				item, err := b.CreateOpsItem(context.Background(), &ssm.CreateOpsItemInput{
					Title:    "Original Title",
					Source:   "test",
					Severity: "2",
					Category: "Performance",
				})
				require.NoError(t, err)
				opsItemID = item.OpsItemID
			}

			payload := map[string]any{"OpsItemId": opsItemID}
			maps.Copy(payload, tt.update)

			body, _ := json.Marshal(payload)
			rec := doRequest(t, h, "UpdateOpsItem", string(body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrMsg != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrMsg)
			}
		})
	}
}
