package shield_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_RequestValidation(t *testing.T) {
	t.Parallel()

	const eip = "arn:aws:ec2:us-east-1:000000000000:eip-allocation/eipalloc-1"

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantCode string
		wantMsg  string
		want     int
	}{
		{
			name:     "protection_name_bad_chars",
			op:       "CreateProtection",
			body:     map[string]any{"Name": "bad name!", "ResourceArn": eip},
			want:     400,
			wantCode: "InvalidParameterException",
			wantMsg:  "Name",
		},
		{
			name:     "protection_name_too_long",
			op:       "CreateProtection",
			body:     map[string]any{"Name": strings.Repeat("a", 129), "ResourceArn": eip},
			want:     400,
			wantCode: "InvalidParameterException",
			wantMsg:  "Name",
		},
		{
			name: "protection_name_ok",
			op:   "CreateProtection",
			body: map[string]any{"Name": "my.name_1-x y", "ResourceArn": eip},
			want: 200,
		},
		{
			name: "group_members_with_all",
			op:   "CreateProtectionGroup",
			body: map[string]any{
				"ProtectionGroupId": "g1",
				"Aggregation":       "SUM",
				"Pattern":           "ALL",
				"Members":           []string{eip},
			},
			want:     400,
			wantCode: "InvalidParameterException",
			wantMsg:  "Members",
		},
		{
			name: "group_resource_type_with_all",
			op:   "CreateProtectionGroup",
			body: map[string]any{
				"ProtectionGroupId": "g1",
				"Aggregation":       "SUM",
				"Pattern":           "ALL",
				"ResourceType":      "EC2_INSTANCE",
			},
			want:     400,
			wantCode: "InvalidParameterException",
			wantMsg:  "ResourceType",
		},
		{
			name:     "group_id_bad_chars",
			op:       "CreateProtectionGroup",
			body:     map[string]any{"ProtectionGroupId": "bad_id", "Aggregation": "SUM", "Pattern": "ALL"},
			want:     400,
			wantCode: "InvalidParameterException",
			wantMsg:  "ProtectionGroupId",
		},
		{
			name: "group_ok",
			op:   "CreateProtectionGroup",
			body: map[string]any{
				"ProtectionGroupId": "g-1",
				"Aggregation":       "SUM",
				"Pattern":           "ARBITRARY",
				"Members":           []string{eip},
			},
			want: 200,
		},
		{
			name:     "contact_bad_email",
			op:       "UpdateEmergencyContactSettings",
			body:     map[string]any{"EmergencyContactList": []map[string]string{{"EmailAddress": "notanemail"}}},
			want:     400,
			wantCode: "InvalidParameterException",
			wantMsg:  "EmailAddress",
		},
		{
			name: "contact_bad_phone",
			op:   "UpdateEmergencyContactSettings",
			body: map[string]any{
				"EmergencyContactList": []map[string]string{{"EmailAddress": "a@b.com", "PhoneNumber": "555-0100"}},
			},
			want:     400,
			wantCode: "InvalidParameterException",
			wantMsg:  "PhoneNumber",
		},
		{
			name: "contact_ok",
			op:   "UpdateEmergencyContactSettings",
			body: map[string]any{
				"EmergencyContactList": []map[string]string{{"EmailAddress": "a@b.com", "PhoneNumber": "+14155550100"}},
			},
			want: 200,
		},
		{
			name:     "engagement_bad_email",
			op:       "AssociateProactiveEngagementDetails",
			body:     map[string]any{"EmergencyContactList": []map[string]string{{"EmailAddress": "x"}}},
			want:     400,
			wantCode: "InvalidParameterException",
			wantMsg:  "EmailAddress",
		},
		{
			name:     "drt_role_not_arn",
			op:       "AssociateDRTRole",
			body:     map[string]any{"RoleArn": "notarn"},
			want:     400,
			wantCode: "InvalidParameterException",
			wantMsg:  "RoleArn",
		},
		{
			name: "drt_role_ok",
			op:   "AssociateDRTRole",
			body: map[string]any{"RoleArn": "arn:aws:iam::000000000000:role/drt"},
			want: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.CreateSubscription())

			rec := doShieldRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.want, rec.Code, rec.Body.String())

			if tt.want == http.StatusOK {
				return
			}

			var out struct {
				Type    string `json:"__type"`
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantCode, out.Type)
			assert.Contains(t, out.Message, tt.wantMsg)
		})
	}
}

func TestHandler_ErrorMessagesOmitCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       string
		body     map[string]any
		wantCode string
		setup    bool
	}{
		{
			name: "no_subscription",
			op:   "CreateProtection",
			body: map[string]any{
				"Name":        "p",
				"ResourceArn": "arn:aws:ec2:us-east-1:000000000000:eip-allocation/e-1",
			},
			wantCode: "InvalidOperationException",
		},
		{
			name:     "not_found",
			op:       "DescribeProtection",
			body:     map[string]any{"ProtectionId": "00000000-0000-0000-0000-000000000000"},
			wantCode: "ResourceNotFoundException",
			setup:    true,
		},
		{
			name:     "invalid_param",
			op:       "DescribeProtection",
			body:     map[string]any{},
			wantCode: "InvalidParameterException",
			setup:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup {
				require.NoError(t, h.Backend.CreateSubscription())
			}

			rec := doShieldRequest(t, h, tt.op, tt.body)

			var out struct {
				Type    string `json:"__type"`
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantCode, out.Type)
			assert.NotContains(t, out.Message, tt.wantCode)
			assert.NotContains(t, out.Message, "invalid request:")
			assert.NotEmpty(t, out.Message)
		})
	}
}
