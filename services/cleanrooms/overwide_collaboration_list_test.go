package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOverWideCollaborationListResponses asserts on the raw response body
// that every ListCollaboration* op in this service emits only the fields
// the real AWS Collaboration*Summary type declares (types.go, cleanrooms@
// v1.49.4) -- not membershipArn/membershipId, which belong only to the
// membership-scoped sibling List op. A typed aws-sdk-go-v2 client cannot
// see this class of bug: it silently discards any key it does not model,
// so a typed-client assertion (as sdk_response_keys_test.go's collaboration
// tests already do) would pass whether or not the fix is applied. Only the
// raw serialized JSON proves the leaked fields are actually absent and the
// required creatorAccountId is actually present.
func TestOverWideCollaborationListResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw       func(t *testing.T) []byte
		name      string
		itemsKey  string
		required  []string
		forbidden []string
	}{
		{
			name:     "list collaboration analysis templates omits membership fields",
			itemsKey: "collaborationAnalysisTemplateSummaries",
			required: []string{
				"arn",
				"collaborationArn",
				"collaborationId",
				"creatorAccountId",
				"id",
				"name",
			},
			// CollaborationAnalysisTemplateSummary (types.go) declares
			// creatorAccountId, never membershipArn/membershipId --
			// AnalysisTemplateSummary (the membership-scoped sibling)
			// declares the opposite.
			forbidden: []string{"membershipArn", "membershipId"},
			raw: func(t *testing.T) []byte {
				t.Helper()

				e := newTestServer(t)
				collabID, memID := createCleanroomsCollabAndMembership(t, e)

				createRec := doRequest(
					t,
					e,
					http.MethodPost,
					"/memberships/"+memID+"/analysistemplates",
					map[string]any{
						"name":   "wide-template",
						"format": "SQL",
						"source": map[string]any{"text": "SELECT 1"},
					},
				)
				require.Equal(t, http.StatusOK, createRec.Code)

				listRec := doRequest(
					t,
					e,
					http.MethodGet,
					"/collaborations/"+collabID+"/analysistemplates",
					nil,
				)
				require.Equal(t, http.StatusOK, listRec.Code)

				return listRec.Body.Bytes()
			},
		},
		{
			name:     "list collaboration configured audience model associations omits membership fields",
			itemsKey: "collaborationConfiguredAudienceModelAssociationSummaries",
			required: []string{
				"arn",
				"collaborationArn",
				"collaborationId",
				"creatorAccountId",
				"id",
				"name",
			},
			forbidden: []string{"membershipArn", "membershipId"},
			raw: func(t *testing.T) []byte {
				t.Helper()

				e := newTestServer(t)
				collabID, memID := createCleanroomsCollabAndMembership(t, e)

				createRec := doRequest(
					t,
					e,
					http.MethodPost,
					"/memberships/"+memID+"/configuredaudiencemodelassociations",
					map[string]any{
						"configuredAudienceModelAssociationName": "wide-cama",
						"configuredAudienceModelArn":             "arn:aws:cleanrooms-ml::123456789012:configured-audience-model/fixture",
						"manageResourcePolicies":                 true,
					},
				)
				require.Equal(t, http.StatusOK, createRec.Code)

				listRec := doRequest(
					t,
					e,
					http.MethodGet,
					"/collaborations/"+collabID+"/configuredaudiencemodelassociations",
					nil,
				)
				require.Equal(t, http.StatusOK, listRec.Code)

				return listRec.Body.Bytes()
			},
		},
		{
			name:     "list collaboration id namespace associations omits membership fields",
			itemsKey: "collaborationIdNamespaceAssociationSummaries",
			required: []string{
				"arn",
				"collaborationArn",
				"collaborationId",
				"creatorAccountId",
				"id",
				"name",
			},
			forbidden: []string{"membershipArn", "membershipId"},
			raw: func(t *testing.T) []byte {
				t.Helper()

				e := newTestServer(t)
				collabID, memID := createCleanroomsCollabAndMembership(t, e)

				createRec := doRequest(
					t,
					e,
					http.MethodPost,
					"/memberships/"+memID+"/idnamespaceassociations",
					map[string]any{
						"name": "wide-ns",
						"inputReferenceConfig": map[string]any{
							"inputReferenceArn":      "arn:aws:cleanrooms:us-east-1:123456789012:membership/" + memID,
							"manageResourcePolicies": true,
						},
					},
				)
				require.Equal(t, http.StatusOK, createRec.Code)

				listRec := doRequest(
					t,
					e,
					http.MethodGet,
					"/collaborations/"+collabID+"/idnamespaceassociations",
					nil,
				)
				require.Equal(t, http.StatusOK, listRec.Code)

				return listRec.Body.Bytes()
			},
		},
		{
			name:     "list collaboration privacy budget templates omits membership fields",
			itemsKey: "collaborationPrivacyBudgetTemplateSummaries",
			required: []string{
				"arn",
				"collaborationArn",
				"collaborationId",
				"creatorAccountId",
				"id",
				"privacyBudgetType",
			},
			forbidden: []string{"membershipArn", "membershipId"},
			raw: func(t *testing.T) []byte {
				t.Helper()

				e := newTestServer(t)
				collabID, memID := createCleanroomsCollabAndMembership(t, e)

				createRec := doRequest(
					t,
					e,
					http.MethodPost,
					"/memberships/"+memID+"/privacybudgettemplates",
					map[string]any{
						"privacyBudgetType": "DIFFERENTIAL_PRIVACY",
						"autoRefresh":       "CALENDAR_MONTH",
						"parameters": map[string]any{
							"differentialPrivacy": map[string]any{
								"epsilon":            10,
								"usersNoisePerQuery": 100,
							},
						},
					},
				)
				require.Equal(t, http.StatusOK, createRec.Code)

				listRec := doRequest(
					t,
					e,
					http.MethodGet,
					"/collaborations/"+collabID+"/privacybudgettemplates",
					nil,
				)
				require.Equal(t, http.StatusOK, listRec.Code)

				return listRec.Body.Bytes()
			},
		},
		{
			name:     "list collaboration privacy budgets omits membership fields",
			itemsKey: "collaborationPrivacyBudgetSummaries",
			required: []string{
				"budget",
				"collaborationArn",
				"collaborationId",
				"creatorAccountId",
				"id",
				"type",
			},
			forbidden: []string{"membershipArn", "membershipId"},
			raw: func(t *testing.T) []byte {
				t.Helper()

				e := newTestServer(t)
				collabID, memID := createCleanroomsCollabAndMembership(t, e)

				createRec := doRequest(
					t,
					e,
					http.MethodPost,
					"/memberships/"+memID+"/privacybudgettemplates",
					map[string]any{
						"privacyBudgetType": "DIFFERENTIAL_PRIVACY",
						"autoRefresh":       "CALENDAR_MONTH",
						"parameters": map[string]any{
							"differentialPrivacy": map[string]any{
								"epsilon":            10,
								"usersNoisePerQuery": 100,
							},
						},
					},
				)
				require.Equal(t, http.StatusOK, createRec.Code)

				listRec := doRequest(
					t,
					e,
					http.MethodGet,
					"/collaborations/"+collabID+"/privacybudgets",
					nil,
				)
				require.Equal(t, http.StatusOK, listRec.Code)

				return listRec.Body.Bytes()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := tt.raw(t)

			var decoded map[string]any
			require.NoError(t, json.Unmarshal(body, &decoded))

			rawItems, ok := decoded[tt.itemsKey]
			require.True(t, ok, "missing items key %q in response: %s", tt.itemsKey, body)
			items, ok := rawItems.([]any)
			require.True(t, ok, "items key %q is not an array: %s", tt.itemsKey, body)
			require.Len(t, items, 1)

			item, ok := items[0].(map[string]any)
			require.True(t, ok, "item is not an object: %v", items[0])

			for _, key := range tt.required {
				assert.Contains(t, item, key, "missing required field %q", key)
			}
			for _, key := range tt.forbidden {
				assert.NotContains(t, item, key, "leaked forbidden field %q", key)
			}
			assert.NotEmpty(
				t,
				item["creatorAccountId"],
				"creatorAccountId must be populated, not just present",
			)
		})
	}
}

// createCleanroomsCollabAndMembership bootstraps a collaboration +
// membership through raw HTTP requests, for tests that need a
// collaboration-scoped fixture without going through the SDK client.
func createCleanroomsCollabAndMembership(t *testing.T, e *echo.Echo) (string, string) {
	t.Helper()

	colRec := doRequest(t, e, http.MethodPost, "/collaborations", map[string]any{
		"name": "wide-collab", "creatorDisplayName": "creator",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	require.Equal(t, http.StatusOK, colRec.Code)
	var colResp map[string]any
	require.NoError(t, json.Unmarshal(colRec.Body.Bytes(), &colResp))
	collabID, _ := colResp["collaboration"].(map[string]any)["id"].(string)
	require.NotEmpty(t, collabID)

	memRec := doRequest(t, e, http.MethodPost, "/memberships", map[string]any{
		"collaborationIdentifier": collabID, "queryLogStatus": "DISABLED",
	})
	require.Equal(t, http.StatusOK, memRec.Code)
	var memResp map[string]any
	require.NoError(t, json.Unmarshal(memRec.Body.Bytes(), &memResp))
	memID, _ := memResp["membership"].(map[string]any)["id"].(string)
	require.NotEmpty(t, memID)

	return collabID, memID
}
