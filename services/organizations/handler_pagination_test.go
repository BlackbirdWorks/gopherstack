package organizations_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/organizations"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

func TestHandler_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nextToken  string
		setupCount int
		maxResults int
		wantCount  int
		wantNext   bool
	}{
		{
			name:       "no_pagination_all_items",
			nextToken:  "",
			setupCount: 5,
			maxResults: 0, // Should default to defaultMaxResults
			wantCount:  6, // 5 created + 1 master account
			wantNext:   false,
		},
		{
			name:       "paginate_first_page",
			setupCount: 5,
			maxResults: 2,
			wantCount:  2,
			wantNext:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := organizations.NewHandler(b)

			_, _, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			// Setup data
			for i := range tt.setupCount {
				_, errCreate := b.CreateAccount(
					fmt.Sprintf("Test Account %d", i),
					fmt.Sprintf("test%d@example.com", i),
					"ROLE",
					"ALLOW",
					nil,
				)
				require.NoError(t, errCreate)
			}

			// Perform request
			e := echo.New()
			reqBody := map[string]any{
				"MaxResults": tt.maxResults,
			}
			if tt.nextToken != "" {
				reqBody["NextToken"] = tt.nextToken
			}
			bodyBytes, _ := json.Marshal(reqBody)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			req.Header.Set("X-Amz-Target", "AWSOrganizationsV20161128.ListAccounts")
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			// Inject logger
			ctx := logger.WithService(req.Context(), "organizations")
			c.SetRequest(req.WithContext(ctx))

			// Route using echo directly or via handler. Since we have to map the route,
			// let's just call the Echo handler directly.
			// The handler registers routes, so we can pass it through h.Handler()(c)
			err = h.Handler()(c)
			require.NoError(t, err)

			require.Equal(t, http.StatusOK, rec.Code)

			var res map[string]any
			err = json.Unmarshal(rec.Body.Bytes(), &res)
			require.NoError(t, err)

			accounts, ok := res["Accounts"].([]any)
			require.True(t, ok)
			require.Len(t, accounts, tt.wantCount)

			_, hasNext := res["NextToken"]
			require.Equal(t, tt.wantNext, hasNext)
		})
	}
}

// TestListOps_HonorMaxResultsAndNextToken exercises MaxResults truncation and
// NextToken continuation for list operations whose request/response wire shapes
// already declared NextToken/MaxResults support but whose handlers previously
// ignored both, silently returning the full unpaginated result set. Real AWS
// truncates every one of these operations at MaxResults (default 100 when
// unset) and returns a NextToken when more results remain.
func TestListOps_HonorMaxResultsAndNextToken(t *testing.T) {
	t.Parallel()

	t.Run("ListAccountsForParent", func(t *testing.T) {
		t.Parallel()

		h, rootID := newHandlerWithOrg(t)

		for i := range 5 {
			rec := doRequest(t, h, "CreateAccount", map[string]any{
				"AccountName": "acct",
				"Email":       accountsForParentEmail(i),
			})
			require.Equal(t, http.StatusOK, rec.Code)
		}

		rec := doRequest(t, h, "ListAccountsForParent", map[string]any{
			"ParentId":   rootID,
			"MaxResults": 2,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			NextToken string           `json:"NextToken"`
			Accounts  []map[string]any `json:"Accounts"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

		require.Len(t, resp.Accounts, 2, "MaxResults=2 must truncate to 2 items")
		require.NotEmpty(t, resp.NextToken, "truncated page must carry a NextToken")

		rec2 := doRequest(t, h, "ListAccountsForParent", map[string]any{
			"ParentId":   rootID,
			"MaxResults": 2,
			"NextToken":  resp.NextToken,
		})
		require.Equal(t, http.StatusOK, rec2.Code)

		var resp2 struct {
			NextToken string           `json:"NextToken"`
			Accounts  []map[string]any `json:"Accounts"`
		}
		require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp2))
		require.Len(t, resp2.Accounts, 2, "second page must continue from the token")
	})

	t.Run("ListPoliciesForTarget", func(t *testing.T) {
		t.Parallel()

		h, rootID := newHandlerWithOrg(t)

		for i := range 3 {
			rec := doRequest(t, h, "CreatePolicy", map[string]any{
				"Name":        policyName(i),
				"Type":        "SERVICE_CONTROL_POLICY",
				"Content":     `{"Version":"2012-10-17","Statement":[]}`,
				"Description": "d",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var pResp struct {
				Policy struct {
					PolicySummary struct {
						ID string `json:"Id"`
					} `json:"PolicySummary"`
				} `json:"Policy"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&pResp))

			rec = doRequest(t, h, "AttachPolicy", map[string]any{
				"PolicyId": pResp.Policy.PolicySummary.ID,
				"TargetId": rootID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
		}

		rec := doRequest(t, h, "ListPoliciesForTarget", map[string]any{
			"TargetId":   rootID,
			"Filter":     "SERVICE_CONTROL_POLICY",
			"MaxResults": 1,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			NextToken string           `json:"NextToken"`
			Policies  []map[string]any `json:"Policies"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Len(t, resp.Policies, 1)
		require.NotEmpty(t, resp.NextToken)
	})

	t.Run("ListTargetsForPolicy", func(t *testing.T) {
		t.Parallel()

		h, rootID := newHandlerWithOrg(t)

		rec := doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
			"ParentId": rootID,
			"Name":     "ou1",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var ouResp struct {
			OrganizationalUnit struct {
				ID string `json:"Id"`
			} `json:"OrganizationalUnit"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&ouResp))

		rec = doRequest(t, h, "CreatePolicy", map[string]any{
			"Name":        "p",
			"Type":        "SERVICE_CONTROL_POLICY",
			"Content":     `{"Version":"2012-10-17","Statement":[]}`,
			"Description": "d",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var pResp struct {
			Policy struct {
				PolicySummary struct {
					ID string `json:"Id"`
				} `json:"PolicySummary"`
			} `json:"Policy"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&pResp))
		policyID := pResp.Policy.PolicySummary.ID

		for _, targetID := range []string{rootID, ouResp.OrganizationalUnit.ID} {
			rec = doRequest(t, h, "AttachPolicy", map[string]any{
				"PolicyId": policyID,
				"TargetId": targetID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
		}

		rec = doRequest(t, h, "ListTargetsForPolicy", map[string]any{
			"PolicyId":   policyID,
			"MaxResults": 1,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			NextToken string           `json:"NextToken"`
			Targets   []map[string]any `json:"Targets"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Len(t, resp.Targets, 1)
		require.NotEmpty(t, resp.NextToken)
	})

	t.Run("ListDelegatedAdministrators", func(t *testing.T) {
		t.Parallel()

		h, _ := newHandlerWithOrg(t)

		rec := doRequest(t, h, "EnableAWSServiceAccess", map[string]any{"ServicePrincipal": "config.amazonaws.com"})
		require.Equal(t, http.StatusOK, rec.Code)

		for i := range 2 {
			caRec := doRequest(t, h, "CreateAccount", map[string]any{
				"AccountName": "acct",
				"Email":       delegatedAdminEmail(i),
			})
			require.Equal(t, http.StatusOK, caRec.Code)

			var caResp struct {
				CreateAccountStatus struct {
					AccountID string `json:"AccountId"`
				} `json:"CreateAccountStatus"`
			}
			require.NoError(t, json.NewDecoder(caRec.Body).Decode(&caResp))

			rec = doRequest(t, h, "RegisterDelegatedAdministrator", map[string]any{
				"AccountId":        caResp.CreateAccountStatus.AccountID,
				"ServicePrincipal": "config.amazonaws.com",
			})
			require.Equal(t, http.StatusOK, rec.Code)
		}

		rec = doRequest(t, h, "ListDelegatedAdministrators", map[string]any{
			"MaxResults": 1,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			NextToken               string           `json:"NextToken"`
			DelegatedAdministrators []map[string]any `json:"DelegatedAdministrators"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Len(t, resp.DelegatedAdministrators, 1)
		require.NotEmpty(t, resp.NextToken)
	})

	t.Run("ListCreateAccountStatus", func(t *testing.T) {
		t.Parallel()

		h, _ := newHandlerWithOrg(t)

		for i := range 3 {
			rec := doRequest(t, h, "CreateAccount", map[string]any{
				"AccountName": "acct",
				"Email":       createStatusEmail(i),
			})
			require.Equal(t, http.StatusOK, rec.Code)
		}

		rec := doRequest(t, h, "ListCreateAccountStatus", map[string]any{
			"MaxResults": 2,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			NextToken             string           `json:"NextToken"`
			CreateAccountStatuses []map[string]any `json:"CreateAccountStatuses"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Len(t, resp.CreateAccountStatuses, 2)
		require.NotEmpty(t, resp.NextToken)
	})

	t.Run("ListTagsForResource", func(t *testing.T) {
		t.Parallel()

		h, rootID := newHandlerWithOrg(t)

		tags := make([]map[string]any, 0, 3)
		for i := range 3 {
			tags = append(tags, map[string]any{"Key": tagKey(i), "Value": "v"})
		}

		rec := doRequest(t, h, "TagResource", map[string]any{
			"ResourceId": rootID,
			"Tags":       tags,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		rec = doRequest(t, h, "ListTagsForResource", map[string]any{
			"ResourceId": rootID,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			NextToken string           `json:"NextToken"`
			Tags      []map[string]any `json:"Tags"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Len(t, resp.Tags, 3, "all 3 tags fit under the default page size")
		require.Empty(t, resp.NextToken)
	})

	t.Run("ListAWSServiceAccessForOrganization", func(t *testing.T) {
		t.Parallel()

		h, _ := newHandlerWithOrg(t)

		for _, sp := range []string{"config.amazonaws.com", "cloudtrail.amazonaws.com"} {
			rec := doRequest(t, h, "EnableAWSServiceAccess", map[string]any{"ServicePrincipal": sp})
			require.Equal(t, http.StatusOK, rec.Code)
		}

		rec := doRequest(t, h, "ListAWSServiceAccessForOrganization", map[string]any{
			"MaxResults": 1,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			NextToken                string           `json:"NextToken"`
			EnabledServicePrincipals []map[string]any `json:"EnabledServicePrincipals"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Len(t, resp.EnabledServicePrincipals, 1)
		require.NotEmpty(t, resp.NextToken)

		// A nil/empty body (as legacy callers may send) must still work: the
		// request has no required fields, matching every other no-arg list op.
		recEmpty := doRequest(t, h, "ListAWSServiceAccessForOrganization", nil)
		require.Equal(t, http.StatusOK, recEmpty.Code)
	})

	t.Run("ListDelegatedServicesForAccount", func(t *testing.T) {
		t.Parallel()

		h, _ := newHandlerWithOrg(t)

		rec := doRequest(t, h, "CreateAccount", map[string]any{
			"AccountName": "acct",
			"Email":       "delegated-services@example.com",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var caResp struct {
			CreateAccountStatus struct {
				AccountID string `json:"AccountId"`
			} `json:"CreateAccountStatus"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&caResp))
		accountID := caResp.CreateAccountStatus.AccountID

		for _, sp := range []string{"config.amazonaws.com", "cloudtrail.amazonaws.com"} {
			rec = doRequest(t, h, "EnableAWSServiceAccess", map[string]any{"ServicePrincipal": sp})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "RegisterDelegatedAdministrator", map[string]any{
				"AccountId":        accountID,
				"ServicePrincipal": sp,
			})
			require.Equal(t, http.StatusOK, rec.Code)
		}

		rec = doRequest(t, h, "ListDelegatedServicesForAccount", map[string]any{
			"AccountId":  accountID,
			"MaxResults": 1,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			NextToken         string           `json:"NextToken"`
			DelegatedServices []map[string]any `json:"DelegatedServices"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Len(t, resp.DelegatedServices, 1)
		require.NotEmpty(t, resp.NextToken)
	})
}

func accountsForParentEmail(i int) string {
	return "parent-acct-" + string(rune('a'+i)) + "@example.com"
}

func policyName(i int) string {
	return "policy-" + string(rune('a'+i))
}

func delegatedAdminEmail(i int) string {
	return "delegated-admin-" + string(rune('a'+i)) + "@example.com"
}

func createStatusEmail(i int) string {
	return "create-status-" + string(rune('a'+i)) + "@example.com"
}

func tagKey(i int) string {
	return "key-" + string(rune('a'+i))
}
