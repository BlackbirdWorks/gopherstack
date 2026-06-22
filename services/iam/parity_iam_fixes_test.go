package iam_test

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestParityIAM_ErrorSentinelDistinctness verifies that each "not found" sentinel
// has a unique error message, enabling message-based inspection to determine which
// resource type was missing. All sentinels share the "NoSuchEntity" prefix (matching
// the AWS error code) but carry a distinct resource-type suffix.
func TestParityIAM_ErrorSentinelDistinctness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sentinel error
		name     string
		wantMsg  string
	}{
		{
			name:     "user_not_found",
			sentinel: iam.ErrUserNotFound,
			wantMsg:  "NoSuchEntity: user",
		},
		{
			name:     "role_not_found",
			sentinel: iam.ErrRoleNotFound,
			wantMsg:  "NoSuchEntity: role",
		},
		{
			name:     "policy_not_found",
			sentinel: iam.ErrPolicyNotFound,
			wantMsg:  "NoSuchEntity: policy",
		},
		{
			name:     "group_not_found",
			sentinel: iam.ErrGroupNotFound,
			wantMsg:  "NoSuchEntity: group",
		},
		{
			name:     "access_key_not_found",
			sentinel: iam.ErrAccessKeyNotFound,
			wantMsg:  "NoSuchEntity: access key",
		},
		{
			name:     "instance_profile_not_found",
			sentinel: iam.ErrInstanceProfileNotFound,
			wantMsg:  "NoSuchEntity: instance profile",
		},
		{
			name:     "inline_policy_not_found",
			sentinel: iam.ErrInlinePolicyNotFound,
			wantMsg:  "NoSuchEntity: inline policy",
		},
		{
			name:     "saml_provider_not_found",
			sentinel: iam.ErrSAMLProviderNotFound,
			wantMsg:  "NoSuchEntity: SAML provider",
		},
		{
			name:     "oidc_provider_not_found",
			sentinel: iam.ErrOIDCProviderNotFound,
			wantMsg:  "NoSuchEntity: OIDC provider",
		},
		{
			name:     "login_profile_not_found",
			sentinel: iam.ErrLoginProfileNotFound,
			wantMsg:  "NoSuchEntity: login profile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.wantMsg, tt.sentinel.Error(),
				"sentinel message must be distinct for message-based inspection")
		})
	}
}

// TestParityIAM_ErrorSentinelUniqueness verifies that each "not found" sentinel
// is a distinct Go error value (pointer inequality), so errors.Is can distinguish them.
func TestParityIAM_ErrorSentinelUniqueness(t *testing.T) {
	t.Parallel()

	type namedErr struct {
		err  error
		name string
	}

	sentinels := []namedErr{
		{name: "user", err: iam.ErrUserNotFound},
		{name: "role", err: iam.ErrRoleNotFound},
		{name: "policy", err: iam.ErrPolicyNotFound},
		{name: "group", err: iam.ErrGroupNotFound},
		{name: "access_key", err: iam.ErrAccessKeyNotFound},
		{name: "instance_profile", err: iam.ErrInstanceProfileNotFound},
		{name: "inline_policy", err: iam.ErrInlinePolicyNotFound},
		{name: "saml_provider", err: iam.ErrSAMLProviderNotFound},
		{name: "oidc_provider", err: iam.ErrOIDCProviderNotFound},
		{name: "login_profile", err: iam.ErrLoginProfileNotFound},
	}

	for i, a := range sentinels {
		for j, b := range sentinels {
			if i == j {
				continue
			}

			t.Run(fmt.Sprintf("%s_ne_%s", a.name, b.name), func(t *testing.T) {
				t.Parallel()
				assert.NotErrorIs(t, a.err, b.err,
					"sentinels %q and %q must not match via errors.Is", a.name, b.name)
			})
		}
	}
}

// TestParityIAM_ErrorSentinelWrapping verifies that backend errors wrap the
// correct sentinel so callers can use errors.Is to detect which resource type
// was missing — without relying on message text.
func TestParityIAM_ErrorSentinelWrapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		triggerErr  func(b *iam.InMemoryBackend) error
		sentinel    error
		notSentinel error
		name        string
	}{
		{
			name:        "get_user_wraps_ErrUserNotFound",
			sentinel:    iam.ErrUserNotFound,
			notSentinel: iam.ErrRoleNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.GetUser("ghost")

				return err
			},
		},
		{
			name:        "get_role_wraps_ErrRoleNotFound",
			sentinel:    iam.ErrRoleNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.GetRole("ghost")

				return err
			},
		},
		{
			name:        "get_policy_wraps_ErrPolicyNotFound",
			sentinel:    iam.ErrPolicyNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.GetPolicy("arn:aws:iam::123456789012:policy/ghost")

				return err
			},
		},
		{
			name:        "get_group_wraps_ErrGroupNotFound",
			sentinel:    iam.ErrGroupNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.GetGroup("ghost")

				return err
			},
		},
		{
			name:        "delete_access_key_wraps_ErrAccessKeyNotFound",
			sentinel:    iam.ErrAccessKeyNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.CreateUser("u1", "/", "")
				require.NoError(t, err)

				return b.DeleteAccessKey("u1", "AKIAXXXXXXXXXXXXXXXX")
			},
		},
		{
			name:        "delete_instance_profile_wraps_ErrInstanceProfileNotFound",
			sentinel:    iam.ErrInstanceProfileNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				return b.DeleteInstanceProfile("ghost")
			},
		},
		{
			name:        "get_user_policy_wraps_ErrInlinePolicyNotFound",
			sentinel:    iam.ErrInlinePolicyNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.CreateUser("u2", "/", "")
				require.NoError(t, err)
				_, err = b.GetUserPolicy("u2", "ghost-policy")

				return err
			},
		},
		{
			name:        "get_login_profile_wraps_ErrLoginProfileNotFound",
			sentinel:    iam.ErrLoginProfileNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.CreateUser("u3", "/", "")
				require.NoError(t, err)
				_, err = b.GetLoginProfile("u3")

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := iam.NewInMemoryBackend()
			err := tt.triggerErr(b)
			require.Error(t, err, "expected an error")
			require.ErrorIs(t, err, tt.sentinel,
				"error %q must wrap sentinel %q", err, tt.sentinel)
			assert.NotErrorIs(t, err, tt.notSentinel,
				"error %q must NOT wrap sentinel %q", err, tt.notSentinel)
		})
	}
}

// TestParityIAM_HandlerNoSuchEntityCode verifies that all "not found" errors
// translate to the "NoSuchEntity" XML error code in HTTP responses, matching AWS.
// IAM uses HTTP 400 for NoSuchEntity (not 404 — IAM is not REST-style).
func TestParityIAM_HandlerNoSuchEntityCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params map[string]string
		name   string
		action string
	}{
		{
			name:   "get_user_not_found",
			action: "GetUser",
			params: map[string]string{"UserName": "ghost"},
		},
		{
			name:   "get_role_not_found",
			action: "GetRole",
			params: map[string]string{"RoleName": "ghost"},
		},
		{
			name:   "get_policy_not_found",
			action: "GetPolicy",
			params: map[string]string{"PolicyArn": "arn:aws:iam::123456789012:policy/ghost"},
		},
		{
			name:   "get_group_not_found",
			action: "GetGroup",
			params: map[string]string{"GroupName": "ghost"},
		},
		{
			name:   "get_instance_profile_not_found",
			action: "GetInstanceProfile",
			params: map[string]string{"InstanceProfileName": "ghost"},
		},
		{
			name:   "get_login_profile_not_found",
			action: "GetLoginProfile",
			params: map[string]string{"UserName": "ghost"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t)
			e := echo.New()
			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, rec.Code,
				"action %s must return 400 for NoSuchEntity", tt.action)

			var errResp iam.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "NoSuchEntity", errResp.Error.Code,
				"action %s must return NoSuchEntity error code", tt.action)
		})
	}
}

// TestParityIAM_SimulatePrincipalPolicy verifies that SimulatePrincipalPolicy
// evaluates actual attached policies rather than returning canned results.
func TestParityIAM_SimulatePrincipalPolicy(t *testing.T) {
	t.Parallel()

	const allowS3GetPolicy = `{
		"Version":"2012-10-17",
		"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]
	}`
	const denyS3Policy = `{
		"Version":"2012-10-17",
		"Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]
	}`

	tests := []struct {
		setup         func(b *iam.InMemoryBackend, userArn *string)
		wantDecisions map[string]string // action → decision
		name          string
		actions       []string
		resources     []string
	}{
		{
			name: "no_policies_all_implicit_deny",
			setup: func(b *iam.InMemoryBackend, userArn *string) {
				u, err := b.CreateUser("alice", "/", "")
				require.NoError(t, err)
				*userArn = u.Arn
			},
			actions:   []string{"s3:GetObject", "ec2:DescribeInstances"},
			resources: []string{"*"},
			wantDecisions: map[string]string{
				"s3:GetObject":          "implicitDeny",
				"ec2:DescribeInstances": "implicitDeny",
			},
		},
		{
			name: "attached_allow_policy_grants_access",
			setup: func(b *iam.InMemoryBackend, userArn *string) {
				u, err := b.CreateUser("bob", "/", "")
				require.NoError(t, err)
				*userArn = u.Arn

				pol, err := b.CreatePolicy("AllowS3", "/", allowS3GetPolicy)
				require.NoError(t, err)

				require.NoError(t, b.AttachUserPolicy("bob", pol.Arn))
			},
			actions:   []string{"s3:GetObject", "ec2:DescribeInstances"},
			resources: []string{"*"},
			wantDecisions: map[string]string{
				"s3:GetObject":          "allowed",
				"ec2:DescribeInstances": "implicitDeny",
			},
		},
		{
			name: "inline_allow_policy_grants_access",
			setup: func(b *iam.InMemoryBackend, userArn *string) {
				u, err := b.CreateUser("carol", "/", "")
				require.NoError(t, err)
				*userArn = u.Arn

				require.NoError(t, b.PutUserPolicy("carol", "S3Access", allowS3GetPolicy))
			},
			actions:   []string{"s3:GetObject"},
			resources: []string{"*"},
			wantDecisions: map[string]string{
				"s3:GetObject": "allowed",
			},
		},
		{
			name: "explicit_deny_overrides_allow",
			setup: func(b *iam.InMemoryBackend, userArn *string) {
				u, err := b.CreateUser("dave", "/", "")
				require.NoError(t, err)
				*userArn = u.Arn

				allowPol, err := b.CreatePolicy("AllowS3b", "/", allowS3GetPolicy)
				require.NoError(t, err)

				denyPol, err := b.CreatePolicy("DenyS3", "/", denyS3Policy)
				require.NoError(t, err)

				require.NoError(t, b.AttachUserPolicy("dave", allowPol.Arn))
				require.NoError(t, b.AttachUserPolicy("dave", denyPol.Arn))
			},
			actions:   []string{"s3:GetObject"},
			resources: []string{"*"},
			wantDecisions: map[string]string{
				"s3:GetObject": "explicitDeny",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := iam.NewInMemoryBackend()
			var principalArn string
			tt.setup(b, &principalArn)

			results, err := b.SimulatePrincipalPolicy(principalArn, tt.actions, tt.resources)
			require.NoError(t, err)
			require.Len(t, results, len(tt.actions)*len(tt.resources))

			for _, r := range results {
				wantDecision, ok := tt.wantDecisions[r.ActionName]
				if !ok {
					continue
				}

				assert.Equal(t, wantDecision, r.Decision,
					"action %q on resource %q: want decision %q, got %q",
					r.ActionName, r.ResourceName, wantDecision, r.Decision)
			}
		})
	}
}

// TestParityIAM_CredentialReportColumns verifies that GetCredentialReport returns
// a base64-encoded CSV containing all required AWS credential report columns.
func TestParityIAM_CredentialReportColumns(t *testing.T) {
	t.Parallel()

	requiredColumns := []string{
		"user",
		"arn",
		"user_creation_time",
		"password_enabled",
		"password_last_used",
		"password_last_changed",
		"password_next_rotation",
		"mfa_active",
		"access_key_1_active",
		"access_key_1_last_rotated",
		"access_key_1_last_used_date",
		"access_key_1_last_used_region",
		"access_key_1_last_used_service",
		"access_key_2_active",
		"access_key_2_last_rotated",
		"access_key_2_last_used_date",
		"access_key_2_last_used_region",
		"access_key_2_last_used_service",
		"cert_1_active",
		"cert_1_last_rotated",
		"cert_2_active",
		"cert_2_last_rotated",
	}

	tests := []struct {
		setup func(b *iam.InMemoryBackend)
		name  string
	}{
		{
			name:  "empty_account",
			setup: func(_ *iam.InMemoryBackend) {},
		},
		{
			name: "account_with_user",
			setup: func(b *iam.InMemoryBackend) {
				_, err := b.CreateUser("alice", "/", "")
				require.NoError(t, err)
			},
		},
		{
			name: "user_with_access_key",
			setup: func(b *iam.InMemoryBackend) {
				_, err := b.CreateUser("bob", "/", "")
				require.NoError(t, err)
				_, err = b.CreateAccessKey("bob")
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := iam.NewInMemoryBackend()
			tt.setup(b)

			h := iam.NewHandler(b)
			e := echo.New()

			req := iamRequest("GenerateCredentialReport", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, http.StatusOK, rec.Code)

			req = iamRequest("GetCredentialReport", nil)
			rec = httptest.NewRecorder()
			c = e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"GetCredentialReportResponse"`
				Result  struct {
					Content      string `xml:"Content"`
					ReportFormat string `xml:"ReportFormat"`
				} `xml:"GetCredentialReportResult"`
			}

			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "text/csv", resp.Result.ReportFormat)
			assert.NotEmpty(t, resp.Result.Content, "Content must not be empty")

			decoded, err := base64.StdEncoding.DecodeString(resp.Result.Content)
			require.NoError(t, err, "Content must be valid base64")

			headerLine := strings.Split(string(decoded), "\n")[0]
			cols := strings.Split(headerLine, ",")

			assert.Len(t, cols, len(requiredColumns),
				"credential report must have exactly %d columns", len(requiredColumns))

			for i, want := range requiredColumns {
				if i < len(cols) {
					assert.Equal(t, want, cols[i],
						"column %d must be %q, got %q", i, want, cols[i])
				}
			}
		})
	}
}

// TestParityIAM_ListPaginationSortedOrder verifies that List operations return
// results in lexicographic (sorted) order and correctly paginate using the
// returned marker token.
func TestParityIAM_ListPaginationSortedOrder(t *testing.T) {
	t.Parallel()

	type listFunc func(b *iam.InMemoryBackend, marker string, pageLimit int) (names []string, next string, err error)

	tests := []struct {
		listFn    listFunc
		name      string
		itemNames []string // names to create, must include more items than pageSize
		pageSize  int
	}{
		{
			name:      "list_users_sorted_paginated",
			pageSize:  3,
			itemNames: []string{"zara", "alice", "mike", "bob", "carol", "dave"},
			listFn: func(b *iam.InMemoryBackend, marker string, pageLimit int) ([]string, string, error) {
				pg, err := b.ListUsers(marker, pageLimit)
				if err != nil {
					return nil, "", err
				}

				names := make([]string, len(pg.Data))
				for i, u := range pg.Data {
					names[i] = u.UserName
				}

				return names, pg.Next, nil
			},
		},
		{
			name:      "list_roles_sorted_paginated",
			pageSize:  2,
			itemNames: []string{"zeta-role", "alpha-role", "beta-role", "gamma-role", "delta-role"},
			listFn: func(b *iam.InMemoryBackend, marker string, pageLimit int) ([]string, string, error) {
				pg, err := b.ListRoles(marker, pageLimit)
				if err != nil {
					return nil, "", err
				}

				names := make([]string, len(pg.Data))
				for i, r := range pg.Data {
					names[i] = r.RoleName
				}

				return names, pg.Next, nil
			},
		},
		{
			name:      "list_groups_sorted_paginated",
			pageSize:  2,
			itemNames: []string{"ops", "dev", "qa", "sre", "mgmt"},
			listFn: func(b *iam.InMemoryBackend, marker string, pageLimit int) ([]string, string, error) {
				pg, err := b.ListGroups(marker, pageLimit)
				if err != nil {
					return nil, "", err
				}

				names := make([]string, len(pg.Data))
				for i, g := range pg.Data {
					names[i] = g.GroupName
				}

				return names, pg.Next, nil
			},
		},
		{
			name:      "list_policies_sorted_paginated",
			pageSize:  3,
			itemNames: []string{"ZPolicy", "APolicy", "MPolicy", "BPolicy", "CPolicy", "DPolicy"},
			listFn: func(b *iam.InMemoryBackend, marker string, pageLimit int) ([]string, string, error) {
				pg, err := b.ListPolicies(marker, pageLimit)
				if err != nil {
					return nil, "", err
				}

				names := make([]string, len(pg.Data))
				for i, p := range pg.Data {
					names[i] = p.PolicyName
				}

				return names, pg.Next, nil
			},
		},
	}

	const validTrustPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	const validPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"*"}]}`

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := iam.NewInMemoryBackend()

			// Create resources in the order given (intentionally unsorted).
			for _, name := range tt.itemNames {
				switch tt.name {
				case "list_users_sorted_paginated":
					_, err := b.CreateUser(name, "/", "")
					require.NoError(t, err)
				case "list_roles_sorted_paginated":
					_, err := b.CreateRole(name, "/", validTrustPolicy, "")
					require.NoError(t, err)
				case "list_groups_sorted_paginated":
					_, err := b.CreateGroup(name, "/")
					require.NoError(t, err)
				case "list_policies_sorted_paginated":
					_, err := b.CreatePolicy(name, "/", validPolicy)
					require.NoError(t, err)
				}
			}

			// Collect all items by paginating.
			var allNames []string
			marker := ""

			for {
				names, next, err := tt.listFn(b, marker, tt.pageSize)
				require.NoError(t, err)

				if len(names) == 0 && next == "" {
					break
				}

				// Each page must have at most pageSize items.
				assert.LessOrEqual(t, len(names), tt.pageSize,
					"page must not exceed pageSize=%d", tt.pageSize)

				allNames = append(allNames, names...)

				if next == "" {
					break
				}

				marker = next
			}

			assert.Len(t, allNames, len(tt.itemNames),
				"paginated result must contain all %d items", len(tt.itemNames))

			// Verify lexicographic sort order across all pages.
			for i := 1; i < len(allNames); i++ {
				assert.Less(t, allNames[i-1], allNames[i],
					"items must be in sorted order: allNames[%d]=%q must be < allNames[%d]=%q",
					i-1, allNames[i-1], i, allNames[i])
			}
		})
	}
}

// TestParityIAM_SortedIndexMaintainedAfterDelete verifies that sorted name indexes
// are correctly updated when resources are deleted, and that subsequent List
// operations return the correct remaining items in sorted order.
func TestParityIAM_SortedIndexMaintainedAfterDelete(t *testing.T) {
	t.Parallel()

	type deleteTestCase struct {
		create     []string
		toDelete   []string
		name       string
		wantRemain []string
	}

	tests := []deleteTestCase{
		{
			name:       "user_delete_updates_index",
			create:     []string{"zara", "alice", "bob", "carol"},
			toDelete:   []string{"alice", "carol"},
			wantRemain: []string{"bob", "zara"},
		},
		{
			name:       "group_delete_updates_index",
			create:     []string{"ops", "dev", "qa"},
			toDelete:   []string{"dev"},
			wantRemain: []string{"ops", "qa"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := iam.NewInMemoryBackend()
			isUser := strings.Contains(tt.name, "user")

			for _, name := range tt.create {
				if isUser {
					_, err := b.CreateUser(name, "/", "")
					require.NoError(t, err)
				} else {
					_, err := b.CreateGroup(name, "/")
					require.NoError(t, err)
				}
			}

			for _, name := range tt.toDelete {
				if isUser {
					require.NoError(t, b.DeleteUser(name))
				} else {
					require.NoError(t, b.DeleteGroup(name))
				}
			}

			// List all remaining items.
			var gotNames []string

			if isUser {
				pg, err := b.ListUsers("", 100)
				require.NoError(t, err)

				for _, u := range pg.Data {
					gotNames = append(gotNames, u.UserName)
				}
			} else {
				pg, err := b.ListGroups("", 100)
				require.NoError(t, err)

				for _, g := range pg.Data {
					gotNames = append(gotNames, g.GroupName)
				}
			}

			assert.Equal(t, tt.wantRemain, gotNames,
				"remaining items after delete must match expected sorted list")
		})
	}
}
