package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Availability Configurations ----

func TestAvailabilityConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		providerType string
		provider     string
	}{
		{
			name:         "EWS provider",
			providerType: "EWS",
			provider:     `"EwsProvider":{"EwsEndpoint":"https://ews.example.com","EwsUsername":"user","EwsPassword":"pass"}`,
		},
		{
			name:         "Lambda provider",
			providerType: "LAMBDA",
			provider:     `"LambdaProvider":{"LambdaArn":"arn:aws:lambda:us-east-1:000000000000:function:avail"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			orgID := createTestOrg(t, h, "avail-test-org")

			// Create
			rec := doOp(t, h, "CreateAvailabilityConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q,"DomainName":"example.com",%s}`, orgID, tc.provider,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			// List
			rec = doOp(t, h, "ListAvailabilityConfigurations", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m := decodeJSON(t, rec)
			cfgs, ok := m["AvailabilityConfigurations"].([]any)
			require.True(t, ok)
			require.Len(t, cfgs, 1)
			cfg := cfgs[0].(map[string]any)
			assert.Equal(t, "example.com", cfg["DomainName"])
			assert.Equal(t, tc.providerType, cfg["ProviderType"])

			// Test
			rec = doOp(t, h, "TestAvailabilityConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q,"DomainName":"example.com"}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m = decodeJSON(t, rec)
			assert.Equal(t, true, m["TestPassed"])

			// Update
			updateProvider := `"LambdaProvider":{"LambdaArn":"arn:aws:lambda:us-east-1:000000000000:function:updated"}`
			if tc.providerType == "LAMBDA" {
				updateProvider = `"EwsProvider":{"EwsEndpoint":"https://ews2.example.com","EwsUsername":"user2","EwsPassword":"pass2"}` //nolint:lll // existing issue.
			}
			rec = doOp(t, h, "UpdateAvailabilityConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q,"DomainName":"example.com",%s}`, orgID, updateProvider,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify update
			rec = doOp(t, h, "ListAvailabilityConfigurations", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			m = decodeJSON(t, rec)
			cfgs = m["AvailabilityConfigurations"].([]any)
			updated := cfgs[0].(map[string]any)
			if tc.providerType == "LAMBDA" {
				assert.Equal(t, "EWS", updated["ProviderType"])
			} else {
				assert.Equal(t, "LAMBDA", updated["ProviderType"])
			}

			// Delete
			rec = doOp(t, h, "DeleteAvailabilityConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q,"DomainName":"example.com"}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify gone
			rec = doOp(t, h, "ListAvailabilityConfigurations", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			m = decodeJSON(t, rec)
			cfgs = m["AvailabilityConfigurations"].([]any)
			assert.Empty(t, cfgs)
		})
	}
}

func TestAvailabilityConfigurationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		body      string
		wantError string
	}{
		{
			name:      "create duplicate",
			action:    "sequence",
			wantError: "EntityAlreadyExistsException",
		},
		{
			name:      "delete nonexistent",
			action:    "DeleteAvailabilityConfiguration",
			body:      `{"OrganizationId":"org-123456789012","DomainName":"nope.com"}`,
			wantError: "EntityNotFoundException",
		},
		{
			name:      "update nonexistent",
			action:    "UpdateAvailabilityConfiguration",
			body:      `{"OrganizationId":"org-123456789012","DomainName":"nope.com","LambdaProvider":{"LambdaArn":"arn:aws:lambda:us-east-1:000:function:f"}}`, //nolint:lll // existing issue.
			wantError: "EntityNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tc.name == "create duplicate" {
				orgID := createTestOrg(t, h, "dup-avail-org")
				body := fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"example.com","LambdaProvider":{"LambdaArn":"arn:x"}}`, orgID,
				)
				doOp(t, h, "CreateAvailabilityConfiguration", body)
				rec := doOp(t, h, "CreateAvailabilityConfiguration", body)
				require.Equal(t, http.StatusBadRequest, rec.Code)
				m := decodeJSON(t, rec)
				assert.Contains(t, m["__type"], tc.wantError)

				return
			}

			rec := doOp(t, h, tc.action, tc.body)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			m := decodeJSON(t, rec)
			assert.Contains(t, m["__type"], tc.wantError)
		})
	}
}
