package ocifilestorage_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuotaRuleLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	fs := e.createFS(t, nil)

	_, err := e.client.ToggleQuotaRules(t.Context(), filestorage.ToggleQuotaRulesRequest{
		FileSystemId: fs.Id, AreQuotaRulesEnabled: new(true),
	})
	require.NoError(t, err)
	got := e.getFS(t, fs.Id)
	assert.True(t, *got.AreQuotaRulesEnabled)
	assert.Equal(t, filestorage.FileSystemQuotaEnforcementStateEnabled, got.QuotaEnforcementState)

	resp, err := e.client.CreateQuotaRule(t.Context(), filestorage.CreateQuotaRuleRequest{
		FileSystemId:  fs.Id,
		PrincipalType: filestorage.CreateQuotaRuleDetailsPrincipalTypeIndividualUser, PrincipalId: new(1001),
		IsHardQuota: new(true), QuotaLimitInGigabytes: new(10), DisplayName: new("alice"),
	})
	require.NoError(t, err)
	assert.Equal(t, 1001, *resp.PrincipalId)
	assert.Equal(t, 10, *resp.QuotaLimitInGigabytes)

	upd, err := e.client.UpdateQuotaRule(t.Context(), filestorage.UpdateQuotaRuleRequest{
		FileSystemId: fs.Id, QuotaRuleId: resp.Id, QuotaLimitInGigabytes: new(20),
	})
	require.NoError(t, err)
	assert.Equal(t, 20, *upd.QuotaLimitInGigabytes)
	assert.True(t, upd.TimeUpdated.After(resp.TimeCreated.Time) || upd.TimeUpdated.Equal(resp.TimeCreated.Time))

	list, err := e.client.ListQuotaRules(t.Context(), filestorage.ListQuotaRulesRequest{
		FileSystemId: fs.Id, PrincipalType: filestorage.ListQuotaRulesPrincipalTypeIndividualUser,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, int64(0), *list.Items[0].UsageInBytes)

	none, err := e.client.ListQuotaRules(
		t.Context(),
		filestorage.ListQuotaRulesRequest{
			FileSystemId:  fs.Id,
			PrincipalType: filestorage.ListQuotaRulesPrincipalTypeDefaultGroup,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, none.Items)

	_, err = e.client.DeleteQuotaRule(
		t.Context(),
		filestorage.DeleteQuotaRuleRequest{FileSystemId: fs.Id, QuotaRuleId: resp.Id},
	)
	require.NoError(t, err)

	_, err = e.client.GetQuotaRule(
		t.Context(),
		filestorage.GetQuotaRuleRequest{FileSystemId: fs.Id, QuotaRuleId: resp.Id},
	)
	assertStatus(t, err, http.StatusNotFound, "NotAuthorizedOrNotFound")

	_, err = e.client.ToggleQuotaRules(t.Context(), filestorage.ToggleQuotaRulesRequest{
		FileSystemId: fs.Id, AreQuotaRulesEnabled: new(false),
	})
	require.NoError(t, err)
	assert.Equal(t, filestorage.FileSystemQuotaEnforcementStateDisabled, e.getFS(t, fs.Id).QuotaEnforcementState)
}

func TestQuotaRuleRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		d      filestorage.CreateQuotaRuleDetails
		name   string
		code   string
		status int
	}{
		{
			name: "individual-no-id",
			d: filestorage.CreateQuotaRuleDetails{
				PrincipalType:         "INDIVIDUAL_USER",
				IsHardQuota:           new(true),
				QuotaLimitInGigabytes: new(1),
			},
			status: 400,
			code:   "MissingParameter",
		},
		{
			name: "default-with-id",
			d: filestorage.CreateQuotaRuleDetails{
				PrincipalType:         "DEFAULT_USER",
				PrincipalId:           new(5),
				IsHardQuota:           new(true),
				QuotaLimitInGigabytes: new(1),
			},
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name: "zero-limit",
			d: filestorage.CreateQuotaRuleDetails{
				PrincipalType:         "DEFAULT_USER",
				IsHardQuota:           new(true),
				QuotaLimitInGigabytes: new(0),
			},
			status: 400,
			code:   "InvalidParameter",
		},
		{
			name:   "no-limit",
			d:      filestorage.CreateQuotaRuleDetails{PrincipalType: "DEFAULT_USER", IsHardQuota: new(true)},
			status: 400,
			code:   "MissingParameter",
		},
		{
			name:   "no-hard",
			d:      filestorage.CreateQuotaRuleDetails{PrincipalType: "DEFAULT_USER", QuotaLimitInGigabytes: new(1)},
			status: 400,
			code:   "MissingParameter",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			fs := e.createFS(t, nil)

			_, err := e.client.CreateQuotaRule(
				t.Context(),
				filestorage.CreateQuotaRuleRequest{FileSystemId: fs.Id, CreateQuotaRuleDetails: tc.d},
			)
			assertStatus(t, err, tc.status, tc.code)
		})
	}

	t.Run("duplicate", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		fs := e.createFS(t, nil)
		d := filestorage.CreateQuotaRuleDetails{
			PrincipalType:         "FILE_SYSTEM_LEVEL",
			IsHardQuota:           new(false),
			QuotaLimitInGigabytes: new(5),
		}

		_, err := e.client.CreateQuotaRule(
			t.Context(),
			filestorage.CreateQuotaRuleRequest{FileSystemId: fs.Id, CreateQuotaRuleDetails: d},
		)
		require.NoError(t, err)

		_, err = e.client.CreateQuotaRule(
			t.Context(),
			filestorage.CreateQuotaRuleRequest{FileSystemId: fs.Id, CreateQuotaRuleDetails: d},
		)
		assertStatus(t, err, http.StatusConflict, "Conflict")
	})

	t.Run("other-fs", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		a, b := e.createFS(t, nil), e.createFS(t, nil)

		r, err := e.client.CreateQuotaRule(t.Context(), filestorage.CreateQuotaRuleRequest{
			FileSystemId: a.Id, PrincipalType: "DEFAULT_USER", IsHardQuota: new(true), QuotaLimitInGigabytes: new(1),
		})
		require.NoError(t, err)

		_, err = e.client.GetQuotaRule(
			t.Context(),
			filestorage.GetQuotaRuleRequest{FileSystemId: b.Id, QuotaRuleId: r.Id},
		)
		assertStatus(t, err, http.StatusNotFound, "NotAuthorizedOrNotFound")
	})
}
