package ocifilestorage_test

import (
	"os"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nfsc "github.com/willscott/go-nfs-client/nfs"
)

const quotaUnit = 100

func quotaEnv(t *testing.T, enabled bool) (*env, filestorage.FileSystem, *nfsc.Target) {
	t.Helper()

	e := newNFSEnv(t)
	e.backend.SetQuotaUnit(quotaUnit)

	mt := e.createMT(t, nil)
	fs := e.createFS(t, nil)
	e.createExport(t, mt.ExportSetId, fs.Id, "/data", nil)

	_, err := e.client.ToggleQuotaRules(t.Context(), filestorage.ToggleQuotaRulesRequest{
		FileSystemId: fs.Id, AreQuotaRulesEnabled: new(enabled),
	})
	require.NoError(t, err)

	tg, err := mount(t, e.endpoint(t, mt.Id), "/data")
	require.NoError(t, err)

	return e, fs, tg
}

func (e *env) addQuota(
	t *testing.T, fsID *string, typ filestorage.CreateQuotaRuleDetailsPrincipalTypeEnum, id *int, hard bool,
) {
	t.Helper()

	_, err := e.client.CreateQuotaRule(t.Context(), filestorage.CreateQuotaRuleRequest{
		FileSystemId:  fsID,
		PrincipalType: typ, PrincipalId: id, IsHardQuota: new(hard), QuotaLimitInGigabytes: new(1),
	})
	require.NoError(t, err)
}

func TestQuotaEnforcedOverNFS(t *testing.T) {
	t.Parallel()

	uid := os.Getuid()
	gid := os.Getgid()
	other := uid + 1

	const (
		fsLevel  = filestorage.CreateQuotaRuleDetailsPrincipalTypeFileSystemLevel
		indUser  = filestorage.CreateQuotaRuleDetailsPrincipalTypeIndividualUser
		defUser  = filestorage.CreateQuotaRuleDetailsPrincipalTypeDefaultUser
		indGroup = filestorage.CreateQuotaRuleDetailsPrincipalTypeIndividualGroup
	)

	tests := []struct {
		id      *int
		name    string
		typ     filestorage.CreateQuotaRuleDetailsPrincipalTypeEnum
		enabled bool
		hard    bool
		blocked bool
	}{
		{name: "file system hard", enabled: true, typ: fsLevel, id: nil, hard: true, blocked: true},
		{name: "individual user hard", enabled: true, typ: indUser, id: &uid, hard: true, blocked: true},
		{name: "default user hard", enabled: true, typ: defUser, id: nil, hard: true, blocked: true},
		{name: "individual group hard", enabled: true, typ: indGroup, id: &gid, hard: true, blocked: true},
		{name: "other user hard", enabled: true, typ: indUser, id: &other, hard: true, blocked: false},
		{name: "soft", enabled: true, typ: fsLevel, id: nil, hard: false, blocked: false},
		{name: "disabled", enabled: false, typ: fsLevel, id: nil, hard: true, blocked: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, fs, tg := quotaEnv(t, tt.enabled)
			e.addQuota(t, fs.Id, tt.typ, tt.id, tt.hard)

			writeFile(t, tg, "/fits.txt", "0123456789")

			f, err := tg.OpenFile("/big.txt", 0o644)
			if err != nil {
				_, err = tg.Create("/big.txt", 0o644)
				require.NoError(t, err)

				f, err = tg.OpenFile("/big.txt", 0o644)
				require.NoError(t, err)
			}

			_, err = f.Write(make([]byte, 2*quotaUnit))

			if tt.blocked {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestQuotaUsageAndViolators(t *testing.T) {
	t.Parallel()

	uid := os.Getuid()
	e, fs, tg := quotaEnv(t, true)
	e.addQuota(t, fs.Id, filestorage.CreateQuotaRuleDetailsPrincipalTypeIndividualUser, &uid, false)
	e.addQuota(t, fs.Id, filestorage.CreateQuotaRuleDetailsPrincipalTypeFileSystemLevel, nil, false)

	writeFile(t, tg, "/a.txt", "0123456789")

	list := func(typ filestorage.ListQuotaRulesPrincipalTypeEnum, violators bool) []filestorage.QuotaRuleSummary {
		resp, err := e.client.ListQuotaRules(t.Context(), filestorage.ListQuotaRulesRequest{
			FileSystemId: fs.Id, PrincipalType: typ, AreViolatorsOnly: new(violators),
		})
		require.NoError(t, err)

		return resp.Items
	}

	users := list(filestorage.ListQuotaRulesPrincipalTypeIndividualUser, false)
	require.Len(t, users, 1)
	assert.EqualValues(t, 10, *users[0].UsageInBytes)

	total := list(filestorage.ListQuotaRulesPrincipalTypeFileSystemLevel, false)
	require.Len(t, total, 1)
	assert.EqualValues(t, 10, *total[0].UsageInBytes)

	assert.Empty(t, list(filestorage.ListQuotaRulesPrincipalTypeIndividualUser, true))

	writeFile(t, tg, "/b.txt", string(make([]byte, 2*quotaUnit)))

	assert.Len(t, list(filestorage.ListQuotaRulesPrincipalTypeIndividualUser, true), 1)
	assert.Len(t, list(filestorage.ListQuotaRulesPrincipalTypeFileSystemLevel, true), 1)
}
