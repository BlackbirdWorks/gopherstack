package dms_test

import (
	"maps"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

// migrationProjectDeps creates an instance profile and two data providers
// via raw HTTP requests and returns the identifiers CreateMigrationProject's
// required InstanceProfileIdentifier/Source+TargetDataProviderDescriptors
// need to resolve against real state.
func migrationProjectDeps(t *testing.T, h *dms.Handler) map[string]any {
	t.Helper()

	ipRec := doDMS(t, h, "CreateInstanceProfile", map[string]any{"InstanceProfileName": "mp-dep-instance-profile"})
	require.Equal(t, http.StatusOK, ipRec.Code)
	instanceProfile := parseJSON(t, ipRec)["InstanceProfile"].(map[string]any)["InstanceProfileName"].(string)

	srcRec := doDMS(t, h, "CreateDataProvider", map[string]any{
		"DataProviderName": "mp-dep-source-provider",
		"Engine":           "mysql",
	})
	require.Equal(t, http.StatusOK, srcRec.Code)
	sourceProvider := parseJSON(t, srcRec)["DataProvider"].(map[string]any)["DataProviderName"].(string)

	tgtRec := doDMS(t, h, "CreateDataProvider", map[string]any{
		"DataProviderName": "mp-dep-target-provider",
		"Engine":           "mysql",
	})
	require.Equal(t, http.StatusOK, tgtRec.Code)
	targetProvider := parseJSON(t, tgtRec)["DataProvider"].(map[string]any)["DataProviderName"].(string)

	return map[string]any{
		"InstanceProfileIdentifier": instanceProfile,
		"SourceDataProviderDescriptors": []map[string]any{
			{"DataProviderIdentifier": sourceProvider},
		},
		"TargetDataProviderDescriptors": []map[string]any{
			{"DataProviderIdentifier": targetProvider},
		},
	}
}

func TestMigrationProjectLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	deps := migrationProjectDeps(t, h)

	createBody := map[string]any{"MigrationProjectName": "proj-1"}
	maps.Copy(createBody, deps)

	createRec := doDMS(t, h, "CreateMigrationProject", createBody)
	require.Equal(t, http.StatusOK, createRec.Code)
	projArn := parseJSON(t, createRec)["MigrationProject"].(map[string]any)["MigrationProjectArn"].(string)

	// Duplicate.
	dupRec := doDMS(t, h, "CreateMigrationProject", createBody)
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Describe.
	descRec := doDMS(t, h, "DescribeMigrationProjects", map[string]any{})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Modify by ARN.
	modRec := doDMS(t, h, "ModifyMigrationProject", map[string]any{
		"MigrationProjectIdentifier": projArn,
		"Description":                "updated",
	})
	assert.Equal(t, http.StatusOK, modRec.Code)

	// Modify not found.
	notFoundRec := doDMS(t, h, "ModifyMigrationProject", map[string]any{
		"MigrationProjectIdentifier": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)

	// Delete.
	delRec := doDMS(t, h, "DeleteMigrationProject", map[string]any{
		"MigrationProjectIdentifier": projArn,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	delRec2 := doDMS(t, h, "DeleteMigrationProject", map[string]any{
		"MigrationProjectIdentifier": projArn,
	})
	assert.Equal(t, http.StatusNotFound, delRec2.Code)
}

func TestModifyMigrationProject_UpdatesDescription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		lookupByArn bool
	}{
		{name: "lookup_by_name", lookupByArn: false},
		{name: "lookup_by_arn", lookupByArn: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			deps := migrationProjectDeps(t, h)

			createBody := map[string]any{
				"MigrationProjectName": "mp-modify",
				"Description":          "original",
			}
			maps.Copy(createBody, deps)

			createRec := doDMS(t, h, "CreateMigrationProject", createBody)
			require.Equal(t, http.StatusOK, createRec.Code)
			mp := parseJSON(t, createRec)["MigrationProject"].(map[string]any)
			mpName := mp["MigrationProjectName"].(string)
			mpArn := mp["MigrationProjectArn"].(string)

			lookupKey := mpName
			if tt.lookupByArn {
				lookupKey = mpArn
			}

			modRec := doDMS(t, h, "ModifyMigrationProject", map[string]any{
				"MigrationProjectIdentifier": lookupKey,
				"Description":                "updated description",
			})
			require.Equal(t, http.StatusOK, modRec.Code)

			updated := parseJSON(t, modRec)["MigrationProject"].(map[string]any)
			assert.Equal(t, "updated description", updated["Description"],
				"ModifyMigrationProject must persist the updated description")
		})
	}
}

// TestCreateMigrationProject_RequiredMembers locks in
// InstanceProfileIdentifier/SourceDataProviderDescriptors/
// TargetDataProviderDescriptors (databasemigrationservice@v1.66.4
// api_op_CreateMigrationProject.go:39-52, all "This member is required")
// being rejected when absent, and resource references being validated
// against real state rather than accepted as opaque strings.
func TestCreateMigrationProject_RequiredMembers(t *testing.T) {
	t.Parallel()

	t.Run("missing instanceprofileidentifier rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		deps := migrationProjectDeps(t, h)
		body := map[string]any{
			"MigrationProjectName":          "no-ip",
			"SourceDataProviderDescriptors": deps["SourceDataProviderDescriptors"],
			"TargetDataProviderDescriptors": deps["TargetDataProviderDescriptors"],
		}

		rec := doDMS(t, h, "CreateMigrationProject", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing sourcedataproviderdescriptors rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		deps := migrationProjectDeps(t, h)
		body := map[string]any{
			"MigrationProjectName":          "no-src",
			"InstanceProfileIdentifier":     deps["InstanceProfileIdentifier"],
			"TargetDataProviderDescriptors": deps["TargetDataProviderDescriptors"],
		}

		rec := doDMS(t, h, "CreateMigrationProject", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing targetdataproviderdescriptors rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		deps := migrationProjectDeps(t, h)
		body := map[string]any{
			"MigrationProjectName":          "no-tgt",
			"InstanceProfileIdentifier":     deps["InstanceProfileIdentifier"],
			"SourceDataProviderDescriptors": deps["SourceDataProviderDescriptors"],
		}

		rec := doDMS(t, h, "CreateMigrationProject", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("unknown instance profile rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		deps := migrationProjectDeps(t, h)
		body := map[string]any{
			"MigrationProjectName":          "bad-ip",
			"InstanceProfileIdentifier":     "nonexistent-instance-profile",
			"SourceDataProviderDescriptors": deps["SourceDataProviderDescriptors"],
			"TargetDataProviderDescriptors": deps["TargetDataProviderDescriptors"],
		}

		rec := doDMS(t, h, "CreateMigrationProject", body)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("unknown data provider rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		deps := migrationProjectDeps(t, h)
		body := map[string]any{
			"MigrationProjectName":      "bad-dp",
			"InstanceProfileIdentifier": deps["InstanceProfileIdentifier"],
			"SourceDataProviderDescriptors": []map[string]any{
				{"DataProviderIdentifier": "nonexistent-provider"},
			},
			"TargetDataProviderDescriptors": deps["TargetDataProviderDescriptors"],
		}

		rec := doDMS(t, h, "CreateMigrationProject", body)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// TestSDKRoundTrip_CreateMigrationProject_EchoesRequiredMembers drives
// CreateMigrationProject through the real aws-sdk-go-v2 client and proves
// InstanceProfileArn/InstanceProfileName and the resolved Source/
// TargetDataProviderDescriptors (both required on the request, both present
// on the real MigrationProject response type -- types.go:2044-2088) are
// supplied on create and observable on DescribeMigrationProjects, not just a
// 2xx.
func TestSDKRoundTrip_CreateMigrationProject_EchoesRequiredMembers(t *testing.T) {
	t.Parallel()

	backend := dms.NewInMemoryBackend(tagsRTAccountID, tagsRTRegion)
	h := dms.NewHandler(backend)
	client := newTestDMSClient(t, h)

	ip, err := client.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
		InstanceProfileName: aws.String("rt-instance-profile"),
	})
	require.NoError(t, err)

	mysqlSettings := &types.DataProviderSettingsMemberMySqlSettings{Value: types.MySqlDataProviderSettings{}}

	src, err := client.CreateDataProvider(t.Context(), &dmssdk.CreateDataProviderInput{
		DataProviderName: aws.String("rt-source-provider"),
		Engine:           aws.String("mysql"),
		Settings:         mysqlSettings,
	})
	require.NoError(t, err)

	tgt, err := client.CreateDataProvider(t.Context(), &dmssdk.CreateDataProviderInput{
		DataProviderName: aws.String("rt-target-provider"),
		Engine:           aws.String("mysql"),
		Settings:         mysqlSettings,
	})
	require.NoError(t, err)

	created, err := client.CreateMigrationProject(t.Context(), &dmssdk.CreateMigrationProjectInput{
		MigrationProjectName:      aws.String("rt-migration-project"),
		InstanceProfileIdentifier: ip.InstanceProfile.InstanceProfileName,
		SourceDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
			{DataProviderIdentifier: src.DataProvider.DataProviderName},
		},
		TargetDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
			{DataProviderIdentifier: tgt.DataProvider.DataProviderName},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.MigrationProject)

	assertMigrationProjectEchoesDeps(t, created.MigrationProject, ip, src, tgt)

	described, err := client.DescribeMigrationProjects(t.Context(), &dmssdk.DescribeMigrationProjectsInput{})
	require.NoError(t, err)
	require.Len(t, described.MigrationProjects, 1)
	assertMigrationProjectEchoesDeps(t, &described.MigrationProjects[0], ip, src, tgt)
}

func assertMigrationProjectEchoesDeps(
	t *testing.T,
	mp *types.MigrationProject,
	ip *dmssdk.CreateInstanceProfileOutput,
	src, tgt *dmssdk.CreateDataProviderOutput,
) {
	t.Helper()

	require.NotNil(t, mp.InstanceProfileArn)
	assert.Equal(t, aws.ToString(ip.InstanceProfile.InstanceProfileArn), aws.ToString(mp.InstanceProfileArn))
	assert.Equal(t, aws.ToString(ip.InstanceProfile.InstanceProfileName), aws.ToString(mp.InstanceProfileName))

	require.Len(t, mp.SourceDataProviderDescriptors, 1)
	assert.Equal(t,
		aws.ToString(src.DataProvider.DataProviderArn),
		aws.ToString(mp.SourceDataProviderDescriptors[0].DataProviderArn),
	)

	require.Len(t, mp.TargetDataProviderDescriptors, 1)
	assert.Equal(t,
		aws.ToString(tgt.DataProvider.DataProviderArn),
		aws.ToString(mp.TargetDataProviderDescriptors[0].DataProviderArn),
	)
}
