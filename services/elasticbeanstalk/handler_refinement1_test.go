package elasticbeanstalk_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

// parseXMLResponse strips the XML declaration and unmarshals into v.
func parseXMLResponse(data []byte, v any) error {
	return xml.Unmarshal(data, v)
}

// TestRefinement1_Reset verifies that Reset clears all maps and resets the counter.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateApplication(context.Background(), "app1", "desc", nil)
	require.NoError(t, err)
	_, err = b.CreateEnvironment(
		context.Background(), "app1", "env1", "64bit", "desc", nil, elasticbeanstalk.CreateEnvironmentParams{},
	)
	require.NoError(t, err)
	_, err = b.CreateApplicationVersion(context.Background(), "app1", "v1", "desc", "", "", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, b.ApplicationCount())
	assert.Equal(t, 1, b.EnvironmentCount())
	assert.Equal(t, 1, b.AppVersionCount())

	b.Reset()

	assert.Equal(t, 0, b.ApplicationCount())
	assert.Equal(t, 0, b.EnvironmentCount())
	assert.Equal(t, 0, b.AppVersionCount())
	assert.Equal(t, 0, b.ConfigTemplateCount())
	assert.Equal(t, 0, b.PlatformVersionCount())
}

// TestRefinement1_MultipleResetCycle verifies that multiple resets work correctly.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")

	for range 3 {
		_, err := b.CreateApplication(context.Background(), "app", "desc", nil)
		require.NoError(t, err)
		assert.Equal(t, 1, b.ApplicationCount())
		b.Reset()
		assert.Equal(t, 0, b.ApplicationCount())
	}
}

// TestRefinement1_HandlerReset verifies that Handler.Reset delegates to Backend.Reset.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=app1")
	assert.Equal(t, 1, h.Backend.ApplicationCount())

	h.Reset()

	assert.Equal(t, 0, h.Backend.ApplicationCount())
}

// TestRefinement1_ProviderInit_NilCtx verifies that Provider.Init returns ErrNilAppContext.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &elasticbeanstalk.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, elasticbeanstalk.ErrNilAppContext)
}

// TestRefinement1_HandlerOpsPreBuilt verifies that the handler dispatch table is pre-built.
func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()

	assert.Equal(t, len(ops), h.HandlerOpsLen(), "ops table should match supported operations count")
}

// TestRefinement1_SortedDescribeApplications verifies alphabetic sort order.
func TestRefinement1_SortedDescribeApplications(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"zapp", "aapp", "mapp"} {
		rec := postEBForm(t, h,
			"Version=2010-12-01&Action=CreateApplication&ApplicationName="+name)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeApplications")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	posA := indexOfFirst(body, "aapp")
	posM := indexOfFirst(body, "mapp")
	posZ := indexOfFirst(body, "zapp")

	assert.Less(t, posA, posM, "aapp should come before mapp")
	assert.Less(t, posM, posZ, "mapp should come before zapp")
}

// TestRefinement1_SortedDescribeEnvironments verifies alphabetic sort order.
func TestRefinement1_SortedDescribeEnvironments(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"zoo-env", "alpha-env", "mid-env"} {
		rec := postEBForm(t, h,
			"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName="+name)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEnvironments")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	posA := indexOfFirst(body, "alpha-env")
	posM := indexOfFirst(body, "mid-env")
	posZ := indexOfFirst(body, "zoo-env")

	assert.Less(t, posA, posM, "alpha-env should come before mid-env")
	assert.Less(t, posM, posZ, "mid-env should come before zoo-env")
}

// TestRefinement1_SortedDescribeApplicationVersions verifies alphabetic sort order.
func TestRefinement1_SortedDescribeApplicationVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, label := range []string{"v3", "v1", "v2"} {
		rec := postEBForm(t, h,
			"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=app&VersionLabel="+label)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeApplicationVersions")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	pos1 := indexOfFirst(body, ">v1<")
	pos2 := indexOfFirst(body, ">v2<")
	pos3 := indexOfFirst(body, ">v3<")

	assert.Less(t, pos1, pos2, "v1 should come before v2")
	assert.Less(t, pos2, pos3, "v2 should come before v3")
}

// TestRefinement1_SortedListTagsForResource verifies deterministic tag sort order.
func TestRefinement1_SortedListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=CreateApplication&ApplicationName=tagged-app"+
			"&Tags.member.1.Key=z-key&Tags.member.1.Value=z-val"+
			"&Tags.member.2.Key=a-key&Tags.member.2.Value=a-val"+
			"&Tags.member.3.Key=m-key&Tags.member.3.Value=m-val")
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		CreateApplicationResult struct {
			Application struct {
				ApplicationArn string `xml:"ApplicationArn"`
			} `xml:"Application"`
		} `xml:"CreateApplicationResult"`
	}

	require.NoError(t, parseXMLResponse(rec.Body.Bytes(), &createResp))

	arn := createResp.CreateApplicationResult.Application.ApplicationArn
	require.NotEmpty(t, arn)

	rec = postEBForm(t, h,
		"Version=2010-12-01&Action=ListTagsForResource&ResourceArn="+arn)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	posA := indexOfFirst(body, "a-key")
	posM := indexOfFirst(body, "m-key")
	posZ := indexOfFirst(body, "z-key")

	assert.Less(t, posA, posM, "a-key should come before m-key")
	assert.Less(t, posM, posZ, "m-key should come before z-key")
}

// TestRefinement1_DeleteApplication_Cascade verifies that deleting an application removes
// its environments, versions, and configuration templates.
func TestRefinement1_DeleteApplication_Cascade(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=my-app")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=env1")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=my-app&VersionLabel=v1")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateConfigurationTemplate"+
			"&ApplicationName=my-app&TemplateName=tmpl1")

	assert.Equal(t, 1, h.Backend.EnvironmentCount())
	assert.Equal(t, 1, h.Backend.AppVersionCount())
	assert.Equal(t, 1, h.Backend.ConfigTemplateCount())

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=DeleteApplication&ApplicationName=my-app")
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 0, h.Backend.ApplicationCount())
	assert.Equal(t, 0, h.Backend.EnvironmentCount())
	assert.Equal(t, 0, h.Backend.AppVersionCount())
	assert.Equal(t, 0, h.Backend.ConfigTemplateCount())
}

// TestRefinement1_ErrValidationMapping verifies that ErrValidation maps to 400.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	// CNAMEPrefix missing → ErrInvalidParameter (validation-style)
	h := newTestHandler()
	rec := postEBForm(t, h, "Version=2010-12-01&Action=CheckDNSAvailability")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_SeedHelpers verifies that seed helpers correctly populate the backend.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddApplicationInternal(&elasticbeanstalk.Application{
		ApplicationName: "seeded-app",
		ApplicationARN:  "arn:aws:elasticbeanstalk:us-east-1:123456789012:application/seeded-app",
		Tags:            map[string]string{"env": "test"},
	})

	b.AddEnvironmentInternal(&elasticbeanstalk.Environment{
		ApplicationName: "seeded-app",
		EnvironmentName: "seeded-env",
		EnvironmentID:   "e-00000001",
		EnvironmentARN:  "arn:aws:elasticbeanstalk:us-east-1:123456789012:environment/seeded-app/seeded-env",
		Status:          "Ready",
		Health:          "Green",
		Tier:            "WebServer",
	})

	b.AddAppVersionInternal(&elasticbeanstalk.ApplicationVersion{
		ApplicationName:       "seeded-app",
		VersionLabel:          "seeded-v1",
		ApplicationVersionARN: "arn:aws:elasticbeanstalk:us-east-1:123456789012:applicationversion/seeded-app/seeded-v1",
		Status:                "Processed",
	})

	b.AddConfigTemplateInternal(&elasticbeanstalk.ConfigurationTemplate{
		ApplicationName: "seeded-app",
		TemplateName:    "seeded-tmpl",
	})

	b.AddPlatformVersionInternal(&elasticbeanstalk.PlatformVersion{
		PlatformArn:     "arn:aws:elasticbeanstalk::123456789012:platform/MyPlatform/1.0",
		PlatformName:    "MyPlatform",
		PlatformVersion: "1.0",
		PlatformStatus:  "Ready",
	})

	assert.Equal(t, 1, b.ApplicationCount())
	assert.Equal(t, 1, b.EnvironmentCount())
	assert.Equal(t, 1, b.AppVersionCount())
	assert.Equal(t, 1, b.ConfigTemplateCount())
	assert.Equal(t, 1, b.PlatformVersionCount())
}

// TestRefinement1_ExportCountHelpers verifies export count helpers via HTTP.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	assert.Equal(t, 0, h.Backend.ApplicationCount())

	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=a1")
	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=a2")

	assert.Equal(t, 2, h.Backend.ApplicationCount())

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=a1&EnvironmentName=env1")
	assert.Equal(t, 1, h.Backend.EnvironmentCount())

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=a1&VersionLabel=v1")
	assert.Equal(t, 1, h.Backend.AppVersionCount())

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateConfigurationTemplate&ApplicationName=a1&TemplateName=tmpl1")
	assert.Equal(t, 1, h.Backend.ConfigTemplateCount())

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0")
	assert.Equal(t, 1, h.Backend.PlatformVersionCount())
}

// TestRefinement1_PersistenceRoundTrip verifies snapshot/restore preserves all state.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h, "Version=2010-12-01&Action=CreateApplication&ApplicationName=persisted-app")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=persisted-app&EnvironmentName=persisted-env")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateApplicationVersion&ApplicationName=persisted-app&VersionLabel=v1")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateConfigurationTemplate"+
			"&ApplicationName=persisted-app&TemplateName=tmpl1")
	postEBForm(t, h,
		"Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0")

	snap := h.Backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := elasticbeanstalk.NewHandler(elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1"))
	require.NoError(t, h2.Backend.Restore(t.Context(), snap))

	assert.Equal(t, 1, h2.Backend.ApplicationCount())
	assert.Equal(t, 1, h2.Backend.EnvironmentCount())
	assert.Equal(t, 1, h2.Backend.AppVersionCount())
	assert.Equal(t, 1, h2.Backend.ConfigTemplateCount())
	assert.Equal(t, 1, h2.Backend.PlatformVersionCount())

	// Verify ARN indexes are rebuilt for tag lookup.
	rec := postEBForm(t, h2,
		"Version=2010-12-01&Action=DescribeApplications")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "persisted-app")
}

// TestRefinement1_AssociateOperationsRole_StoresRole verifies role is persisted on environment.
func TestRefinement1_AssociateOperationsRole_StoresRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=my-env")

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=AssociateEnvironmentOperationsRole"+
			"&EnvironmentName=my-env&OperationsRole=arn:aws:iam::123:role/MyRole")
	require.Equal(t, http.StatusOK, rec.Code)

	// Now retrieve the environment and verify it has the role stored.
	envs := h.Backend.DescribeEnvironments(context.Background(), "app", []string{"my-env"}, nil)
	require.Len(t, envs, 1)
	assert.Equal(t, "arn:aws:iam::123:role/MyRole", envs[0].OperationsRole)
}

// TestRefinement1_CheckDNSAvailability_Conflict verifies unavailable CNAME when env exists.
func TestRefinement1_CheckDNSAvailability_Conflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=my-prefix")

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=CheckDNSAvailability&CNAMEPrefix=my-prefix")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Available>false</Available>")
	assert.Contains(t, rec.Body.String(), "my-prefix.us-east-1.elasticbeanstalk.com")
}

// TestRefinement1_CheckDNSAvailability_Available verifies available CNAME when no conflict.
func TestRefinement1_CheckDNSAvailability_Available(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=CheckDNSAvailability&CNAMEPrefix=free-prefix")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Available>true</Available>")
	assert.Contains(t, rec.Body.String(), "free-prefix.us-east-1.elasticbeanstalk.com")
}

// TestRefinement1_UpdateEnvironment_CrossAppLookup verifies UpdateEnvironment without ApplicationName.
func TestRefinement1_UpdateEnvironment_CrossAppLookup(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	postEBForm(t, h,
		"Version=2010-12-01&Action=CreateEnvironment&ApplicationName=my-app&EnvironmentName=my-env")

	// Update without specifying ApplicationName.
	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=UpdateEnvironment&EnvironmentName=my-env&Description=updated-desc")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "UpdateEnvironmentResponse")
}

// TestRefinement1_DescribeConfigurationSettings_EmptyEnvName verifies empty list when both params absent.
func TestRefinement1_DescribeConfigurationSettings_EmptyEnvName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// With no ApplicationName and no EnvironmentName, should return empty list.
	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeConfigurationSettings")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeConfigurationSettingsResponse")
	// Empty ConfigurationSettings list.
	assert.NotContains(t, rec.Body.String(), "<member>")
}

// TestRefinement1_StorageLocationIdempotent verifies CreateStorageLocation is idempotent.
func TestRefinement1_StorageLocationIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec1 := postEBForm(t, h, "Version=2010-12-01&Action=CreateStorageLocation")
	rec2 := postEBForm(t, h, "Version=2010-12-01&Action=CreateStorageLocation")

	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Equal(t, rec1.Body.String(), rec2.Body.String())
	assert.Contains(t, rec1.Body.String(), "elasticbeanstalk-us-east-1-123456789012")
}

// TestRefinement1_GetSupportedOperations_NewOps verifies the 10 new ops are in supported list.
func TestRefinement1_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()

	newOps := []string{
		"AbortEnvironmentUpdate",
		"ApplyEnvironmentManagedAction",
		"AssociateEnvironmentOperationsRole",
		"CheckDNSAvailability",
		"ComposeEnvironments",
		"CreateConfigurationTemplate",
		"CreatePlatformVersion",
		"CreateStorageLocation",
		"DeleteConfigurationTemplate",
		"DeleteEnvironmentConfiguration",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}
}

// TestRefinement1_SeedHelpers_DeepCopy verifies seed helpers perform deep copy of tags.
func TestRefinement1_SeedHelpers_DeepCopy(t *testing.T) {
	t.Parallel()

	b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")

	tags := map[string]string{"key": "original"}
	b.AddApplicationInternal(&elasticbeanstalk.Application{
		ApplicationName: "app",
		ApplicationARN:  "arn:aws:elasticbeanstalk:us-east-1:123456789012:application/app",
		Tags:            tags,
	})

	// Mutate the original tags map.
	tags["key"] = "mutated"

	// The stored application should still have the original value.
	apps := b.DescribeApplications(context.Background(), nil)
	require.Len(t, apps, 1)
	assert.Equal(t, "original", apps[0].Tags["key"])
}

// TestRefinement1_SnapshotNotNil verifies Snapshot returns non-nil on a fresh backend.
func TestRefinement1_SnapshotNotNil(t *testing.T) {
	t.Parallel()

	b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Must be valid JSON.
	var m map[string]any
	require.NoError(t, json.Unmarshal(snap, &m))
}

// TestRefinement1_DeleteConfigurationTemplate_RoundTrip verifies create/delete/verify cycle.
func TestRefinement1_DeleteConfigurationTemplate_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=CreateConfigurationTemplate"+
			"&ApplicationName=app&TemplateName=tmpl1")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, h.Backend.ConfigTemplateCount())

	rec = postEBForm(t, h,
		"Version=2010-12-01&Action=DeleteConfigurationTemplate"+
			"&ApplicationName=app&TemplateName=tmpl1")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.ConfigTemplateCount())

	// Deleting again should return not-found.
	rec = postEBForm(t, h,
		"Version=2010-12-01&Action=DeleteConfigurationTemplate"+
			"&ApplicationName=app&TemplateName=tmpl1")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_PlatformVersion_DuplicateRejected verifies duplicate platform versions fail.
func TestRefinement1_PlatformVersion_DuplicateRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec1 := postEBForm(t, h,
		"Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0")
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := postEBForm(t, h,
		"Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0")
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// indexOfFirst returns the position of the first occurrence of substr in s, or -1.
func indexOfFirst(s, substr string) int {
	return strings.Index(s, substr)
}
