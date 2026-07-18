package elasticbeanstalk_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

// TestInMemoryBackend_Reset verifies that Reset clears all maps and resets the counter.
func TestInMemoryBackend_Reset(t *testing.T) {
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

// TestInMemoryBackend_Reset_MultipleCycles verifies that multiple resets work correctly.
func TestInMemoryBackend_Reset_MultipleCycles(t *testing.T) {
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

// TestInMemoryBackend_Snapshot_NotNilOnFreshBackend verifies Snapshot returns
// non-nil, valid JSON on a fresh backend.
func TestInMemoryBackend_Snapshot_NotNilOnFreshBackend(t *testing.T) {
	t.Parallel()

	b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Must be valid JSON.
	var m map[string]any
	require.NoError(t, json.Unmarshal(snap, &m))
}

// TestInMemoryBackend_SeedHelpers_PopulateAllCollections verifies that seed
// helpers correctly populate the backend across every resource collection.
func TestInMemoryBackend_SeedHelpers_PopulateAllCollections(t *testing.T) {
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

// TestInMemoryBackend_SeedHelpers_DeepCopyTags verifies seed helpers perform a
// deep copy of tags, so mutating the caller's map afterward does not corrupt
// stored state.
func TestInMemoryBackend_SeedHelpers_DeepCopyTags(t *testing.T) {
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
