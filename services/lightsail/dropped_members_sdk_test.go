package lightsail_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateBucket_AccessRulesAndLogConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rules      *lightsailtypes.AccessRules
		logCfg     *lightsailtypes.BucketAccessLogConfig
		name       string
		wantGet    string
		wantPrefix string
		wantOver   bool
		wantLog    bool
		wantErr    bool
	}{
		{name: "public_rules", rules: &lightsailtypes.AccessRules{
			GetObject: lightsailtypes.AccessTypePublic, AllowPublicOverrides: aws.Bool(true),
		}, wantGet: "public", wantOver: true},
		{name: "overrides_only_keeps_private", rules: &lightsailtypes.AccessRules{
			AllowPublicOverrides: aws.Bool(true),
		}, wantGet: "private", wantOver: true},
		{name: "enable_logging", logCfg: &lightsailtypes.BucketAccessLogConfig{
			Enabled: aws.Bool(true), Destination: aws.String("log-dest"), Prefix: aws.String("logs/"),
		}, wantGet: "private", wantLog: true, wantPrefix: "logs/"},
		{name: "enable_without_destination", logCfg: &lightsailtypes.BucketAccessLogConfig{
			Enabled: aws.Bool(true),
		}, wantErr: true},
		{name: "unknown_destination", logCfg: &lightsailtypes.BucketAccessLogConfig{
			Enabled: aws.Bool(true), Destination: aws.String("missing"),
		}, wantErr: true},
		{name: "defaults", wantGet: "private"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t)
			ctx := t.Context()

			for _, n := range []string{"src", "log-dest"} {
				_, err := client.CreateBucket(ctx, &lightsailsdk.CreateBucketInput{
					BucketName: aws.String(n), BundleId: aws.String("small_1_0"),
				})
				require.NoError(t, err)
			}

			if tt.rules != nil || tt.logCfg != nil {
				_, err := client.UpdateBucket(ctx, &lightsailsdk.UpdateBucketInput{
					BucketName: aws.String("src"), AccessRules: tt.rules, AccessLogConfig: tt.logCfg,
				})
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
			}

			got, err := client.GetBuckets(ctx, &lightsailsdk.GetBucketsInput{BucketName: aws.String("src")})
			require.NoError(t, err)
			require.Len(t, got.Buckets, 1)

			bk := got.Buckets[0]
			require.NotNil(t, bk.AccessRules)
			assert.Equal(t, tt.wantGet, string(bk.AccessRules.GetObject))
			assert.Equal(t, tt.wantOver, aws.ToBool(bk.AccessRules.AllowPublicOverrides))

			if !tt.wantLog {
				assert.Nil(t, bk.AccessLogConfig)

				return
			}

			require.NotNil(t, bk.AccessLogConfig)
			assert.True(t, aws.ToBool(bk.AccessLogConfig.Enabled))
			assert.Equal(t, "log-dest", aws.ToString(bk.AccessLogConfig.Destination))
			assert.Equal(t, tt.wantPrefix, aws.ToString(bk.AccessLogConfig.Prefix))
		})
	}
}

func TestGetBuckets_IncludeConnectedResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		include *bool
		name    string
		want    int
	}{
		{name: "default_omits", want: 0},
		{name: "false_omits", include: aws.Bool(false), want: 0},
		{name: "true_includes", include: aws.Bool(true), want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t)
			ctx := t.Context()

			_, err := client.CreateBucket(ctx, &lightsailsdk.CreateBucketInput{
				BucketName: aws.String("b1"), BundleId: aws.String("small_1_0"),
			})
			require.NoError(t, err)

			_, err = client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
				InstanceNames:    []string{"i1"},
				AvailabilityZone: aws.String("us-east-1a"),
				BlueprintId:      aws.String("amazon_linux_2023"),
				BundleId:         aws.String("nano_3_0"),
			})
			require.NoError(t, err)

			_, err = client.SetResourceAccessForBucket(ctx, &lightsailsdk.SetResourceAccessForBucketInput{
				ResourceName: aws.String("i1"), BucketName: aws.String("b1"),
				Access: lightsailtypes.ResourceBucketAccessAllow,
			})
			require.NoError(t, err)

			got, err := client.GetBuckets(ctx, &lightsailsdk.GetBucketsInput{IncludeConnectedResources: tt.include})
			require.NoError(t, err)
			require.Len(t, got.Buckets, 1)
			assert.Len(t, got.Buckets[0].ResourcesReceivingAccess, tt.want)
		})
	}
}

func TestGetBuckets_PageToken(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	const total = 130

	for i := range total {
		_, err := client.CreateBucket(ctx, &lightsailsdk.CreateBucketInput{
			BucketName: aws.String(fmt.Sprintf("bucket-%03d", i)), BundleId: aws.String("small_1_0"),
		})
		require.NoError(t, err)
	}

	seen := map[string]bool{}
	pages := 0

	var token *string

	for {
		out, err := client.GetBuckets(ctx, &lightsailsdk.GetBucketsInput{PageToken: token})
		require.NoError(t, err)

		pages++

		for _, bk := range out.Buckets {
			assert.False(t, seen[aws.ToString(bk.Name)], "duplicate bucket across pages")
			seen[aws.ToString(bk.Name)] = true
		}

		if out.NextPageToken == nil {
			break
		}

		token = out.NextPageToken
	}

	assert.Len(t, seen, total)
	assert.Equal(t, 2, pages)

	active, err := client.GetActiveNames(ctx, &lightsailsdk.GetActiveNamesInput{})
	require.NoError(t, err)
	require.NotNil(t, active.NextPageToken)
	assert.Len(t, active.ActiveNames, 100)

	next, err := client.GetActiveNames(ctx, &lightsailsdk.GetActiveNamesInput{PageToken: active.NextPageToken})
	require.NoError(t, err)
	assert.Len(t, next.ActiveNames, total-100)
	assert.Nil(t, next.NextPageToken)
}

func TestGetRelationalDatabaseBlueprints_PageTokenValidated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token   *string
		name    string
		wantErr bool
	}{
		{name: "no_token"},
		{name: "garbage_token", token: aws.String("%%%not-a-token"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := newTestClient(t).GetRelationalDatabaseBlueprints(t.Context(),
				&lightsailsdk.GetRelationalDatabaseBlueprintsInput{PageToken: tt.token})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out.Blueprints)
		})
	}
}
