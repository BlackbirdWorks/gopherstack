package appconfig_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	appconfigtypes "github.com/aws/aws-sdk-go-v2/service/appconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTagResource_Realism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags    map[string]string
		name    string
		wantErr string
		missing bool
	}{
		{name: "valid", tags: map[string]string{"a": "b"}},
		{
			name:    "missing_resource",
			tags:    map[string]string{"a": "b"},
			missing: true,
			wantErr: "ResourceNotFoundException",
		},
		{name: "reserved_prefix", tags: map[string]string{"aws:a": "b"}, wantErr: "BadRequestException"},
		{name: "key_too_long", tags: map[string]string{strings.Repeat("k", 129): "b"}, wantErr: "BadRequestException"},
		{
			name:    "value_too_long",
			tags:    map[string]string{"a": strings.Repeat("v", 257)},
			wantErr: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestAppConfigClient(t, newRealClientHandler())
			app, err := c.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{Name: aws.String("app")})
			require.NoError(t, err)

			id := aws.ToString(app.Id)
			if tt.missing {
				id = "nosuchid"
			}

			arn := "arn:aws:appconfig:us-east-1:123456789012:application/" + id
			_, err = c.TagResource(
				t.Context(),
				&appconfigsdk.TagResourceInput{ResourceArn: aws.String(arn), Tags: tt.tags},
			)

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, tt.wantErr)

			_, err = c.ListTagsForResource(
				t.Context(),
				&appconfigsdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
			)
			if tt.missing {
				require.ErrorContains(t, err, "ResourceNotFoundException")
			}
		})
	}
}

func TestTagResource_TooManyTags(t *testing.T) {
	t.Parallel()

	c := newTestAppConfigClient(t, newRealClientHandler())
	app, err := c.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{Name: aws.String("app")})
	require.NoError(t, err)

	tags := make(map[string]string, 51)
	for i := range 51 {
		tags[strings.Repeat("k", i+1)] = "v"
	}

	_, err = c.TagResource(t.Context(), &appconfigsdk.TagResourceInput{
		ResourceArn: aws.String("arn:aws:appconfig:us-east-1:123456789012:application/" + aws.ToString(app.Id)),
		Tags:        tags,
	})
	require.ErrorContains(t, err, "BadRequestException")
}

func TestCreateDeploymentStrategy_Realism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		growthType   appconfigtypes.GrowthType
		replicateTo  appconfigtypes.ReplicateTo
		wantType     string
		duration     int32
		bake         int32
		growthFactor float32
		wantErr      bool
	}{
		{name: "defaults", duration: 10, growthFactor: 50, wantType: "LINEAR"},
		{name: "exponential", duration: 10, growthFactor: 50, growthType: "EXPONENTIAL", wantType: "EXPONENTIAL"},
		{name: "factor_high", duration: 10, growthFactor: 150, wantErr: true},
		{name: "factor_low", duration: 10, growthFactor: 0.5, wantErr: true},
		{name: "duration_high", duration: 1441, growthFactor: 50, wantErr: true},
		{name: "bake_high", duration: 10, bake: 1441, growthFactor: 50, wantErr: true},
		{name: "bad_growth_type", duration: 10, growthFactor: 50, growthType: "BOGUS", wantErr: true},
		{name: "bad_replicate", duration: 10, growthFactor: 50, replicateTo: "BOGUS", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestAppConfigClient(t, newRealClientHandler())
			out, err := c.CreateDeploymentStrategy(t.Context(), &appconfigsdk.CreateDeploymentStrategyInput{
				Name:                        aws.String("strat"),
				DeploymentDurationInMinutes: aws.Int32(tt.duration),
				FinalBakeTimeInMinutes:      tt.bake,
				GrowthFactor:                aws.Float32(tt.growthFactor),
				GrowthType:                  tt.growthType,
				ReplicateTo:                 tt.replicateTo,
			})

			if tt.wantErr {
				require.ErrorContains(t, err, "BadRequestException")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantType, string(out.GrowthType))
			assert.Equal(t, "NONE", string(out.ReplicateTo))
		})
	}
}

func TestCreateConfigurationProfile_LocationURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		uri     string
		wantErr bool
	}{
		{name: "hosted", uri: "hosted"},
		{name: "ssm_parameter", uri: "ssm-parameter://p"},
		{name: "s3", uri: "s3://bucket/key"},
		{name: "secret", uri: "secretsmanager://s"},
		{name: "bogus", uri: "bogus://x", wantErr: true},
		{name: "bare_prefix", uri: "s3://", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestAppConfigClient(t, newRealClientHandler())
			app, err := c.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{Name: aws.String("app")})
			require.NoError(t, err)

			_, err = c.CreateConfigurationProfile(t.Context(), &appconfigsdk.CreateConfigurationProfileInput{
				ApplicationId: app.Id,
				Name:          aws.String("p"),
				LocationUri:   aws.String(tt.uri),
			})

			if tt.wantErr {
				require.ErrorContains(t, err, "BadRequestException")

				return
			}

			require.NoError(t, err)
		})
	}
}
