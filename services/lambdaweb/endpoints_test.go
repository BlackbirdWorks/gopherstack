package lambdaweb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func weights(pairs ...any) []types.RevisionWeight {
	var out []types.RevisionWeight

	for i := 0; i < len(pairs); i += 2 {
		out = append(
			out,
			types.RevisionWeight{RevisionId: aws.String(pairs[i].(string)), Weight: aws.Int32(int32(pairs[i+1].(int)))},
		)
	}

	return out
}

func TestCreateWebFunctionEndpoint_Types(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		typ          types.EndpointType
		regions      []string
		wantRegions  []string
		wantGlobal   bool
		wantPerRegDN bool
	}{
		{name: "home", typ: types.EndpointTypeHomeRegion, wantRegions: []string{"us-east-1"}},
		{
			name:        "multi",
			typ:         types.EndpointTypeMultiRegion,
			regions:     []string{"eu-west-1"},
			wantRegions: []string{"us-east-1", "eu-west-1"},
			wantGlobal:  true,
		},
		{
			name:         "per region",
			typ:          types.EndpointTypePerRegion,
			regions:      []string{"eu-west-1", "us-east-1"},
			wantRegions:  []string{"us-east-1", "eu-west-1"},
			wantPerRegDN: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			created := env.createFunction(t, "app")
			env.settle()

			out, err := env.client.CreateWebFunctionEndpoint(t.Context(), &lwsdk.CreateWebFunctionEndpointInput{
				FunctionName: aws.String("app"), EndpointName: aws.String("second"), EndpointType: tc.typ,
				AuthType: types.AuthTypeIamAuth, Regions: tc.regions,
				RevisionWeights: weights(aws.ToString(created.Revision.RevisionId), 100),
			})
			require.NoError(t, err)

			assert.Equal(t, tc.wantRegions, out.Regions)
			assert.Len(t, out.RegionalEndpoints, len(tc.wantRegions))
			assert.Equal(t, tc.wantGlobal, containsStr(aws.ToString(out.DomainName), ".lambda-web.global.on.aws"))

			for r, re := range out.RegionalEndpoints {
				if tc.wantPerRegDN {
					assert.Contains(t, aws.ToString(re.DomainName), ".lambda-web."+r+".on.aws")
				}
			}
		})
	}
}

func containsStr(s, sub string) bool { return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0) }

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}

	return -1
}

func TestCreateWebFunctionEndpoint_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(in *lwsdk.CreateWebFunctionEndpointInput, rev, other string)
		name   string
		code   string
	}{
		{
			name:   "weights required in disabled mode",
			mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) { in.RevisionWeights = nil },
			code:   "ValidationException",
		},
		{
			name:   "sum not 100",
			mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, rev, _ string) { in.RevisionWeights = weights(rev, 50) },
			code:   "ValidationException",
		},
		{name: "three revisions", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, rev, other string) {
			in.RevisionWeights = weights(rev, 40, other, 30, "rev-x", 30)
		}, code: "ValidationException"},
		{name: "duplicate revision", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, rev, _ string) {
			in.RevisionWeights = weights(rev, 50, rev, 50)
		}, code: "ValidationException"},
		{name: "weight zero", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, rev, other string) {
			in.RevisionWeights = weights(rev, 100, other, 0)
		}, code: "ValidationException"},
		{name: "unknown revision", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) {
			in.RevisionWeights = weights("rev-nope", 100)
		}, code: "ResourceNotFoundException"},
		{name: "pending revision", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, other string) {
			in.RevisionWeights = weights(other, 100)
		}, code: "ConflictException"},
		{name: "weights with latest revision", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) {
			in.AutoDeploymentMode = types.AutoDeploymentModeLatestRevision
		}, code: "ValidationException"},
		{name: "latest revision multi region", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) {
			in.AutoDeploymentMode, in.RevisionWeights = types.AutoDeploymentModeLatestRevision, nil
			in.EndpointType, in.Regions = types.EndpointTypeMultiRegion, []string{"eu-west-1"}
		}, code: "ValidationException"},
		{name: "home with other region", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) {
			in.Regions = []string{"eu-west-1"}
		}, code: "ValidationException"},
		{name: "per region home only", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) {
			in.EndpointType, in.Regions = types.EndpointTypePerRegion, []string{"us-east-1"}
		}, code: "ValidationException"},
		{name: "bad region", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) {
			in.EndpointType, in.Regions = types.EndpointTypePerRegion, []string{"mars-1"}
		}, code: "ValidationException"},
		{
			name:   "bad name",
			mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) { in.EndpointName = aws.String("bad_") },
			code:   "ValidationException",
		},
		{
			name:   "duplicate name",
			mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) { in.EndpointName = aws.String("prod") },
			code:   "ConflictException",
		},
		{name: "max environments", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) {
			in.ScalingConfig = &types.ScalingConfig{MaxEnvironments: aws.Int32(1)}
		}, code: "ValidationException"},
		{name: "rate limit step low", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) {
			in.ThrottleConfig = &types.ThrottleConfig{RateLimit: aws.Int32(150)}
		}, code: "ValidationException"},
		{name: "rate limit step high", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) {
			in.ThrottleConfig = &types.ThrottleConfig{RateLimit: aws.Int32(1500)}
		}, code: "ValidationException"},
		{name: "rate limit max", mutate: func(in *lwsdk.CreateWebFunctionEndpointInput, _, _ string) {
			in.ThrottleConfig = &types.ThrottleConfig{RateLimit: aws.Int32(11000)}
		}, code: "ValidationException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			created := env.createFunction(t, "app")
			env.settle()

			other := env.createRevision(t)
			rev := aws.ToString(created.Revision.RevisionId)

			in := &lwsdk.CreateWebFunctionEndpointInput{
				FunctionName: aws.String(
					"app",
				),
				EndpointName:    aws.String("extra"),
				EndpointType:    types.EndpointTypeHomeRegion,
				AuthType:        types.AuthTypeApplicationManaged,
				RevisionWeights: weights(rev, 100),
			}
			tc.mutate(in, rev, aws.ToString(other.RevisionId))

			_, err := env.client.CreateWebFunctionEndpoint(t.Context(), in)
			require.Error(t, err)
			assertAPIErrorCode(t, err, tc.code)
		})
	}
}

func TestCreateWebFunctionEndpoint_Quota(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")
	env.settle()

	for i := range 9 {
		_, err := env.client.CreateWebFunctionEndpoint(t.Context(), &lwsdk.CreateWebFunctionEndpointInput{
			FunctionName: aws.String("app"), EndpointName: aws.String("e" + string(rune('a'+i))),
			EndpointType: types.EndpointTypeHomeRegion, AuthType: types.AuthTypeApplicationManaged,
			RevisionWeights: weights(aws.ToString(created.Revision.RevisionId), 100),
		})
		require.NoError(t, err)
	}

	_, err := env.client.CreateWebFunctionEndpoint(t.Context(), &lwsdk.CreateWebFunctionEndpointInput{
		FunctionName: aws.String(
			"app",
		),
		EndpointName:    aws.String("overflow"),
		EndpointType:    types.EndpointTypeHomeRegion,
		AuthType:        types.AuthTypeApplicationManaged,
		RevisionWeights: weights(aws.ToString(created.Revision.RevisionId), 100),
	})
	assertAPIErrorCode(t, err, "ServiceQuotaExceededException")
}

func TestUpdateWebFunctionEndpoint_Weights(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")
	env.settle()

	rev1 := aws.ToString(created.Revision.RevisionId)
	rev2 := aws.ToString(env.createRevision(t).RevisionId)
	env.settle()

	out, err := env.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"), RevisionWeights: weights(rev1, 90, rev2, 10),
		Description: aws.String("canary"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.EndpointUpdateStatusInProgress, out.UpdateStatus)
	assert.Equal(t, types.EndpointStateActive, out.State, "state stays Active during an update")
	assert.Len(t, out.RevisionWeights, 2)

	_, err = env.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"), RevisionWeights: weights(rev1, 100),
	})
	assertAPIErrorCode(t, err, "ConflictException")

	env.clock.Advance(5 * time.Second)

	got, err := env.client.GetWebFunctionEndpoint(t.Context(), &lwsdk.GetWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.EndpointUpdateStatusSuccessful, got.UpdateStatus)
	assert.Equal(t, "canary", aws.ToString(got.Description))
	assert.Equal(t, int32(90), aws.ToInt32(got.RevisionWeights[0].Weight))

	_, err = env.client.DeleteWebFunctionRevision(t.Context(), &lwsdk.DeleteWebFunctionRevisionInput{
		FunctionName: aws.String("app"), RevisionId: aws.String(rev2),
	})
	assertAPIErrorCode(t, err, "ConflictException")
}

func TestUpdateWebFunctionEndpoint_Modes(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")
	env.settle()

	rev1 := aws.ToString(created.Revision.RevisionId)
	pending := aws.ToString(env.createRevision(t).RevisionId)

	update := func(in lwsdk.UpdateWebFunctionEndpointInput) (*lwsdk.UpdateWebFunctionEndpointOutput, error) {
		in.FunctionName, in.EndpointName = aws.String("app"), aws.String("prod")
		out, err := env.client.UpdateWebFunctionEndpoint(t.Context(), &in)
		env.settle()

		return out, err
	}

	_, err := update(lwsdk.UpdateWebFunctionEndpointInput{AutoDeploymentMode: types.AutoDeploymentModeLatestRevision})
	assertAPIErrorCode(t, err, "ConflictException")

	env.settle()

	out, err := update(lwsdk.UpdateWebFunctionEndpointInput{AutoDeploymentMode: types.AutoDeploymentModeLatestRevision})
	require.NoError(t, err)
	assert.Equal(t, pending, aws.ToString(out.RevisionWeights[0].RevisionId), "latest revision takes all traffic")

	_, err = update(lwsdk.UpdateWebFunctionEndpointInput{RevisionWeights: weights(rev1, 100)})
	assertAPIErrorCode(t, err, "ValidationException")

	out, err = update(lwsdk.UpdateWebFunctionEndpointInput{
		AutoDeploymentMode: types.AutoDeploymentModeDisabled, RevisionWeights: weights(rev1, 100),
	})
	require.NoError(t, err)
	assert.Equal(t, rev1, aws.ToString(out.RevisionWeights[0].RevisionId))

	got, err := env.client.GetWebFunctionEndpoint(t.Context(), &lwsdk.GetWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AutoDeploymentModeDisabled, got.AutoDeploymentMode)
}

func TestUpdateWebFunctionEndpoint_ScalingAndThrottle(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.createFunction(t, "app")
	env.settle()

	set := func(in lwsdk.UpdateWebFunctionEndpointInput) *lwsdk.UpdateWebFunctionEndpointOutput {
		in.FunctionName, in.EndpointName = aws.String("app"), aws.String("prod")
		out, updateErr := env.client.UpdateWebFunctionEndpoint(t.Context(), &in)
		require.NoError(t, updateErr)
		env.settle()

		return out
	}

	out := set(lwsdk.UpdateWebFunctionEndpointInput{
		ScalingConfig: &types.ScalingConfig{
			MaxEnvironments: aws.Int32(50),
		},
		ThrottleConfig: &types.ThrottleConfig{RateLimit: aws.Int32(8000)},
	})
	assert.Equal(t, int32(50), aws.ToInt32(out.ScalingConfig.MaxEnvironments))
	assert.Equal(t, int32(8000), aws.ToInt32(out.ThrottleConfig.RateLimit))

	out = set(lwsdk.UpdateWebFunctionEndpointInput{Description: aws.String("only desc")})
	assert.NotNil(t, out.ScalingConfig, "omitted scalingConfig keeps the value")

	out = set(
		lwsdk.UpdateWebFunctionEndpointInput{
			ScalingConfig:  &types.ScalingConfig{},
			ThrottleConfig: &types.ThrottleConfig{},
		},
	)
	assert.Nil(t, out.ScalingConfig, "an empty object clears scalingConfig")
	assert.Nil(t, out.ThrottleConfig, "an empty object clears throttleConfig")
}

func TestDeleteWebFunctionEndpoint(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.createFunction(t, "app")
	env.settle()

	_, err := env.client.DeleteWebFunctionEndpoint(t.Context(), &lwsdk.DeleteWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
	})
	require.NoError(t, err)

	got, err := env.client.GetWebFunctionEndpoint(t.Context(), &lwsdk.GetWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.EndpointStateDeleting, got.State)

	env.settle()

	_, err = env.client.GetWebFunctionEndpoint(t.Context(), &lwsdk.GetWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
	})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}

func TestListWebFunctionEndpoints(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")
	env.settle()

	_, err := env.client.CreateWebFunctionEndpoint(t.Context(), &lwsdk.CreateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("auto"), EndpointType: types.EndpointTypeHomeRegion,
		AuthType: types.AuthTypeIamAuth, AutoDeploymentMode: types.AutoDeploymentModeLatestRevision,
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{name: "all", want: []string{"auto", "prod"}},
		{
			name:    "mode",
			filters: []types.Filter{{Name: aws.String("autoDeploymentMode"), Values: []string{"LatestRevision"}}},
			want:    []string{"auto"},
		},
		{name: "type", filters: []types.Filter{{Name: aws.String("endpointType"), Values: []string{"MultiRegion"}}}},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"Active"}}},
			want:    []string{"prod"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, listErr := env.client.ListWebFunctionEndpoints(t.Context(), &lwsdk.ListWebFunctionEndpointsInput{
				FunctionName: aws.String("app"), Filters: tc.filters,
			})
			require.NoError(t, listErr)

			var names []string
			for _, e := range out.Endpoints {
				names = append(names, aws.ToString(e.EndpointName))
			}

			assert.Equal(t, tc.want, names)
		})
	}

	got, err := env.client.GetWebFunctionEndpoint(t.Context(), &lwsdk.GetWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("auto"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Revision.RevisionId), aws.ToString(got.RevisionWeights[0].RevisionId))
}
