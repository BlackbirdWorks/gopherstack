package bedrockagentcorecontrol_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func containerArtifact(uri string) types.AgentRuntimeArtifact {
	return &types.AgentRuntimeArtifactMemberContainerConfiguration{
		Value: types.ContainerConfiguration{ContainerUri: aws.String(uri)},
	}
}

func createRuntime(t *testing.T, c *sdk.Client, name string) *sdk.CreateAgentRuntimeOutput {
	t.Helper()

	out, err := c.CreateAgentRuntime(t.Context(), &sdk.CreateAgentRuntimeInput{
		AgentRuntimeName:     aws.String(name),
		AgentRuntimeArtifact: containerArtifact(testImage),
		RoleArn:              aws.String(testRole),
		NetworkConfiguration: &types.NetworkConfiguration{NetworkMode: types.NetworkModePublic},
		Tags:                 map[string]string{"team": "a"},
	})
	require.NoError(t, err)

	return out
}

func TestRuntimeLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetLifecycleDelay(time.Minute)

	ctx := t.Context()
	created := createRuntime(t, f.client, "my_agent")
	assert.Equal(t, types.AgentRuntimeStatusCreating, created.Status)
	assert.Equal(t, "1", aws.ToString(created.AgentRuntimeVersion))
	assert.Contains(t, aws.ToString(created.AgentRuntimeId), "my_agent-")
	assert.Contains(t, aws.ToString(created.AgentRuntimeArn), ":runtime/"+aws.ToString(created.AgentRuntimeId))
	require.NotNil(t, created.WorkloadIdentityDetails)

	id := created.AgentRuntimeId

	got, err := f.client.GetAgentRuntime(ctx, &sdk.GetAgentRuntimeInput{AgentRuntimeId: id})
	require.NoError(t, err)
	assert.Equal(t, types.AgentRuntimeStatusCreating, got.Status)

	_, err = f.client.UpdateAgentRuntime(ctx, &sdk.UpdateAgentRuntimeInput{
		AgentRuntimeId: id, AgentRuntimeArtifact: containerArtifact(testImage), RoleArn: aws.String(testRole),
	})
	assert.Equal(t, "ConflictException", apiCode(err), "a CREATING runtime cannot be updated")

	f.clock.advance()

	got, err = f.client.GetAgentRuntime(ctx, &sdk.GetAgentRuntimeInput{AgentRuntimeId: id})
	require.NoError(t, err)
	assert.Equal(t, types.AgentRuntimeStatusReady, got.Status)
	assert.Equal(t, "my_agent", aws.ToString(got.AgentRuntimeName))
	assert.WithinDuration(t, f.clock.t.Add(-time.Minute), aws.ToTime(got.CreatedAt), time.Second)

	_, err = f.client.CreateAgentRuntime(ctx, &sdk.CreateAgentRuntimeInput{
		AgentRuntimeName: aws.String("my_agent"), AgentRuntimeArtifact: containerArtifact(testImage),
		RoleArn: aws.String(testRole),
	})
	assert.Equal(t, "ConflictException", apiCode(err))

	upd, err := f.client.UpdateAgentRuntime(ctx, &sdk.UpdateAgentRuntimeInput{
		AgentRuntimeId: id, AgentRuntimeArtifact: containerArtifact(testImage), RoleArn: aws.String(testRole),
		Description: aws.String("v2"),
	})
	require.NoError(t, err)
	assert.Equal(t, "2", aws.ToString(upd.AgentRuntimeVersion))
	assert.Equal(t, types.AgentRuntimeStatusUpdating, upd.Status)

	f.clock.advance()

	v1, err := f.client.GetAgentRuntime(
		ctx,
		&sdk.GetAgentRuntimeInput{AgentRuntimeId: id, AgentRuntimeVersion: aws.String("1")},
	)
	require.NoError(t, err)
	assert.Nil(t, v1.Description)
	assert.Equal(t, "1", aws.ToString(v1.AgentRuntimeVersion))

	versions, err := f.client.ListAgentRuntimeVersions(ctx, &sdk.ListAgentRuntimeVersionsInput{AgentRuntimeId: id})
	require.NoError(t, err)
	require.Len(t, versions.AgentRuntimes, 2)
	assert.Equal(t, "2", aws.ToString(versions.AgentRuntimes[1].AgentRuntimeVersion))

	list, err := f.client.ListAgentRuntimes(ctx, &sdk.ListAgentRuntimesInput{})
	require.NoError(t, err)
	require.Len(t, list.AgentRuntimes, 1)
	assert.Equal(t, "2", aws.ToString(list.AgentRuntimes[0].AgentRuntimeVersion))

	del, err := f.client.DeleteAgentRuntime(ctx, &sdk.DeleteAgentRuntimeInput{AgentRuntimeId: id})
	require.NoError(t, err)
	assert.Equal(t, types.AgentRuntimeStatusDeleting, del.Status)

	f.clock.advance()

	_, err = f.client.GetAgentRuntime(ctx, &sdk.GetAgentRuntimeInput{AgentRuntimeId: id})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
	assert.Equal(t, 0, f.backend.ResourceCount(), "runtime, default endpoint and workload identity are all gone")
}

func TestRuntimeEndpoints(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	rt := createRuntime(t, f.client, "ep_agent")

	eps, err := f.client.ListAgentRuntimeEndpoints(
		ctx,
		&sdk.ListAgentRuntimeEndpointsInput{AgentRuntimeId: rt.AgentRuntimeId},
	)
	require.NoError(t, err)
	require.Len(t, eps.RuntimeEndpoints, 1)
	assert.Equal(t, "DEFAULT", aws.ToString(eps.RuntimeEndpoints[0].Name))

	_, err = f.client.UpdateAgentRuntime(ctx, &sdk.UpdateAgentRuntimeInput{
		AgentRuntimeId:       rt.AgentRuntimeId,
		AgentRuntimeArtifact: containerArtifact(testImage),
		RoleArn:              aws.String(testRole),
	})
	require.NoError(t, err)

	created, err := f.client.CreateAgentRuntimeEndpoint(ctx, &sdk.CreateAgentRuntimeEndpointInput{
		AgentRuntimeId: rt.AgentRuntimeId, Name: aws.String("prod"), AgentRuntimeVersion: aws.String("1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "prod", aws.ToString(created.EndpointName))
	assert.Equal(t, "1", aws.ToString(created.TargetVersion))

	ep, err := f.client.GetAgentRuntimeEndpoint(ctx, &sdk.GetAgentRuntimeEndpointInput{
		AgentRuntimeId: rt.AgentRuntimeId, EndpointName: aws.String("prod"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AgentRuntimeEndpointStatusReady, ep.Status)
	assert.Equal(t, "1", aws.ToString(ep.LiveVersion))

	upd, err := f.client.UpdateAgentRuntimeEndpoint(ctx, &sdk.UpdateAgentRuntimeEndpointInput{
		AgentRuntimeId: rt.AgentRuntimeId, EndpointName: aws.String("prod"), AgentRuntimeVersion: aws.String("2"),
	})
	require.NoError(t, err)
	assert.Equal(t, "1", aws.ToString(upd.LiveVersion), "live version moves once the update settles")
	assert.Equal(t, "2", aws.ToString(upd.TargetVersion))

	_, err = f.client.UpdateAgentRuntimeEndpoint(ctx, &sdk.UpdateAgentRuntimeEndpointInput{
		AgentRuntimeId: rt.AgentRuntimeId, EndpointName: aws.String("prod"), AgentRuntimeVersion: aws.String("9"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteAgentRuntime(ctx, &sdk.DeleteAgentRuntimeInput{AgentRuntimeId: rt.AgentRuntimeId})
	assert.Equal(t, "ConflictException", apiCode(err), "a custom endpoint blocks runtime deletion")

	_, err = f.client.DeleteAgentRuntimeEndpoint(ctx, &sdk.DeleteAgentRuntimeEndpointInput{
		AgentRuntimeId: rt.AgentRuntimeId, EndpointName: aws.String("prod"),
	})
	require.NoError(t, err)

	_, err = f.client.DeleteAgentRuntime(
		ctx,
		&sdk.DeleteAgentRuntimeInput{AgentRuntimeId: rt.AgentRuntimeId, AgentRuntimeVersion: aws.String("1")},
	)
	require.NoError(t, err, "deleting an old version only drops that version")

	got, err := f.client.GetAgentRuntime(ctx, &sdk.GetAgentRuntimeInput{AgentRuntimeId: rt.AgentRuntimeId})
	require.NoError(t, err)
	assert.Equal(t, "2", aws.ToString(got.AgentRuntimeVersion))

	_, err = f.client.CreateAgentRuntimeEndpoint(ctx, &sdk.CreateAgentRuntimeEndpointInput{
		AgentRuntimeId: rt.AgentRuntimeId, Name: aws.String("DEFAULT"),
	})
	assert.Equal(t, "ConflictException", apiCode(err))
}

func TestRuntimeValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetReferences(fakeRefs{repos: map[string]bool{"agent": true}})

	tests := []struct {
		name     string
		mutate   func(in *sdk.CreateAgentRuntimeInput)
		wantCode string
	}{
		{"ok", func(*sdk.CreateAgentRuntimeInput) {}, ""},
		{
			"bad name",
			func(in *sdk.CreateAgentRuntimeInput) { in.AgentRuntimeName = aws.String("1-bad name") },
			"ValidationException",
		},
		{
			"bad uri",
			func(in *sdk.CreateAgentRuntimeInput) { in.AgentRuntimeArtifact = containerArtifact("docker.io/x") },
			"ValidationException",
		},
		{"missing repo", func(in *sdk.CreateAgentRuntimeInput) {
			in.AgentRuntimeArtifact = containerArtifact(testAccount + ".dkr.ecr.us-east-1.amazonaws.com/other:1")
		}, "ValidationException"},
		{"bad role", func(in *sdk.CreateAgentRuntimeInput) { in.RoleArn = aws.String("role") }, "ValidationException"},
		{"vpc without config", func(in *sdk.CreateAgentRuntimeInput) {
			in.NetworkConfiguration = &types.NetworkConfiguration{NetworkMode: types.NetworkModeVpc}
		}, "ValidationException"},
		{"bad jwt", func(in *sdk.CreateAgentRuntimeInput) {
			in.AuthorizerConfiguration = &types.AuthorizerConfigurationMemberCustomJWTAuthorizer{
				Value: types.CustomJWTAuthorizerConfiguration{DiscoveryUrl: aws.String("http://x")},
			}
		}, "ValidationException"},
		{"unknown capacity provider", func(in *sdk.CreateAgentRuntimeInput) {
			in.CapacityProviderConfiguration = &types.CapacityProviderConfiguration{
				CapacityProviderArn: aws.String(
					"arn:aws:bedrock-agentcore:us-east-1:" + testAccount + ":capacity-provider/x",
				),
			}
		}, "ResourceNotFoundException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.CreateAgentRuntimeInput{
				AgentRuntimeName:     aws.String("agent"),
				AgentRuntimeArtifact: containerArtifact(testImage),
				RoleArn:              aws.String(testRole),
			}
			tt.mutate(in)

			_, err := f.client.CreateAgentRuntime(t.Context(), in)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}
}

func TestRuntimeTagsAndPolicy(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	rt := createRuntime(t, f.client, "tag_agent")

	tags, err := f.client.ListTagsForResource(ctx, &sdk.ListTagsForResourceInput{ResourceArn: rt.AgentRuntimeArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "a"}, tags.Tags)

	_, err = f.client.TagResource(
		ctx,
		&sdk.TagResourceInput{ResourceArn: rt.AgentRuntimeArn, Tags: map[string]string{"env": "dev"}},
	)
	require.NoError(t, err)
	_, err = f.client.UntagResource(
		ctx,
		&sdk.UntagResourceInput{ResourceArn: rt.AgentRuntimeArn, TagKeys: []string{"team"}},
	)
	require.NoError(t, err)

	tags, err = f.client.ListTagsForResource(ctx, &sdk.ListTagsForResourceInput{ResourceArn: rt.AgentRuntimeArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "dev"}, tags.Tags)

	_, err = f.client.TagResource(
		ctx,
		&sdk.TagResourceInput{ResourceArn: rt.AgentRuntimeArn, Tags: map[string]string{"aws:x": "y"}},
	)
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = f.client.GetResourcePolicy(ctx, &sdk.GetResourcePolicyInput{ResourceArn: rt.AgentRuntimeArn})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",` +
		`"Action":"bedrock-agentcore:InvokeAgentRuntime","Resource":"*"}]}`
	_, err = f.client.PutResourcePolicy(
		ctx,
		&sdk.PutResourcePolicyInput{ResourceArn: rt.AgentRuntimeArn, Policy: aws.String(doc)},
	)
	require.NoError(t, err)

	got, err := f.client.GetResourcePolicy(ctx, &sdk.GetResourcePolicyInput{ResourceArn: rt.AgentRuntimeArn})
	require.NoError(t, err)
	assert.JSONEq(t, doc, aws.ToString(got.Policy))

	_, err = f.client.PutResourcePolicy(
		ctx,
		&sdk.PutResourcePolicyInput{ResourceArn: rt.AgentRuntimeArn, Policy: aws.String("nope")},
	)
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = f.client.DeleteResourcePolicy(ctx, &sdk.DeleteResourcePolicyInput{ResourceArn: rt.AgentRuntimeArn})
	require.NoError(t, err)
}

func TestRuntimePersistence(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	rt := createRuntime(t, f.client, "persist_agent")

	snap := f.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	g := newFixture(t)
	require.NoError(t, g.backend.Restore(t.Context(), snap))

	got, err := g.client.GetAgentRuntime(t.Context(), &sdk.GetAgentRuntimeInput{AgentRuntimeId: rt.AgentRuntimeId})
	require.NoError(t, err)
	assert.Equal(t, "persist_agent", aws.ToString(got.AgentRuntimeName))
	assert.Equal(t, rt.AgentRuntimeArn, got.AgentRuntimeArn)
}
