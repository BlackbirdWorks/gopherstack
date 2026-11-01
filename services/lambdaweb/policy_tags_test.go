package lambdaweb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const samplePolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"AWS":"arn:aws:iam::222233334444:role/InvokeRole"},` +
	`"Action":"lambda:InvokeWebFunctionEndpoint",` +
	`"Resource":"arn:aws:lambda:us-east-1:123456789012:web-function/app/endpoint/prod"}]}`

func TestResourcePolicy_Lifecycle(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")
	arn := created.FunctionArn

	_, err := env.client.GetResourcePolicy(t.Context(), &lwsdk.GetResourcePolicyInput{ResourceArn: arn})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")

	put, err := env.client.PutResourcePolicy(
		t.Context(),
		&lwsdk.PutResourcePolicyInput{ResourceArn: arn, Policy: aws.String(samplePolicy)},
	)
	require.NoError(t, err)
	assert.JSONEq(t, samplePolicy, aws.ToString(put.Policy))
	assert.NotEmpty(t, aws.ToString(put.RevisionId))

	got, err := env.client.GetResourcePolicy(t.Context(), &lwsdk.GetResourcePolicyInput{ResourceArn: arn})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(put.RevisionId), aws.ToString(got.RevisionId))

	_, err = env.client.PutResourcePolicy(t.Context(), &lwsdk.PutResourcePolicyInput{
		ResourceArn: arn, Policy: aws.String(samplePolicy), RevisionId: aws.String("stale"),
	})
	assertAPIErrorCode(t, err, "ConflictException")

	replaced, err := env.client.PutResourcePolicy(t.Context(), &lwsdk.PutResourcePolicyInput{
		ResourceArn: arn, Policy: aws.String(samplePolicy), RevisionId: put.RevisionId,
	})
	require.NoError(t, err)
	assert.NotEqual(t, aws.ToString(put.RevisionId), aws.ToString(replaced.RevisionId))

	_, err = env.client.DeleteResourcePolicy(
		t.Context(),
		&lwsdk.DeleteResourcePolicyInput{ResourceArn: arn, RevisionId: put.RevisionId},
	)
	assertAPIErrorCode(t, err, "ConflictException")

	_, err = env.client.DeleteResourcePolicy(
		t.Context(),
		&lwsdk.DeleteResourcePolicyInput{ResourceArn: arn, RevisionId: replaced.RevisionId},
	)
	require.NoError(t, err)

	_, err = env.client.DeleteResourcePolicy(t.Context(), &lwsdk.DeleteResourcePolicyInput{ResourceArn: arn})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}

func TestResourcePolicy_Validation(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")

	tests := []struct {
		name   string
		arn    string
		policy string
		code   string
	}{
		{name: "not json", arn: aws.ToString(created.FunctionArn), policy: "nope", code: "ValidationException"},
		{
			name:   "no statement",
			arn:    aws.ToString(created.FunctionArn),
			policy: `{"Version":"2012-10-17"}`,
			code:   "ValidationException",
		},
		{
			name:   "bad arn",
			arn:    "arn:aws:lambda:us-east-1:123456789012:function:x",
			policy: samplePolicy,
			code:   "ValidationException",
		},
		{
			name:   "endpoint arn",
			arn:    aws.ToString(created.Endpoint.EndpointArn),
			policy: samplePolicy,
			code:   "ValidationException",
		},
		{
			name:   "unknown function",
			arn:    "arn:aws:lambda:us-east-1:123456789012:web-function/ghost",
			policy: samplePolicy,
			code:   "ResourceNotFoundException",
		},
		{
			name:   "other account",
			arn:    "arn:aws:lambda:us-east-1:999999999999:web-function/app",
			policy: samplePolicy,
			code:   "ValidationException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := env.client.PutResourcePolicy(
				t.Context(),
				&lwsdk.PutResourcePolicyInput{ResourceArn: aws.String(tc.arn), Policy: aws.String(tc.policy)},
			)
			assertAPIErrorCode(t, err, tc.code)
		})
	}
}

func TestTags_Lifecycle(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")
	arn := created.FunctionArn

	_, err := env.client.TagResource(
		t.Context(),
		&lwsdk.TagResourceInput{Resource: arn, Tags: map[string]string{"a": "1", "b": "2"}},
	)
	require.NoError(t, err)

	_, err = env.client.TagResource(
		t.Context(),
		&lwsdk.TagResourceInput{Resource: arn, Tags: map[string]string{"b": "3"}},
	)
	require.NoError(t, err)

	out, err := env.client.ListTags(t.Context(), &lwsdk.ListTagsInput{Resource: arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "b": "3"}, out.Tags)

	_, err = env.client.UntagResource(
		t.Context(),
		&lwsdk.UntagResourceInput{Resource: arn, TagKeys: []string{"a", "missing"}},
	)
	require.NoError(t, err)

	out, err = env.client.ListTags(t.Context(), &lwsdk.ListTagsInput{Resource: arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"b": "3"}, out.Tags)
}

func TestTags_Errors(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")

	many := map[string]string{}
	for i := range 51 {
		many[string(rune('a'+i%26))+string(rune('a'+i/26))] = "v"
	}

	tests := []struct {
		name string
		arn  string
		tags map[string]string
		code string
	}{
		{
			name: "reserved prefix",
			arn:  aws.ToString(created.FunctionArn),
			tags: map[string]string{"AWS:x": "y"},
			code: "ValidationException",
		},
		{name: "too many", arn: aws.ToString(created.FunctionArn), tags: many, code: "ValidationException"},
		{
			name: "unknown function",
			arn:  "arn:aws:lambda:us-east-1:123456789012:web-function/ghost",
			tags: map[string]string{"a": "b"},
			code: "ResourceNotFoundException",
		},
		{
			name: "revision arn",
			arn:  aws.ToString(created.Revision.RevisionArn),
			tags: map[string]string{"a": "b"},
			code: "ValidationException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := env.client.TagResource(
				t.Context(),
				&lwsdk.TagResourceInput{Resource: aws.String(tc.arn), Tags: tc.tags},
			)
			assertAPIErrorCode(t, err, tc.code)
		})
	}

	_, err := env.client.ListTags(
		t.Context(),
		&lwsdk.ListTagsInput{Resource: aws.String("arn:aws:lambda:us-east-1:123456789012:web-function/ghost")},
	)
	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
}
