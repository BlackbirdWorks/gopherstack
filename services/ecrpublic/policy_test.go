package ecrpublic_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrpublicsdk "github.com/aws/aws-sdk-go-v2/service/ecrpublic"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPolicyText = `{"Version":"2012-10-17","Statement":[` +
	`{"Sid":"AllowPull","Effect":"Allow","Principal":"*","Action":["ecr:BatchGetImage"]}]}`

func TestRepositoryPolicyLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(
		ctx,
		&ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("policy-repo")},
	)
	require.NoError(t, err)

	set, err := client.SetRepositoryPolicy(ctx, &ecrpublicsdk.SetRepositoryPolicyInput{
		RepositoryName: aws.String("policy-repo"),
		PolicyText:     aws.String(testPolicyText),
	})
	require.NoError(t, err)
	assert.JSONEq(t, testPolicyText, aws.ToString(set.PolicyText))

	got, err := client.GetRepositoryPolicy(ctx, &ecrpublicsdk.GetRepositoryPolicyInput{
		RepositoryName: aws.String("policy-repo"),
	})
	require.NoError(t, err)
	assert.JSONEq(t, testPolicyText, aws.ToString(got.PolicyText))

	deleted, err := client.DeleteRepositoryPolicy(ctx, &ecrpublicsdk.DeleteRepositoryPolicyInput{
		RepositoryName: aws.String("policy-repo"),
	})
	require.NoError(t, err)
	assert.JSONEq(t, testPolicyText, aws.ToString(deleted.PolicyText))

	_, err = client.GetRepositoryPolicy(ctx, &ecrpublicsdk.GetRepositoryPolicyInput{
		RepositoryName: aws.String("policy-repo"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "RepositoryPolicyNotFoundException", apiErr.ErrorCode())
}

func TestGetRepositoryPolicy_RepositoryNotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.GetRepositoryPolicy(t.Context(), &ecrpublicsdk.GetRepositoryPolicyInput{
		RepositoryName: aws.String("missing"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "RepositoryNotFoundException", apiErr.ErrorCode())
}
