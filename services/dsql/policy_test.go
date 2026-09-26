package dsql_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPolicyDoc = `{"Version":"2012-10-17","Statement":[` +
	`{"Effect":"Allow","Principal":"*","Action":"dsql:DbConnect","Resource":"*"}]}`

func TestGetClusterPolicy_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	_, err = client.GetClusterPolicy(ctx, &dsqlsdk.GetClusterPolicyInput{Identifier: created.Identifier})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}

func TestPutAndGetClusterPolicy(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	putOut, err := client.PutClusterPolicy(ctx, &dsqlsdk.PutClusterPolicyInput{
		Identifier: created.Identifier,
		Policy:     aws.String(testPolicyDoc),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(putOut.PolicyVersion))

	getOut, err := client.GetClusterPolicy(ctx, &dsqlsdk.GetClusterPolicyInput{Identifier: created.Identifier})
	require.NoError(t, err)
	assert.JSONEq(t, testPolicyDoc, aws.ToString(getOut.Policy))
	assert.Equal(t, aws.ToString(putOut.PolicyVersion), aws.ToString(getOut.PolicyVersion))
}

func TestPutClusterPolicy_ExpectedVersionMismatch(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	_, err = client.PutClusterPolicy(ctx, &dsqlsdk.PutClusterPolicyInput{
		Identifier: created.Identifier,
		Policy:     aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	_, err = client.PutClusterPolicy(ctx, &dsqlsdk.PutClusterPolicyInput{
		Identifier:            created.Identifier,
		Policy:                aws.String(testPolicyDoc),
		ExpectedPolicyVersion: aws.String("stale-version"),
	})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ConflictException")
}

func TestDeleteClusterPolicy(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	putOut, err := client.PutClusterPolicy(ctx, &dsqlsdk.PutClusterPolicyInput{
		Identifier: created.Identifier,
		Policy:     aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	_, err = client.DeleteClusterPolicy(ctx, &dsqlsdk.DeleteClusterPolicyInput{
		Identifier:            created.Identifier,
		ExpectedPolicyVersion: putOut.PolicyVersion,
	})
	require.NoError(t, err)

	_, err = client.GetClusterPolicy(ctx, &dsqlsdk.GetClusterPolicyInput{Identifier: created.Identifier})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}

func TestDeleteClusterPolicy_ExpectedVersionMismatch(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	_, err = client.PutClusterPolicy(ctx, &dsqlsdk.PutClusterPolicyInput{
		Identifier: created.Identifier,
		Policy:     aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	_, err = client.DeleteClusterPolicy(ctx, &dsqlsdk.DeleteClusterPolicyInput{
		Identifier:            created.Identifier,
		ExpectedPolicyVersion: aws.String("stale-version"),
	})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ConflictException")
}
