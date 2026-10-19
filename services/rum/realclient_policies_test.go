package rum_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrum "github.com/aws/aws-sdk-go-v2/service/rum"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const allowPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",` +
	`"Action":"rum:PutRumEvents","Resource":"*"}]}`

func TestRealClient_ResourcePolicy(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	f.createMonitor(t, "web")

	name := aws.String("web")

	_, err := f.client.GetResourcePolicy(ctx, &awsrum.GetResourcePolicyInput{Name: name})
	assert.Equal(t, "PolicyNotFoundException", apiCode(err))

	_, err = f.client.DeleteResourcePolicy(ctx, &awsrum.DeleteResourcePolicyInput{Name: name})
	assert.Equal(t, "PolicyNotFoundException", apiCode(err))

	cases := []struct {
		name string
		doc  string
		code string
	}{
		{"not_json", "nope", "MalformedPolicyDocumentException"},
		{"no_statement", `{"Version":"2012-10-17"}`, "MalformedPolicyDocumentException"},
		{"too_big", `{"Statement":"` + strings.Repeat("a", 5000) + `"}`, "PolicySizeLimitExceededException"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, perr := f.client.PutResourcePolicy(
				ctx,
				&awsrum.PutResourcePolicyInput{Name: name, PolicyDocument: aws.String(tc.doc)},
			)
			assert.Equal(t, tc.code, apiCode(perr))
		})
	}

	_, err = f.client.PutResourcePolicy(
		ctx,
		&awsrum.PutResourcePolicyInput{Name: aws.String("nope"), PolicyDocument: aws.String(allowPolicy)},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	put, err := f.client.PutResourcePolicy(
		ctx,
		&awsrum.PutResourcePolicyInput{Name: name, PolicyDocument: aws.String(allowPolicy)},
	)
	require.NoError(t, err)
	assert.JSONEq(t, allowPolicy, aws.ToString(put.PolicyDocument))
	assert.NotEmpty(t, aws.ToString(put.PolicyRevisionId))

	got, err := f.client.GetResourcePolicy(ctx, &awsrum.GetResourcePolicyInput{Name: name})
	require.NoError(t, err)
	assert.JSONEq(t, allowPolicy, aws.ToString(got.PolicyDocument))
	assert.Equal(t, aws.ToString(put.PolicyRevisionId), aws.ToString(got.PolicyRevisionId))

	_, err = f.client.PutResourcePolicy(ctx, &awsrum.PutResourcePolicyInput{
		Name: name, PolicyDocument: aws.String(allowPolicy), PolicyRevisionId: aws.String("stale"),
	})
	assert.Equal(t, "InvalidPolicyRevisionIdException", apiCode(err))

	again, err := f.client.PutResourcePolicy(ctx, &awsrum.PutResourcePolicyInput{
		Name: name, PolicyDocument: aws.String(allowPolicy), PolicyRevisionId: put.PolicyRevisionId,
	})
	require.NoError(t, err)
	assert.NotEqual(
		t,
		aws.ToString(put.PolicyRevisionId),
		aws.ToString(again.PolicyRevisionId),
		"every write mints a revision",
	)

	_, err = f.client.DeleteResourcePolicy(
		ctx,
		&awsrum.DeleteResourcePolicyInput{Name: name, PolicyRevisionId: aws.String("stale")},
	)
	assert.Equal(t, "InvalidPolicyRevisionIdException", apiCode(err))

	del, err := f.client.DeleteResourcePolicy(
		ctx,
		&awsrum.DeleteResourcePolicyInput{Name: name, PolicyRevisionId: again.PolicyRevisionId},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(again.PolicyRevisionId), aws.ToString(del.PolicyRevisionId))

	_, err = f.client.GetResourcePolicy(ctx, &awsrum.GetResourcePolicyInput{Name: name})
	assert.Equal(t, "PolicyNotFoundException", apiCode(err))
}
