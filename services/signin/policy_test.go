package signin_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	signinsdk "github.com/aws/aws-sdk-go-v2/service/signin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutStatementValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   signinsdk.PutResourcePermissionStatementInput
	}{
		{name: "no condition", in: signinsdk.PutResourcePermissionStatementInput{}},
		{name: "bad vpc", in: signinsdk.PutResourcePermissionStatementInput{SourceVpc: aws.String("vpc-xyz"), RequestedRegion: aws.String("us-east-1")}},
		{name: "vpc without region", in: signinsdk.PutResourcePermissionStatementInput{SourceVpc: aws.String("vpc-0a1b2c3d")}},
		{name: "bad vpce", in: signinsdk.PutResourcePermissionStatementInput{SigninSourceVpce: aws.String("vpce-!"), RequestedRegion: aws.String("us-east-1")}},
		{name: "bad ip", in: signinsdk.PutResourcePermissionStatementInput{SourceIp: aws.String("300.1.1.1")}},
		{name: "bad principal", in: signinsdk.PutResourcePermissionStatementInput{ExcludedPrincipal: aws.String("arn:aws:s3:::bucket-with-a-long-name")}},
		{name: "bad region", in: signinsdk.PutResourcePermissionStatementInput{SourceVpc: aws.String("vpc-0a1b2c3d"), RequestedRegion: aws.String("mars")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newFixture(t).self(t).PutResourcePermissionStatement(t.Context(), &tt.in)
			assert.Equal(t, "ValidationException", apiCode(err))
		})
	}
}

func TestPermissionStatementLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.self(t)

	_, err := c.GetResourcePolicy(t.Context(), &signinsdk.GetResourcePolicyInput{})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	empty, err := c.ListResourcePermissionStatements(t.Context(), &signinsdk.ListResourcePermissionStatementsInput{})
	require.NoError(t, err)
	assert.Empty(t, empty.PermissionStatements)

	put, err := c.PutResourcePermissionStatement(t.Context(), &signinsdk.PutResourcePermissionStatementInput{
		SourceVpc: aws.String("vpc-0a1b2c3d4e5f67890"), RequestedRegion: aws.String("us-east-1"),
		SigninSourceVpce: aws.String("vpce-1a2b3c4d"), ExcludedPrincipal: aws.String("arn:aws:iam::123456789012:role/BreakGlass"),
		ClientToken: aws.String("tok-1"),
	})
	require.NoError(t, err)
	assert.Regexp(t, `^[A-Za-z0-9+/]{64}$`, aws.ToString(put.StatementId))

	replay, err := c.PutResourcePermissionStatement(t.Context(), &signinsdk.PutResourcePermissionStatementInput{
		SourceVpc: aws.String("vpc-0a1b2c3d4e5f67890"), RequestedRegion: aws.String("us-east-1"),
		SigninSourceVpce: aws.String("vpce-1a2b3c4d"), ExcludedPrincipal: aws.String("arn:aws:iam::123456789012:role/BreakGlass"),
		ClientToken: aws.String("tok-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(put.StatementId), aws.ToString(replay.StatementId))

	_, err = c.PutResourcePermissionStatement(t.Context(), &signinsdk.PutResourcePermissionStatementInput{
		SourceIp: aws.String("203.0.113.0/24"), ClientToken: aws.String("tok-1"),
	})
	assert.Equal(t, "ConflictException", apiCode(err))

	_, err = c.PutResourcePermissionStatement(t.Context(), &signinsdk.PutResourcePermissionStatementInput{
		SourceVpc: aws.String("vpc-0a1b2c3d4e5f67890"), RequestedRegion: aws.String("us-east-1"),
		SigninSourceVpce: aws.String("vpce-1a2b3c4d"), ExcludedPrincipal: aws.String("arn:aws:iam::123456789012:role/BreakGlass"),
		ClientToken: aws.String("tok-2"),
	})
	assert.Equal(t, "ConflictException", apiCode(err))

	f.clock.advance(time.Second)

	second, err := c.PutResourcePermissionStatement(t.Context(), &signinsdk.PutResourcePermissionStatementInput{
		SourceIp: aws.String("203.0.113.0/24"), ClientToken: aws.String("tok-3"),
	})
	require.NoError(t, err)

	pol, err := c.GetResourcePolicy(t.Context(), &signinsdk.GetResourcePolicyInput{})
	require.NoError(t, err)
	require.Len(t, pol.SigninResourceBasedPolicy.Statement, 2)
	assert.Equal(t, "2012-10-17", aws.ToString(pol.SigninResourceBasedPolicy.Version))
	assert.Equal(t, "Deny", aws.ToString(pol.SigninResourceBasedPolicy.Statement[0].Effect))
	assert.Equal(t, []string{"vpc-0a1b2c3d4e5f67890"}, pol.SigninResourceBasedPolicy.Statement[0].Condition["StringNotEquals"]["aws:SourceVpc"])
	assert.Equal(t, []string{"203.0.113.0/24"}, pol.SigninResourceBasedPolicy.Statement[1].Condition["NotIpAddress"]["aws:SourceIp"])

	other, err := f.client(t, "999999999999").ListResourcePermissionStatements(t.Context(), &signinsdk.ListResourcePermissionStatementsInput{})
	require.NoError(t, err)
	assert.Empty(t, other.PermissionStatements)

	list, err := c.ListResourcePermissionStatements(t.Context(), &signinsdk.ListResourcePermissionStatementsInput{})
	require.NoError(t, err)
	require.Len(t, list.PermissionStatements, 2)
	assert.Equal(t, aws.ToString(put.StatementId), aws.ToString(list.PermissionStatements[0].Sid))

	_, err = c.DeleteResourcePermissionStatement(t.Context(), &signinsdk.DeleteResourcePermissionStatementInput{StatementId: put.StatementId})
	require.NoError(t, err)

	_, err = c.DeleteResourcePermissionStatement(t.Context(), &signinsdk.DeleteResourcePermissionStatementInput{StatementId: put.StatementId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = c.DeleteResourcePermissionStatement(t.Context(), &signinsdk.DeleteResourcePermissionStatementInput{StatementId: aws.String("short")})
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = c.DeleteResourcePermissionStatement(t.Context(), &signinsdk.DeleteResourcePermissionStatementInput{StatementId: second.StatementId})
	require.NoError(t, err)

	_, err = c.GetResourcePolicy(t.Context(), &signinsdk.GetResourcePolicyInput{})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestListStatementsPagination(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.self(t)

	for _, ip := range []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"} {
		_, err := c.PutResourcePermissionStatement(t.Context(), &signinsdk.PutResourcePermissionStatementInput{SourceIp: aws.String(ip)})
		require.NoError(t, err)
		f.clock.advance(time.Second)
	}

	pager := signinsdk.NewListResourcePermissionStatementsPaginator(c, &signinsdk.ListResourcePermissionStatementsInput{MaxResults: aws.Int32(2)})
	pages, total := 0, 0

	for pager.HasMorePages() {
		out, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		pages++
		total += len(out.PermissionStatements)
	}

	assert.Equal(t, 2, pages)
	assert.Equal(t, 3, total)

	_, err := c.ListResourcePermissionStatements(t.Context(), &signinsdk.ListResourcePermissionStatementsInput{MaxResults: aws.Int32(0)})
	assert.Equal(t, "ValidationException", apiCode(err))
}
