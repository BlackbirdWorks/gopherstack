package iot_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iotsdktypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestListPolicies_AscendingOrderAndPagination proves ListPolicies honors
// AscendingOrder ("If true, the results are returned in ascending creation
// order", iot@v1.77.4 api_op_ListPolicies.go) and the real pageSize/marker
// pagination binding (awsRestjson1_serializeOpHttpBindingsListPoliciesInput,
// serializers.go) -- the handler previously read maxResults/nextToken (a
// different op's binding), so a real client's pageSize/marker were always
// ignored, and results had no creation-date ordering at all (backend
// returned them name-sorted).
func TestListPolicies_AscendingOrderAndPagination(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)
	client := newTestIoTClient(t, h)

	base := time.Now()
	b.AddPolicyInternal(iot.Policy{PolicyName: "zulu", CreatedAt: base})
	b.AddPolicyInternal(iot.Policy{PolicyName: "alpha", CreatedAt: base.Add(time.Second)})
	b.AddPolicyInternal(iot.Policy{PolicyName: "mike", CreatedAt: base.Add(2 * time.Second)})

	out, err := client.ListPolicies(t.Context(), &iotsdk.ListPoliciesInput{AscendingOrder: true})
	require.NoError(t, err)
	require.Len(t, out.Policies, 3)
	assert.Equal(t, []string{"zulu", "alpha", "mike"}, policyNames(out.Policies))

	page, err := client.ListPolicies(t.Context(), &iotsdk.ListPoliciesInput{PageSize: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, page.Policies, 2)
	require.NotNil(t, page.NextMarker)

	rest, err := client.ListPolicies(
		t.Context(), &iotsdk.ListPoliciesInput{PageSize: aws.Int32(2), Marker: page.NextMarker},
	)
	require.NoError(t, err)
	assert.Len(t, rest.Policies, 1)
}

// TestListPrincipalPolicies_AscendingOrder proves ListPrincipalPolicies
// sorts by each policy's own creation date ("results are returned in
// ascending creation order", api_op_ListPrincipalPolicies.go) rather than
// alphabetically by name.
func TestListPrincipalPolicies_AscendingOrder(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)
	client := newTestIoTClient(t, h)

	principal := "arn:aws:iot:us-east-1:123456789012:cert/abc"
	base := time.Now()
	b.AddPolicyInternal(iot.Policy{PolicyName: "zulu", CreatedAt: base})
	b.AddPolicyInternal(iot.Policy{PolicyName: "alpha", CreatedAt: base.Add(time.Second)})
	require.NoError(t, b.AttachPolicy(&iot.AttachPolicyInput{PolicyName: "zulu", Target: principal}))
	require.NoError(t, b.AttachPolicy(&iot.AttachPolicyInput{PolicyName: "alpha", Target: principal}))

	//nolint:staticcheck // deprecated-but-real op still routed by this backend
	out, err := client.ListPrincipalPolicies(t.Context(), &iotsdk.ListPrincipalPoliciesInput{
		Principal: aws.String(principal), AscendingOrder: true,
	})
	require.NoError(t, err)
	require.Len(t, out.Policies, 2)
	assert.Equal(t, []string{"zulu", "alpha"}, policyNames(out.Policies))
}

// TestListAuthorizers_AscendingOrderStatusAndPagination proves
// ListAuthorizers honors AscendingOrder ("ascending alphabetical order"),
// Status, and pageSize/marker pagination -- all previously unread.
func TestListAuthorizers_AscendingOrderStatusAndPagination(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)
	client := newTestIoTClient(t, h)

	fnArn := "arn:aws:lambda:us-east-1:123456789012:function:auth"
	for _, name := range []string{"zulu", "alpha", "mike"} {
		_, err := client.CreateAuthorizer(t.Context(), &iotsdk.CreateAuthorizerInput{
			AuthorizerName:        aws.String(name),
			AuthorizerFunctionArn: aws.String(fnArn),
			Status:                iotsdktypes.AuthorizerStatusActive,
		})
		require.NoError(t, err)
	}

	_, err := client.CreateAuthorizer(t.Context(), &iotsdk.CreateAuthorizerInput{
		AuthorizerName:        aws.String("inactive-one"),
		AuthorizerFunctionArn: aws.String(fnArn),
		Status:                iotsdktypes.AuthorizerStatusInactive,
	})
	require.NoError(t, err)

	out, err := client.ListAuthorizers(t.Context(), &iotsdk.ListAuthorizersInput{AscendingOrder: true})
	require.NoError(t, err)
	require.Len(t, out.Authorizers, 4)
	assert.Equal(t, []string{"alpha", "inactive-one", "mike", "zulu"}, authorizerNames(out.Authorizers))

	active, err := client.ListAuthorizers(t.Context(), &iotsdk.ListAuthorizersInput{
		Status: iotsdktypes.AuthorizerStatusActive,
	})
	require.NoError(t, err)
	assert.Len(t, active.Authorizers, 3)

	paged, err := client.ListAuthorizers(t.Context(), &iotsdk.ListAuthorizersInput{PageSize: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, paged.Authorizers, 2)
	require.NotNil(t, paged.NextMarker)
}

// TestListRoleAliases_AscendingOrderAndPagination proves ListRoleAliases
// honors AscendingOrder and pageSize/marker -- previously unread.
func TestListRoleAliases_AscendingOrderAndPagination(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)
	client := newTestIoTClient(t, h)

	roleARN := "arn:aws:iam::123456789012:role/R"
	for _, alias := range []string{"zulu", "alpha", "mike"} {
		_, err := client.CreateRoleAlias(t.Context(), &iotsdk.CreateRoleAliasInput{
			RoleAlias: aws.String(alias), RoleArn: aws.String(roleARN),
		})
		require.NoError(t, err)
	}

	out, err := client.ListRoleAliases(t.Context(), &iotsdk.ListRoleAliasesInput{AscendingOrder: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "mike", "zulu"}, out.RoleAliases)

	paged, err := client.ListRoleAliases(t.Context(), &iotsdk.ListRoleAliasesInput{PageSize: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, paged.RoleAliases, 2)
	require.NotNil(t, paged.NextMarker)
}

// TestListStreams_AscendingOrderAndPagination proves ListStreams honors
// AscendingOrder and maxResults/nextToken pagination -- previously unread.
func TestListStreams_AscendingOrderAndPagination(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)
	client := newTestIoTClient(t, h)

	roleARN := "arn:aws:iam::123456789012:role/R"
	files := []iotsdktypes.StreamFile{{
		FileId:     aws.Int32(1),
		S3Location: &iotsdktypes.S3Location{Bucket: aws.String("b"), Key: aws.String("k")},
	}}

	for _, id := range []string{"zulu", "alpha", "mike"} {
		_, err := client.CreateStream(t.Context(), &iotsdk.CreateStreamInput{
			StreamId: aws.String(id), RoleArn: aws.String(roleARN), Files: files,
		})
		require.NoError(t, err)
	}

	out, err := client.ListStreams(t.Context(), &iotsdk.ListStreamsInput{AscendingOrder: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "mike", "zulu"}, streamIDs(out.Streams))

	paged, err := client.ListStreams(t.Context(), &iotsdk.ListStreamsInput{MaxResults: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, paged.Streams, 2)
	require.NotNil(t, paged.NextToken)
}

func policyNames(ps []iotsdktypes.Policy) []string {
	names := make([]string, len(ps))
	for i, p := range ps {
		names[i] = aws.ToString(p.PolicyName)
	}

	return names
}

func authorizerNames(as []iotsdktypes.AuthorizerSummary) []string {
	names := make([]string, len(as))
	for i, a := range as {
		names[i] = aws.ToString(a.AuthorizerName)
	}

	return names
}

func streamIDs(ss []iotsdktypes.StreamSummary) []string {
	ids := make([]string, len(ss))
	for i, s := range ss {
		ids[i] = aws.ToString(s.StreamId)
	}

	return ids
}
