package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAddCustomAttributes_ConstraintsRoundTrip drives AddCustomAttributes and
// DescribeUserPool through a real SDK client and asserts that
// StringAttributeConstraints/NumberAttributeConstraints decode non-nil with
// correct values (gopherstack-xasq). Before the fix, SchemaAttribute wrote
// these as flattened top-level fields (StringAttributeMinLength,
// NumberAttributeMinValue, ...) that the real client never reads -- both the
// request the client sends (nested constraint objects under
// CustomAttributes[i]) and the response it parses (nested constraint objects
// under SchemaAttributes[i]) use the nested shape, so constraints never
// bound and never appeared on read.
func TestAddCustomAttributes_ConstraintsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("constraints-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	_, err = client.AddCustomAttributes(t.Context(), &cognitoidpsdk.AddCustomAttributesInput{
		UserPoolId: aws.String(poolID),
		CustomAttributes: []types.SchemaAttributeType{
			{
				Name:              aws.String("custom:department"),
				AttributeDataType: types.AttributeDataTypeString,
				StringAttributeConstraints: &types.StringAttributeConstraintsType{
					MinLength: aws.String("1"),
					MaxLength: aws.String("256"),
				},
			},
			{
				Name:              aws.String("custom:headcount"),
				AttributeDataType: types.AttributeDataTypeNumber,
				NumberAttributeConstraints: &types.NumberAttributeConstraintsType{
					MinValue: aws.String("0"),
					MaxValue: aws.String("100000"),
				},
			},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeUserPool(t.Context(), &cognitoidpsdk.DescribeUserPoolInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)

	byName := make(map[string]types.SchemaAttributeType, len(desc.UserPool.SchemaAttributes))
	for _, a := range desc.UserPool.SchemaAttributes {
		byName[aws.ToString(a.Name)] = a
	}

	dept, ok := byName["custom:department"]
	require.True(t, ok, "custom:department missing from SchemaAttributes")
	require.NotNil(t, dept.StringAttributeConstraints, "StringAttributeConstraints decoded nil")
	assert.Equal(t, "1", aws.ToString(dept.StringAttributeConstraints.MinLength))
	assert.Equal(t, "256", aws.ToString(dept.StringAttributeConstraints.MaxLength))

	headcount, ok := byName["custom:headcount"]
	require.True(t, ok, "custom:headcount missing from SchemaAttributes")
	require.NotNil(t, headcount.NumberAttributeConstraints, "NumberAttributeConstraints decoded nil")
	assert.Equal(t, "0", aws.ToString(headcount.NumberAttributeConstraints.MinValue))
	assert.Equal(t, "100000", aws.ToString(headcount.NumberAttributeConstraints.MaxValue))
}
