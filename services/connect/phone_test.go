package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPhoneNumberPool(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "phones")
	instARN := "arn:aws:connect:us-east-1:" + testAccount + ":instance/" + inst

	search, err := f.client.SearchAvailablePhoneNumbers(t.Context(), &sdk.SearchAvailablePhoneNumbersInput{
		TargetArn: aws.String(
			instARN,
		),
		PhoneNumberCountryCode: types.PhoneNumberCountryCodeUs,
		PhoneNumberType:        types.PhoneNumberTypeDid,
		PhoneNumberPrefix:      aws.String("+1415"),
		MaxResults:             aws.Int32(3),
	})
	require.NoError(t, err)
	require.Len(t, search.AvailableNumbersList, 3)
	assert.NotNil(t, search.NextToken)

	number := aws.ToString(search.AvailableNumbersList[0].PhoneNumber)
	assert.Contains(t, number, "+1415")

	_, err = f.client.SearchAvailablePhoneNumbers(t.Context(), &sdk.SearchAvailablePhoneNumbersInput{
		TargetArn: aws.String(
			instARN,
		),
		PhoneNumberCountryCode: types.PhoneNumberCountryCodeZw,
		PhoneNumberType:        types.PhoneNumberTypeDid,
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	claim, err := f.client.ClaimPhoneNumber(t.Context(), &sdk.ClaimPhoneNumberInput{
		TargetArn: aws.String(
			instARN,
		),
		PhoneNumber:            aws.String(number),
		PhoneNumberDescription: aws.String("main"),
		Tags:                   map[string]string{"t": "1"},
	})
	require.NoError(t, err)

	again, err := f.client.SearchAvailablePhoneNumbers(t.Context(), &sdk.SearchAvailablePhoneNumbersInput{
		TargetArn: aws.String(
			instARN,
		),
		PhoneNumberCountryCode: types.PhoneNumberCountryCodeUs,
		PhoneNumberType:        types.PhoneNumberTypeDid,
		PhoneNumberPrefix:      aws.String("+1415"),
		MaxResults:             aws.Int32(3),
	})
	require.NoError(t, err)
	assert.NotEqual(
		t,
		number,
		aws.ToString(again.AvailableNumbersList[0].PhoneNumber),
		"claimed numbers leave the pool",
	)

	_, err = f.client.ClaimPhoneNumber(
		t.Context(),
		&sdk.ClaimPhoneNumberInput{TargetArn: aws.String(instARN), PhoneNumber: aws.String(number)},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	_, err = f.client.ClaimPhoneNumber(
		t.Context(),
		&sdk.ClaimPhoneNumberInput{TargetArn: aws.String(instARN), PhoneNumber: aws.String("+15550000000")},
	)
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	got, err := f.client.DescribePhoneNumber(
		t.Context(),
		&sdk.DescribePhoneNumberInput{PhoneNumberId: claim.PhoneNumberId},
	)
	require.NoError(t, err)
	s := got.ClaimedPhoneNumberSummary
	assert.Equal(t, number, aws.ToString(s.PhoneNumber))
	assert.Equal(t, types.PhoneNumberTypeDid, s.PhoneNumberType)
	assert.Equal(t, types.PhoneNumberCountryCodeUs, s.PhoneNumberCountryCode)
	assert.Equal(t, types.PhoneNumberWorkflowStatusClaimed, s.PhoneNumberStatus.Status)
	assert.Equal(t, instARN, aws.ToString(s.TargetArn))

	list, err := f.client.ListPhoneNumbers(t.Context(), &sdk.ListPhoneNumbersInput{InstanceId: aws.String(inst)})
	require.NoError(t, err)
	require.Len(t, list.PhoneNumberSummaryList, 1)

	v2, err := f.client.ListPhoneNumbersV2(
		t.Context(),
		&sdk.ListPhoneNumbersV2Input{PhoneNumberTypes: []types.PhoneNumberType{types.PhoneNumberTypeTollFree}},
	)
	require.NoError(t, err)
	assert.Empty(t, v2.ListPhoneNumbersSummaryList)

	v2, err = f.client.ListPhoneNumbersV2(t.Context(), &sdk.ListPhoneNumbersV2Input{TargetArn: aws.String(instARN)})
	require.NoError(t, err)
	require.Len(t, v2.ListPhoneNumbersSummaryList, 1)
	assert.Equal(t, "main", aws.ToString(v2.ListPhoneNumbersSummaryList[0].PhoneNumberDescription))

	_, err = f.client.UpdatePhoneNumberMetadata(
		t.Context(),
		&sdk.UpdatePhoneNumberMetadataInput{
			PhoneNumberId:          claim.PhoneNumberId,
			PhoneNumberDescription: aws.String("renamed"),
		},
	)
	require.NoError(t, err)

	_, err = f.client.ReleasePhoneNumber(t.Context(), &sdk.ReleasePhoneNumberInput{PhoneNumberId: claim.PhoneNumberId})
	require.NoError(t, err)

	_, err = f.client.DescribePhoneNumber(
		t.Context(),
		&sdk.DescribePhoneNumberInput{PhoneNumberId: claim.PhoneNumberId},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	back, err := f.client.SearchAvailablePhoneNumbers(t.Context(), &sdk.SearchAvailablePhoneNumbersInput{
		TargetArn: aws.String(
			instARN,
		),
		PhoneNumberCountryCode: types.PhoneNumberCountryCodeUs,
		PhoneNumberType:        types.PhoneNumberTypeDid,
		PhoneNumberPrefix:      aws.String("+1415"),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		number,
		aws.ToString(back.AvailableNumbersList[0].PhoneNumber),
		"released numbers return to the pool",
	)
}

func TestPhoneNumberFlowAssociationAndMove(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	a, b := f.newInstance(t, "pa"), f.newInstance(t, "pb")
	arnOf := func(id string) string { return "arn:aws:connect:us-east-1:" + testAccount + ":instance/" + id }

	flow, err := f.client.CreateContactFlow(t.Context(), &sdk.CreateContactFlowInput{
		InstanceId: aws.String(
			a,
		),
		Name:    aws.String("in"),
		Type:    types.ContactFlowTypeContactFlow,
		Content: aws.String(validFlow),
	})
	require.NoError(t, err)

	search, err := f.client.SearchAvailablePhoneNumbers(t.Context(), &sdk.SearchAvailablePhoneNumbersInput{
		TargetArn: aws.String(
			arnOf(a),
		),
		PhoneNumberCountryCode: types.PhoneNumberCountryCodeGb,
		PhoneNumberType:        types.PhoneNumberTypeTollFree,
	})
	require.NoError(t, err)

	claim, err := f.client.ClaimPhoneNumber(
		t.Context(),
		&sdk.ClaimPhoneNumberInput{
			TargetArn:   aws.String(arnOf(a)),
			PhoneNumber: search.AvailableNumbersList[0].PhoneNumber,
		},
	)
	require.NoError(t, err)

	_, err = f.client.AssociatePhoneNumberContactFlow(t.Context(), &sdk.AssociatePhoneNumberContactFlowInput{
		PhoneNumberId: claim.PhoneNumberId, InstanceId: aws.String(a), ContactFlowId: flow.ContactFlowId,
	})
	require.NoError(t, err)

	_, err = f.client.DeleteContactFlow(
		t.Context(),
		&sdk.DeleteContactFlowInput{InstanceId: aws.String(a), ContactFlowId: flow.ContactFlowId},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	_, err = f.client.AssociatePhoneNumberContactFlow(t.Context(), &sdk.AssociatePhoneNumberContactFlowInput{
		PhoneNumberId: claim.PhoneNumberId, InstanceId: aws.String(b), ContactFlowId: flow.ContactFlowId,
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = f.client.DisassociatePhoneNumberContactFlow(
		t.Context(),
		&sdk.DisassociatePhoneNumberContactFlowInput{PhoneNumberId: claim.PhoneNumberId, InstanceId: aws.String(a)},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteContactFlow(
		t.Context(),
		&sdk.DeleteContactFlowInput{InstanceId: aws.String(a), ContactFlowId: flow.ContactFlowId},
	)
	require.NoError(t, err)

	_, err = f.client.UpdatePhoneNumber(
		t.Context(),
		&sdk.UpdatePhoneNumberInput{PhoneNumberId: claim.PhoneNumberId, TargetArn: aws.String(arnOf(b))},
	)
	require.NoError(t, err)

	moved, err := f.client.ListPhoneNumbers(t.Context(), &sdk.ListPhoneNumbersInput{InstanceId: aws.String(b)})
	require.NoError(t, err)
	assert.Len(t, moved.PhoneNumberSummaryList, 1)

	left, err := f.client.ListPhoneNumbers(t.Context(), &sdk.ListPhoneNumbersInput{InstanceId: aws.String(a)})
	require.NoError(t, err)
	assert.Empty(t, left.PhoneNumberSummaryList)
}
