package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPredefinedAttributeLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "predef"))
	values := func(v ...string) types.PredefinedAttributeValues {
		return &types.PredefinedAttributeValuesMemberStringList{Value: append([]string{}, v...)}
	}

	_, err := f.client.CreatePredefinedAttribute(t.Context(), &sdk.CreatePredefinedAttributeInput{
		InstanceId: inst, Name: aws.String("language"), Values: values("en", "de"), Purposes: []string{"EMAIL_ROUTING"},
		AttributeConfiguration: &types.InputPredefinedAttributeConfiguration{EnableValueValidationOnAssociation: true},
	})
	require.NoError(t, err)

	tests := []struct {
		in   sdk.CreatePredefinedAttributeInput
		name string
		code string
	}{
		{
			name: "duplicate",
			in:   sdk.CreatePredefinedAttributeInput{Name: aws.String("language"), Values: values("x")},
			code: "DuplicateResourceException",
		},
		{
			name: "empty values",
			in:   sdk.CreatePredefinedAttributeInput{Name: aws.String("empty"), Values: values()},
			code: "InvalidParameterException",
		},
		{
			name: "blank value",
			in:   sdk.CreatePredefinedAttributeInput{Name: aws.String("blank"), Values: values(" ")},
			code: "InvalidParameterException",
		},
	}
	for _, tt := range tests {
		in := tt.in
		in.InstanceId = inst
		_, cerr := f.client.CreatePredefinedAttribute(t.Context(), &in)
		assert.Equal(t, tt.code, apiCode(cerr), tt.name)
	}

	got, err := f.client.DescribePredefinedAttribute(t.Context(),
		&sdk.DescribePredefinedAttributeInput{InstanceId: inst, Name: aws.String("language")})
	require.NoError(t, err)

	list, ok := got.PredefinedAttribute.Values.(*types.PredefinedAttributeValuesMemberStringList)
	require.True(t, ok)
	assert.Equal(t, []string{"en", "de"}, list.Value)
	assert.True(t, got.PredefinedAttribute.AttributeConfiguration.EnableValueValidationOnAssociation)
	assert.NotNil(t, got.PredefinedAttribute.LastModifiedTime)

	_, err = f.client.UpdatePredefinedAttribute(t.Context(), &sdk.UpdatePredefinedAttributeInput{
		InstanceId: inst, Name: aws.String("language"), Values: values("fr"),
	})
	require.NoError(t, err)

	for i := range 3 {
		_, err = f.client.CreatePredefinedAttribute(t.Context(), &sdk.CreatePredefinedAttributeInput{
			InstanceId: inst, Name: aws.String("attr" + string(rune('a'+i))), Values: values("v"),
		})
		require.NoError(t, err)
	}

	page, err := f.client.ListPredefinedAttributes(t.Context(),
		&sdk.ListPredefinedAttributesInput{InstanceId: inst, MaxResults: aws.Int32(2)})
	require.NoError(t, err)
	assert.Len(t, page.PredefinedAttributeSummaryList, 2)
	assert.NotEmpty(t, aws.ToString(page.NextToken))

	dst := f.restored(t)
	got, err = dst.client.DescribePredefinedAttribute(t.Context(),
		&sdk.DescribePredefinedAttributeInput{InstanceId: inst, Name: aws.String("language")})
	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{"fr"},
		got.PredefinedAttribute.Values.(*types.PredefinedAttributeValuesMemberStringList).Value,
	)
	assert.Equal(t, []string{"EMAIL_ROUTING"}, got.PredefinedAttribute.Purposes)

	_, err = f.client.DeletePredefinedAttribute(t.Context(),
		&sdk.DeletePredefinedAttributeInput{InstanceId: inst, Name: aws.String("language")})
	require.NoError(t, err)
	_, err = f.client.DescribePredefinedAttribute(t.Context(),
		&sdk.DescribePredefinedAttributeInput{InstanceId: inst, Name: aws.String("language")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
