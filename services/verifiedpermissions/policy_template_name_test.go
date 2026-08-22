package verifiedpermissions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPolicyTemplate_NameRoundTrip drives CreatePolicyTemplate,
// UpdatePolicyTemplate, GetPolicyTemplate, and ListPolicyTemplates through a
// real aws-sdk-go-v2 client and asserts Name decodes non-empty and correct
// on every op that carries it (gopherstack-tpu3). Name is settable on
// CreatePolicyTemplateInput/UpdatePolicyTemplateInput
// (api_op_CreatePolicyTemplate.go:94, api_op_UpdatePolicyTemplate.go:104,
// verifiedpermissions@v1.36.4) and required by GetPolicyTemplateOutput and
// PolicyTemplateItem's "name" deserializer case (deserializers.go:9615,
// :7584). CreatePolicyTemplateOutput/UpdatePolicyTemplateOutput carry no
// Name member at all, so it is not asserted there.
func TestPolicyTemplate_NameRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	store, err := client.CreatePolicyStore(t.Context(), &avpsdk.CreatePolicyStoreInput{
		ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
	})
	require.NoError(t, err)

	created, err := client.CreatePolicyTemplate(t.Context(), &avpsdk.CreatePolicyTemplateInput{
		PolicyStoreId: store.PolicyStoreId,
		Statement:     aws.String("permit(principal, action, resource);"),
		Name:          aws.String("original-template-name"),
	})
	require.NoError(t, err)

	got, err := client.GetPolicyTemplate(t.Context(), &avpsdk.GetPolicyTemplateInput{
		PolicyStoreId:    store.PolicyStoreId,
		PolicyTemplateId: created.PolicyTemplateId,
	})
	require.NoError(t, err)
	assert.Equal(t, "original-template-name", aws.ToString(got.Name),
		"GetPolicyTemplate.Name decoded empty: real deserializer requires the \"name\" key")

	listed, err := client.ListPolicyTemplates(t.Context(), &avpsdk.ListPolicyTemplatesInput{
		PolicyStoreId: store.PolicyStoreId,
	})
	require.NoError(t, err)
	require.Len(t, listed.PolicyTemplates, 1)
	assert.Equal(t, "original-template-name", aws.ToString(listed.PolicyTemplates[0].Name),
		"ListPolicyTemplates PolicyTemplateItem.Name decoded empty")

	_, err = client.UpdatePolicyTemplate(t.Context(), &avpsdk.UpdatePolicyTemplateInput{
		PolicyStoreId:    store.PolicyStoreId,
		PolicyTemplateId: created.PolicyTemplateId,
		Statement:        aws.String("permit(principal, action, resource);"),
		Name:             aws.String("updated-template-name"),
	})
	require.NoError(t, err)

	got, err = client.GetPolicyTemplate(t.Context(), &avpsdk.GetPolicyTemplateInput{
		PolicyStoreId:    store.PolicyStoreId,
		PolicyTemplateId: created.PolicyTemplateId,
	})
	require.NoError(t, err)
	assert.Equal(t, "updated-template-name", aws.ToString(got.Name),
		"UpdatePolicyTemplate must persist the new Name, visible on the next Get")
}
