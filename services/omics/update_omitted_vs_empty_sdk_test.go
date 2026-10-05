package omics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateSequenceStore_OmittedVsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		in       omicssdk.UpdateSequenceStoreInput
		wantName string
		wantDesc string
		wantErr  bool
	}{
		{name: "omitted_keeps", wantName: "orig", wantDesc: "orig desc"},
		{
			name:     "explicit_values_apply",
			in:       omicssdk.UpdateSequenceStoreInput{Name: aws.String("new"), Description: aws.String("new desc")},
			wantName: "new", wantDesc: "new desc",
		},
		{
			name:     "explicit_empty_description_clears",
			in:       omicssdk.UpdateSequenceStoreInput{Description: aws.String("")},
			wantName: "orig", wantDesc: "",
		},
		{
			name:    "explicit_empty_name_rejected",
			in:      omicssdk.UpdateSequenceStoreInput{Name: aws.String("")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			created, err := client.CreateSequenceStore(t.Context(), &omicssdk.CreateSequenceStoreInput{
				Name: aws.String("orig"), Description: aws.String("orig desc"),
			})
			require.NoError(t, err)

			in := tt.in
			in.Id = created.Id

			out, err := client.UpdateSequenceStore(t.Context(), &in)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantName, aws.ToString(out.Name))
			assert.Equal(t, tt.wantDesc, aws.ToString(out.Description))
		})
	}
}

func TestUpdateStores_DescriptionOmittedVsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc     *string
		name     string
		wantDesc string
	}{
		{name: "omitted_keeps", desc: nil, wantDesc: "orig desc"},
		{name: "explicit_empty_clears", desc: aws.String(""), wantDesc: ""},
		{name: "explicit_value_applies", desc: aws.String("x"), wantDesc: "x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)

			_, err := client.CreateAnnotationStore(t.Context(), &omicssdk.CreateAnnotationStoreInput{
				Name: aws.String("ann"), Description: aws.String("orig desc"), StoreFormat: types.StoreFormatVcf,
			})
			require.NoError(t, err)

			ann, err := client.UpdateAnnotationStore(t.Context(), &omicssdk.UpdateAnnotationStoreInput{
				Name: aws.String("ann"), Description: tt.desc,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, aws.ToString(ann.Description))

			_, err = client.CreateVariantStore(t.Context(), &omicssdk.CreateVariantStoreInput{
				Name:        aws.String("var"),
				Description: aws.String("orig desc"),
				Reference:   &types.ReferenceItemMemberReferenceArn{Value: testReferenceArn},
			})
			require.NoError(t, err)

			vs, err := client.UpdateVariantStore(t.Context(), &omicssdk.UpdateVariantStoreInput{
				Name: aws.String("var"), Description: tt.desc,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, aws.ToString(vs.Description))
		})
	}
}
