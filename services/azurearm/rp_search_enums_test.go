package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/search/armsearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchSDK_SemanticAndCMK(t *testing.T) {
	t.Parallel()

	enforcement := func(e armsearch.SearchEncryptionWithCmk) *armsearch.EncryptionWithCmk {
		return &armsearch.EncryptionWithCmk{Enforcement: &e}
	}

	tests := []struct {
		name         string
		props        armsearch.ServiceProperties
		wantSemantic string
		wantEnforce  armsearch.SearchEncryptionWithCmk
		wantErr      bool
	}{
		{name: "defaults", wantEnforce: armsearch.SearchEncryptionWithCmkUnspecified},
		{
			name:         "semantic standard",
			props:        armsearch.ServiceProperties{SemanticSearch: new(armsearch.SearchSemanticSearchStandard)},
			wantSemantic: "standard", wantEnforce: armsearch.SearchEncryptionWithCmkUnspecified,
		},
		{
			name:    "semantic bogus",
			props:   armsearch.ServiceProperties{SemanticSearch: new(armsearch.SearchSemanticSearch("max"))},
			wantErr: true,
		},
		{
			name: "cmk enabled",
			props: armsearch.ServiceProperties{
				EncryptionWithCmk: enforcement(armsearch.SearchEncryptionWithCmkEnabled),
			},
			wantEnforce: armsearch.SearchEncryptionWithCmkEnabled,
		},
		{
			name:    "cmk bogus",
			props:   armsearch.ServiceProperties{EncryptionWithCmk: enforcement("Always")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSearchStack(t)
			svc := basicService(armsearch.SKUNameBasic)
			svc.Properties = &tt.props

			poller, err := s.factory.NewServicesClient().
				BeginCreateOrUpdate(t.Context(), searchRG, "svc1", svc, nil, nil)
			if err == nil {
				_, err = poller.PollUntilDone(t.Context(), nil)
			}

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := s.factory.NewServicesClient().Get(t.Context(), searchRG, "svc1", nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEnforce, *got.Properties.EncryptionWithCmk.Enforcement)
			assert.Equal(t, armsearch.SearchEncryptionComplianceStatusCompliant,
				*got.Properties.EncryptionWithCmk.EncryptionComplianceStatus)

			if tt.wantSemantic != "" {
				assert.Equal(t, tt.wantSemantic, string(*got.Properties.SemanticSearch))
			}
		})
	}
}
