package verifiedpermissions_test

import (
	"testing"

	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	avptypes "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPolicyStore_TagsOnlyWhenRequested(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags map[string]string
		name     string
		request  bool
	}{
		{name: "omitted by default"},
		{name: "returned when requested", request: true, wantTags: map[string]string{"env": "prod"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			created, err := client.CreatePolicyStore(t.Context(), &avpsdk.CreatePolicyStoreInput{
				ValidationSettings: &avptypes.ValidationSettings{Mode: avptypes.ValidationModeOff},
				Tags:               map[string]string{"env": "prod"},
			})
			require.NoError(t, err)

			got, err := client.GetPolicyStore(t.Context(), &avpsdk.GetPolicyStoreInput{
				PolicyStoreId: created.PolicyStoreId,
				Tags:          tt.request,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, got.Tags)
		})
	}
}
