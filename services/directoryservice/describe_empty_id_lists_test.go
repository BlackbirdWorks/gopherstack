package directoryservice_test

import (
	"testing"

	directoryservicesdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// TestDescribe_EmptyIDListRejected pins api_op_DescribeDirectories.go and
// api_op_DescribeTrusts.go: null means all, an empty list is InvalidParameterException.
func TestDescribe_EmptyIDListRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(c *directoryservicesdk.Client) error
		name    string
		wantErr bool
	}{
		{
			name: "directories_nil",
			call: func(c *directoryservicesdk.Client) error {
				_, err := c.DescribeDirectories(t.Context(), &directoryservicesdk.DescribeDirectoriesInput{})

				return err
			},
		},
		{
			name:    "directories_empty",
			wantErr: true,
			call: func(c *directoryservicesdk.Client) error {
				_, err := c.DescribeDirectories(t.Context(), &directoryservicesdk.DescribeDirectoriesInput{
					DirectoryIds: []string{},
				})

				return err
			},
		},
		{
			name: "trusts_nil",
			call: func(c *directoryservicesdk.Client) error {
				_, err := c.DescribeTrusts(t.Context(), &directoryservicesdk.DescribeTrustsInput{})

				return err
			},
		},
		{
			name:    "trusts_empty",
			wantErr: true,
			call: func(c *directoryservicesdk.Client) error {
				_, err := c.DescribeTrusts(t.Context(), &directoryservicesdk.DescribeTrustsInput{TrustIds: []string{}})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
			err := tc.call(newTestDirectoryServiceClient(t, h))

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameterException")

				return
			}

			require.NoError(t, err)
		})
	}
}
