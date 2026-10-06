package ram_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ramsdk "github.com/aws/aws-sdk-go-v2/service/ram"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// TestListResourceTypes_Pagination pins MaxResults/NextToken
// (ram@v1.39.4 api_op_ListResourceTypes.go).
func TestListResourceTypes_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pageSize int32
	}{
		{name: "size_1", pageSize: 1},
		{name: "size_5", pageSize: 5},
		{name: "size_7", pageSize: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestRAMClient(t, ram.NewHandler(ram.NewInMemoryBackend("000000000000", "us-east-1")))

			full, err := client.ListResourceTypes(t.Context(), &ramsdk.ListResourceTypesInput{})
			require.NoError(t, err)

			var got []string

			var token *string

			for {
				out, listErr := client.ListResourceTypes(t.Context(), &ramsdk.ListResourceTypesInput{
					MaxResults: aws.Int32(tt.pageSize), NextToken: token,
				})
				require.NoError(t, listErr)
				require.LessOrEqual(t, len(out.ResourceTypes), int(tt.pageSize))

				for _, rt := range out.ResourceTypes {
					got = append(got, aws.ToString(rt.ResourceType))
				}

				if out.NextToken == nil {
					break
				}

				token = out.NextToken
			}

			want := make([]string, 0, len(full.ResourceTypes))
			for _, rt := range full.ResourceTypes {
				want = append(want, aws.ToString(rt.ResourceType))
			}

			require.Equal(t, want, got)
		})
	}
}
