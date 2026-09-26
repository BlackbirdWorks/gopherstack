package memorydb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	memorydbtypes "github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeUsers_FiltersByUserName proves DescribeUsersInput.Filters is
// honoured (types.Filter doc comment: "The property being filtered. For
// example, UserName", value example "user-123") -- previously silently
// dropped, returning every user regardless of the filter.
func TestDescribeUsers_FiltersByUserName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMemoryDBClient(t, h)
	ctx := t.Context()

	for _, name := range []string{"user-alpha", "user-beta"} {
		_, err := client.CreateUser(ctx, &memorydbsdk.CreateUserInput{
			UserName:     aws.String(name),
			AccessString: aws.String("on ~* &* +@all"),
			AuthenticationMode: &memorydbtypes.AuthenticationMode{
				Type: memorydbtypes.InputAuthenticationTypeIam,
			},
		})
		require.NoError(t, err)
	}

	tests := []struct {
		name      string
		filters   []memorydbtypes.Filter
		wantNames []string
	}{
		{
			name:      "no filter returns all users",
			filters:   nil,
			wantNames: []string{"user-alpha", "user-beta"},
		},
		{
			name: "UserName filter narrows to matching user",
			filters: []memorydbtypes.Filter{
				{Name: aws.String("UserName"), Values: []string{"user-alpha"}},
			},
			wantNames: []string{"user-alpha"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.DescribeUsers(ctx, &memorydbsdk.DescribeUsersInput{Filters: tt.filters})
			require.NoError(t, err)

			gotNames := make([]string, 0, len(out.Users))
			for _, u := range out.Users {
				gotNames = append(gotNames, aws.ToString(u.Name))
			}

			assert.ElementsMatch(t, tt.wantNames, gotNames)
		})
	}
}
