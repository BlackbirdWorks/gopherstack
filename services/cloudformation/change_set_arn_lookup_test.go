package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// The AWS CLI deploy waiter passes the change set ARN as ChangeSetName.
func TestChangeSet_AddressedByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act  func(b *cloudformation.InMemoryBackend, stack, arn string) error
		name string
	}{
		{
			name: "describe",
			act: func(b *cloudformation.InMemoryBackend, stack, arn string) error {
				_, err := b.DescribeChangeSet(stack, arn)

				return err
			},
		},
		{
			name: "describe_without_stack_name",
			act: func(b *cloudformation.InMemoryBackend, _, arn string) error {
				_, err := b.DescribeChangeSet("", arn)

				return err
			},
		},
		{
			name: "execute",
			act: func(b *cloudformation.InMemoryBackend, stack, arn string) error {
				return b.ExecuteChangeSet(t.Context(), stack, arn, false, false)
			},
		},
		{
			name: "delete",
			act: func(b *cloudformation.InMemoryBackend, stack, arn string) error {
				return b.DeleteChangeSet(stack, arn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudformation.NewInMemoryBackend()
			cs, err := b.CreateChangeSet(
				t.Context(), "arn-stack", "arn-cs", simpleTemplate, "", nil, nil, nil,
				cloudformation.CreateChangeSetOptions{},
			)
			require.NoError(t, err)

			require.NoError(t, tt.act(b, "arn-stack", cs.ChangeSetID))
		})
	}
}
