package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestExecuteStackRefactor_StackDefinitions verifies that
// CreateStackRefactorInput.StackDefinitions (cloudformation@v1.76.1
// api_op_CreateStackRefactor.go, required) is actually consumed:
// ExecuteStackRefactor must create a mapping's destination stack from its
// StackDefinition when EnableStackCreation is set and the stack doesn't
// exist yet, and must go on failing with ErrStackNotFound when it isn't.
// Before this fix, StackDefinitions was parsed by nothing and every
// destination stack had to already exist regardless of EnableStackCreation.
func TestExecuteStackRefactor_StackDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		enableStackCreation  bool
		giveStackDefinitions bool
		wantErr              bool
	}{
		{
			name:                 "creates missing destination when enabled with definition",
			enableStackCreation:  true,
			giveStackDefinitions: true,
			wantErr:              false,
		},
		{
			name:                 "still fails when creation disabled",
			enableStackCreation:  false,
			giveStackDefinitions: true,
			wantErr:              true,
		},
		{
			name:                 "still fails when no matching definition",
			enableStackCreation:  true,
			giveStackDefinitions: false,
			wantErr:              true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateStack(
				t.Context(), "refdef-src", simpleTemplate, nil, cloudformation.StackOptions{},
			)
			require.NoError(t, err)

			mappings := []cloudformation.ResourceMapping{
				{
					Source: cloudformation.ResourceLocation{
						StackName:         "refdef-src",
						LogicalResourceID: "MyBucket",
					},
					Destination: cloudformation.ResourceLocation{
						StackName:         "refdef-dst",
						LogicalResourceID: "MyBucket",
					},
				},
			}
			var defs []cloudformation.StackDefinition
			if tc.giveStackDefinitions {
				defs = []cloudformation.StackDefinition{
					{StackName: "refdef-dst", TemplateBody: simpleTemplate},
				}
			}

			refactorID, err := b.CreateStackRefactor(
				"refdef test",
				defs,
				mappings,
				tc.enableStackCreation,
			)
			require.NoError(t, err)

			err = b.ExecuteStackRefactor(t.Context(), refactorID)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, cloudformation.ErrStackNotFound)

				return
			}
			require.NoError(t, err)

			dst, err := b.DescribeStack("refdef-dst")
			require.NoError(t, err, "ExecuteStackRefactor must have created the destination stack")
			assert.Equal(t, "CREATE_COMPLETE", dst.StackStatus)
		})
	}
}
