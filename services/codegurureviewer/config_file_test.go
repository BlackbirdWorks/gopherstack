package codegurureviewer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/codegurureviewer"
	"github.com/aws/aws-sdk-go-v2/service/codegurureviewer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodeReviewConfigFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		config         string
		wantState      types.ConfigFileState
		wantFindings   int64
		wantSuppressed bool
	}{
		{name: "no_excludes", config: "version: 1\n", wantState: types.ConfigFileStatePresent, wantFindings: 5},
		{
			name:           "exclude_by_glob",
			config:         "excludeFiles:\n  - '**/*.go'\n",
			wantState:      types.ConfigFileStatePresent,
			wantFindings:   2,
			wantSuppressed: true,
		},
		{
			name:         "exclude_other_dir",
			config:       "excludeFiles:\n  - 'src/**'\n",
			wantState:    types.ConfigFileStatePresent,
			wantFindings: 5,
		},
		{
			name:         "invalid_yaml",
			config:       "excludeFiles: [\n",
			wantState:    types.ConfigFileStatePresentWithErrors,
			wantFindings: 5,
		},
		{
			name:         "wrong_shape",
			config:       "excludeFiles: {a: b}\n",
			wantState:    types.ConfigFileStatePresentWithErrors,
			wantFindings: 5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.commitRepo(t, "app", map[string]string{
				"main.go": badGo, "keys.py": badPy, "aws-codeguru-reviewer.yml": tc.config,
			})
			assoc := f.associateCodeCommit(t, "app")
			cr := f.review(t, "r", assoc, "main").CodeReview
			f.settle()

			in := &sdk.DescribeCodeReviewInput{CodeReviewArn: cr.CodeReviewArn}
			d, err := f.client.DescribeCodeReview(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, tc.wantState, d.CodeReview.ConfigFileState)
			assert.Equal(t, tc.wantFindings, aws.ToInt64(d.CodeReview.Metrics.FindingsCount))

			if tc.wantSuppressed {
				assert.Positive(t, aws.ToInt64(d.CodeReview.Metrics.SuppressedLinesOfCodeCount))
			} else {
				assert.Zero(t, aws.ToInt64(d.CodeReview.Metrics.SuppressedLinesOfCodeCount))
			}
		})
	}
}
