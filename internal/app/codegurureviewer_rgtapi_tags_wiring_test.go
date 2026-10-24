package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	codecommitbackend "github.com/blackbirdworks/gopherstack/services/codecommit"
	codegurureviewerbackend "github.com/blackbirdworks/gopherstack/services/codegurureviewer"
)

func TestInitializeServices_CodeGuruReviewerTagsWiring(t *testing.T) {
	t.Parallel()

	byName, rgtH := newRGTWiringEnv(t)

	h, ok := byName["CodeGuruReviewer"].(*codegurureviewerbackend.Handler)
	require.True(t, ok)

	ccH, ok := byName["CodeCommit"].(*codecommitbackend.Handler)
	require.True(t, ok)

	_, err := ccH.Backend.CreateRepository("repo", "", "", nil)
	require.NoError(t, err)

	assoc, err := h.Backend.AssociateRepository(t.Context(), &codegurureviewerbackend.AssociateRepositoryInput{
		Repository: &codegurureviewerbackend.Repository{
			CodeCommit: &codegurureviewerbackend.CodeCommitRepository{Name: "repo"},
		},
		Tags: map[string]string{"Team": "emu"},
	})
	require.NoError(t, err)

	runRGTWiringCases(t, rgtH, []rgtWiringCase{
		{
			name:         "association",
			arn:          assoc.ARN,
			resourceType: "codeguru-reviewer:association",
			listTags: func(context.Context) (map[string]string, error) {
				return h.Backend.ListTagsForResource(assoc.ARN)
			},
		},
	})
}
