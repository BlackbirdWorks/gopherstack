package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_Recipe_DescribeList(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "ListRecipes", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	listed := personalizeUnmarshal(t, rec)
	recipes := listed["recipes"].([]any)
	assert.NotEmpty(t, recipes)

	first := recipes[0].(map[string]any)
	recipeArn, _ := first["recipeArn"].(string)
	assert.NotEmpty(t, recipeArn)

	rec = personalizeDo(t, h, "DescribeRecipe", map[string]any{"recipeArn": recipeArn})
	require.Equal(t, http.StatusOK, rec.Code)
	m := personalizeUnmarshal(t, rec)
	r := m["recipe"].(map[string]any)
	assert.Equal(t, "ACTIVE", r["status"])

	// Missing recipe → 400
	rec = personalizeDo(
		t,
		h,
		"DescribeRecipe",
		map[string]any{"recipeArn": "arn:aws:personalize:::recipe/not-a-real-recipe"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- FeatureTransformation / Algorithm ---
