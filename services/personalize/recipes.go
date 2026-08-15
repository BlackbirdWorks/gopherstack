package personalize

import "fmt"

// --- Recipe (read-only) ---

func getBuiltinRecipes() []map[string]any {
	return []map[string]any{
		{
			keyName:       "aws-user-personalization",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-user-personalization",
			keyStatus:     statusActive,
			keyRecipeType: recipeTypeUserPersonalization,
		},
		{
			keyName:       "aws-popularity-count",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-popularity-count",
			keyStatus:     statusActive,
			keyRecipeType: recipeTypeUserPersonalization,
		},
		{
			keyName:       "aws-hrnn",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-hrnn",
			keyStatus:     statusActive,
			keyRecipeType: recipeTypeUserPersonalization,
		},
		{
			keyName:       "aws-hrnn-coldstart",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-hrnn-coldstart",
			keyStatus:     statusActive,
			keyRecipeType: recipeTypeUserPersonalization,
		},
		{
			keyName:       "aws-hrnn-metadata",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-hrnn-metadata",
			keyStatus:     statusActive,
			keyRecipeType: recipeTypeUserPersonalization,
		},
		{
			keyName:       "aws-similar-items",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-similar-items",
			keyStatus:     statusActive,
			keyRecipeType: "RELATED_ITEMS",
		},
		{
			keyName:       "aws-sims",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-sims",
			keyStatus:     statusActive,
			keyRecipeType: "RELATED_ITEMS",
		},
		{
			keyName:       "aws-personalized-ranking",
			keyRecipeArn:  "arn:aws:personalize:::recipe/aws-personalized-ranking",
			keyStatus:     statusActive,
			keyRecipeType: "PERSONALIZED_RANKING",
		},
	}
}

// recipeExists reports whether recipeArn matches one of the built-in
// recipe catalog entries. Used to FK-validate CreateRecommender/
// CreateSolution's recipeArn against the read-only recipe catalog the same
// way arnExists validates mutable resource ARNs.
func recipeExists(recipeArn string) bool {
	for _, r := range getBuiltinRecipes() {
		if r[keyRecipeArn] == recipeArn {
			return true
		}
	}

	return false
}

func (h *Handler) describeRecipe(input map[string]any) (map[string]any, error) {
	recipeArn, _ := input[keyRecipeArn].(string)

	for _, r := range getBuiltinRecipes() {
		if r[keyRecipeArn] == recipeArn {
			return map[string]any{"recipe": r}, nil
		}
	}

	return nil, fmt.Errorf("%w: recipe %q not found", ErrNotFound, recipeArn)
}

func (h *Handler) listRecipes(input map[string]any) (map[string]any, error) {
	recipes := getBuiltinRecipes()
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	if maxResults <= 0 || maxResults > len(recipes) {
		maxResults = len(recipes)
	}

	// Find start index from nextToken (which is the recipeArn of the next page).
	start := 0
	if nextToken != "" {
		for i, r := range recipes {
			if r[keyRecipeArn] == nextToken {
				start = i

				break
			}
		}
	}

	end := start + maxResults
	var outToken string
	if end < len(recipes) {
		outToken = recipes[end][keyRecipeArn].(string) //nolint:errcheck // keyRecipeArn is always string
	} else {
		end = len(recipes)
	}

	page := recipes[start:end]
	summaries := make([]map[string]any, 0, len(page))
	for _, r := range page {
		summaries = append(summaries, recipeSummaryToMap(r))
	}

	result := map[string]any{"recipes": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// recipeSummaryToMap builds the types.RecipeSummary shape (types.go:1647)
// from a full built-in recipe entry -- no recipeType. domain,
// creationDateTime, and lastUpdatedDateTime are real Summary members, but
// the built-in recipe catalog is a static list with no source for any of
// them, so all three stay absent rather than being fabricated.
func recipeSummaryToMap(r map[string]any) map[string]any {
	return map[string]any{
		keyName:      r[keyName],
		keyRecipeArn: r[keyRecipeArn],
		keyStatus:    r[keyStatus],
	}
}
