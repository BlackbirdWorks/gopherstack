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

	result := map[string]any{"recipes": recipes[start:end]}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}
