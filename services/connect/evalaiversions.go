package connect

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// AIVersionLifecycle is the availability window of an evaluation form AI version.
type AIVersionLifecycle struct {
	EndOfLifeTime   *awstime.EpochTime `json:"EndOfLifeTime,omitempty"`
	StartOfLifeTime awstime.EpochTime  `json:"StartOfLifeTime"`
	Status          string             `json:"Status"`
}

// AIVersionSummary is one entry of the evaluation form AI version catalog.
type AIVersionSummary struct {
	AIVersionName      string             `json:"AIVersionName"`
	AIVersionLifecycle AIVersionLifecycle `json:"AIVersionLifecycle"`
}

// AIVersionsRequest is the ListEvaluationFormAIVersions request.
type AIVersionsRequest struct {
	InteractionType string `json:"-" query:"contactInteractionType"`
	NextToken       string `json:"-" query:"nextToken"`
	instPath
	MaxResults int32 `json:"-" query:"maxResults"`
}

const (
	interactionAgent = recAgent
	aiVersionYear    = 2025
	aiVersionRelease = 12
)

func interactionTypes() []string { return []string{interactionAgent, "AUTOMATED", "CUSTOMER"} }

func epochOf(year int, month time.Month, day int) awstime.EpochTime {
	return awstime.NewEpochTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
}

// aiVersionCatalog lists the versions for an interaction type; only the AGENT catalog is documented.
func aiVersionCatalog(interaction string) []AIVersionSummary {
	if interaction != interactionAgent {
		return nil
	}

	retired := epochOf(aiVersionYear, time.December, 1)

	return []AIVersionSummary{
		{
			AIVersionName: "AGENT_EVALUATION_2025-11-12",
			AIVersionLifecycle: AIVersionLifecycle{
				StartOfLifeTime: epochOf(aiVersionYear, time.November, aiVersionRelease),
				Status:          "ACTIVE",
			},
		},
		{
			AIVersionName: "AGENT_EVALUATION_2025-09-01",
			AIVersionLifecycle: AIVersionLifecycle{
				StartOfLifeTime: epochOf(
					aiVersionYear,
					time.September,
					1,
				),
				EndOfLifeTime: &retired,
				Status:        "DEPRECATED",
			},
		},
	}
}

// ListEvaluationFormAIVersions pages through the AI version catalog of an instance for one interaction type.
func (b *Backend) ListEvaluationFormAIVersions(in *AIVersionsRequest) (*map[string]any, error) {
	if err := oneOf("contactInteractionType", in.InteractionType, interactionTypes()...); err != nil {
		return nil, err
	}

	all, err := readInstance(b, "ListEvaluationFormAIVersions", in.InstanceID,
		func(string) ([]AIVersionSummary, error) { return aiVersionCatalog(in.InteractionType), nil })
	if err != nil {
		return nil, err
	}

	if in.MaxResults > maxEvalSearchPage {
		return nil, invalidParamErr("maxResults must be 1-%d", maxEvalSearchPage)
	}

	page, next, err := paged(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return listBody("AIVersionSummaries", orEmpty(page), next), nil
}
