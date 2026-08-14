// Package dynamodb implements the AWS DynamoDB mock service.
// handler_contributor_insights.go implements the wire-JSON handlers for the
// contributor insights family. Routing (dispatchExtraOps) stays in
// handler.go; these are the leaf implementations it calls into. Backend
// logic lives in contributor_insights.go.
package dynamodb

import (
	"context"
	"encoding/json"

	sdkDDB "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

type describeContributorInsightsInput struct {
	TableName string `json:"TableName"`
	IndexName string `json:"IndexName,omitempty"`
}

type describeContributorInsightsOutput struct {
	TableName                   string   `json:"TableName,omitempty"`
	IndexName                   string   `json:"IndexName,omitempty"`
	ContributorInsightsStatus   string   `json:"ContributorInsightsStatus,omitempty"`
	ContributorInsightsMode     string   `json:"ContributorInsightsMode,omitempty"`
	ContributorInsightsRuleList []string `json:"ContributorInsightsRuleList"`
}

func (h *DynamoDBHandler) handleDescribeContributorInsights(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req describeContributorInsightsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	input := &sdkDDB.DescribeContributorInsightsInput{TableName: &req.TableName}
	if req.IndexName != "" {
		input.IndexName = &req.IndexName
	}

	out, err := h.Backend.DescribeContributorInsights(ctx, input)
	if err != nil {
		return nil, err
	}

	wire := &describeContributorInsightsOutput{
		TableName:                   ptrconv.String(out.TableName),
		ContributorInsightsStatus:   string(out.ContributorInsightsStatus),
		ContributorInsightsMode:     string(out.ContributorInsightsMode),
		ContributorInsightsRuleList: out.ContributorInsightsRuleList,
	}

	if out.IndexName != nil {
		wire.IndexName = *out.IndexName
	}

	return wire, nil
}

// --- ListContributorInsights handler ---

type listContributorInsightsInput struct {
	TableName  string `json:"TableName,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

type contributorInsightsSummaryWire struct {
	TableName                 string `json:"TableName,omitempty"`
	IndexName                 string `json:"IndexName,omitempty"`
	ContributorInsightsStatus string `json:"ContributorInsightsStatus,omitempty"`
	ContributorInsightsMode   string `json:"ContributorInsightsMode,omitempty"`
}

type listContributorInsightsOutput struct {
	NextToken                    string                           `json:"NextToken,omitempty"`
	ContributorInsightsSummaries []contributorInsightsSummaryWire `json:"ContributorInsightsSummaries"`
}

func (h *DynamoDBHandler) handleListContributorInsights(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req listContributorInsightsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	input := &sdkDDB.ListContributorInsightsInput{MaxResults: req.MaxResults}
	if req.TableName != "" {
		input.TableName = &req.TableName
	}
	if req.NextToken != "" {
		input.NextToken = &req.NextToken
	}

	out, err := h.Backend.ListContributorInsights(ctx, input)
	if err != nil {
		return nil, err
	}

	summaries := make([]contributorInsightsSummaryWire, 0, len(out.ContributorInsightsSummaries))
	for _, s := range out.ContributorInsightsSummaries {
		summaries = append(summaries, contributorInsightsSummaryWire{
			TableName:                 ptrconv.String(s.TableName),
			IndexName:                 ptrconv.String(s.IndexName),
			ContributorInsightsStatus: string(s.ContributorInsightsStatus),
			ContributorInsightsMode:   string(s.ContributorInsightsMode),
		})
	}

	return &listContributorInsightsOutput{
		ContributorInsightsSummaries: summaries,
		NextToken:                    ptrconv.String(out.NextToken),
	}, nil
}

// --- UpdateContributorInsights handler ---

type updateContributorInsightsInput struct {
	TableName                 string `json:"TableName"`
	IndexName                 string `json:"IndexName,omitempty"`
	ContributorInsightsAction string `json:"ContributorInsightsAction"`
	ContributorInsightsMode   string `json:"ContributorInsightsMode,omitempty"`
}

type updateContributorInsightsOutput struct {
	TableName                 string `json:"TableName,omitempty"`
	IndexName                 string `json:"IndexName,omitempty"`
	ContributorInsightsStatus string `json:"ContributorInsightsStatus,omitempty"`
	ContributorInsightsMode   string `json:"ContributorInsightsMode,omitempty"`
}

func (h *DynamoDBHandler) handleUpdateContributorInsights(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req updateContributorInsightsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	sdkInput := &sdkDDB.UpdateContributorInsightsInput{
		TableName:                 &req.TableName,
		ContributorInsightsAction: types.ContributorInsightsAction(req.ContributorInsightsAction),
		ContributorInsightsMode:   types.ContributorInsightsMode(req.ContributorInsightsMode),
	}

	if req.IndexName != "" {
		sdkInput.IndexName = &req.IndexName
	}

	out, err := h.Backend.UpdateContributorInsights(ctx, sdkInput)
	if err != nil {
		return nil, err
	}

	return &updateContributorInsightsOutput{
		TableName:                 ptrconv.String(out.TableName),
		IndexName:                 ptrconv.String(out.IndexName),
		ContributorInsightsStatus: string(out.ContributorInsightsStatus),
		ContributorInsightsMode:   string(out.ContributorInsightsMode),
	}, nil
}
