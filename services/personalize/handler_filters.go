package personalize

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// --- Filter ---

func (h *Handler) createFilter(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	filterExpression, _ := input["filterExpression"].(string)
	tags := extractTags(input)

	f, err := h.Backend.CreateFilter(name, datasetGroupArn, filterExpression, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyFilterArn: f.FilterArn}, nil
}

func (h *Handler) describeFilter(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input[keyFilterArn].(string)

	f, err := h.Backend.DescribeFilter(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"filter": filterToMap(f)}, nil
}

func (h *Handler) deleteFilter(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input[keyFilterArn].(string)

	return map[string]any{}, h.Backend.DeleteFilter(nameOrArn)
}

func (h *Handler) listFilters(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListFilters(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, f := range list {
		summaries = append(summaries, filterSummaryToMap(f))
	}

	result := map[string]any{"filters": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func filterToMap(f *Filter) map[string]any {
	return map[string]any{
		keyFilterArn:           f.FilterArn,
		keyName:                f.Name,
		keyDatasetGroupArn:     f.DatasetGroupArn,
		"filterExpression":     f.FilterExpression,
		keyStatus:              f.Status,
		keyCreationDateTime:    awstime.Epoch(f.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(f.LastUpdatedDateTime),
	}
}

// filterSummaryToMap builds the types.FilterSummary shape (types.go:1370) --
// no filterExpression. failureReason is a real member but the backend's
// Filter model has no source for it, so it stays absent rather than being
// fabricated.
func filterSummaryToMap(f *Filter) map[string]any {
	return map[string]any{
		keyFilterArn:           f.FilterArn,
		keyName:                f.Name,
		keyDatasetGroupArn:     f.DatasetGroupArn,
		keyStatus:              f.Status,
		keyCreationDateTime:    awstime.Epoch(f.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(f.LastUpdatedDateTime),
	}
}
