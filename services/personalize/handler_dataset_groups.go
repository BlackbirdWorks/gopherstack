package personalize

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// --- DatasetGroup ---

func (h *Handler) createDatasetGroup(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	domain, _ := input["domain"].(string)
	kmsKeyArn, _ := input["kmsKeyArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	tags := extractTags(input)

	dg, err := h.Backend.CreateDatasetGroup(name, domain, kmsKeyArn, roleArn, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyDatasetGroupArn: dg.DatasetGroupArn,
		keyDomain:          dg.Domain,
	}, nil
}

func (h *Handler) describeDatasetGroup(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["datasetGroupArn"].(string)

	dg, err := h.Backend.DescribeDatasetGroup(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"datasetGroup": datasetGroupToMap(dg),
	}, nil
}

func (h *Handler) deleteDatasetGroup(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["datasetGroupArn"].(string)

	return map[string]any{}, h.Backend.DeleteDatasetGroup(nameOrArn)
}

func (h *Handler) listDatasetGroups(input map[string]any) (map[string]any, error) {
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListDatasetGroups(maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, dg := range list {
		summaries = append(summaries, datasetGroupToMap(dg))
	}

	result := map[string]any{"datasetGroups": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func datasetGroupToMap(dg *DatasetGroup) map[string]any {
	return map[string]any{
		keyDatasetGroupArn:     dg.DatasetGroupArn,
		keyName:                dg.Name,
		keyDomain:              dg.Domain,
		"kmsKeyArn":            dg.KmsKeyArn,
		keyRoleArn:             dg.RoleArn,
		keyStatus:              dg.Status,
		keyCreationDateTime:    awstime.Epoch(dg.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(dg.LastUpdatedDateTime),
	}
}
