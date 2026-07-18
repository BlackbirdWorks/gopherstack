package translate

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- Parallel Data ---

func (h *Handler) createParallelData(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	description, _ := input["Description"].(string)
	cfg := extractParallelDataConfig(input)
	encKey := extractEncryptionKey(input)
	tags := extractTags(input)

	pd, err := h.Backend.CreateParallelData(name, description, cfg, encKey, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyName:   pd.Name,
		keyStatus: pd.Status,
	}, nil
}

func (h *Handler) getParallelData(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	pd, err := h.Backend.GetParallelData(name)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ParallelDataProperties": parallelDataToMap(pd),
		"DataLocation": map[string]any{
			"RepositoryType": "S3",
			"Location":       "s3://gopherstack-translate/parallel-data/" + name,
		},
	}, nil
}

func (h *Handler) updateParallelData(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	description, _ := input["Description"].(string)
	cfg := extractParallelDataConfig(input)

	pd, err := h.Backend.UpdateParallelData(name, description, cfg)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyName:                     pd.Name,
		keyStatus:                   pd.Status,
		"LatestUpdateAttemptStatus": "ACTIVE",
		"LatestUpdateAttemptAt":     awstime.Epoch(pd.LastUpdatedAt),
	}, nil
}

func (h *Handler) deleteParallelData(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	pd, err := h.Backend.DeleteParallelData(name)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyName:   pd.Name,
		keyStatus: pd.Status,
	}, nil
}

func (h *Handler) listParallelData(input map[string]any) (map[string]any, error) {
	maxResults := maxResultsField(input)
	nextToken, _ := input["NextToken"].(string)

	list, outToken := h.Backend.ListParallelData(maxResults, nextToken)

	props := make([]map[string]any, 0, len(list))
	for _, pd := range list {
		props = append(props, parallelDataToMap(pd))
	}

	result := map[string]any{
		"ParallelDataPropertiesList": props,
	}

	if outToken != "" {
		result["NextToken"] = outToken
	}

	return result, nil
}

func parallelDataToMap(pd *ParallelData) map[string]any {
	m := map[string]any{
		"Arn":                  pd.ARN,
		keyName:                pd.Name,
		"Description":          pd.Description,
		keyStatus:              pd.Status,
		keySourceLanguageCode:  pd.SourceLanguage,
		keyTargetLanguageCodes: pd.TargetLanguages,
		"CreatedAt":            awstime.Epoch(pd.CreatedAt),
		"LastUpdatedAt":        awstime.Epoch(pd.LastUpdatedAt),
	}

	if pd.ParallelDataConfig != nil {
		m["ParallelDataConfig"] = map[string]any{
			"S3Uri":  pd.ParallelDataConfig.S3URI,
			"Format": pd.ParallelDataConfig.Format,
		}
	}

	return m
}

func extractParallelDataConfig(input map[string]any) *ParallelDataConfig {
	cfg, ok := input["ParallelDataConfig"].(map[string]any)
	if !ok {
		return nil
	}

	return &ParallelDataConfig{
		S3URI:  strField(cfg, "S3Uri"),
		Format: strField(cfg, "Format"),
	}
}
