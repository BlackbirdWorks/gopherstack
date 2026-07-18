package translate

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- Terminology ---

func (h *Handler) importTerminology(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	mergeStrategy, _ := input["MergeStrategy"].(string)
	if mergeStrategy != "" && mergeStrategy != "OVERWRITE" {
		return nil, fmt.Errorf("%w: MergeStrategy must be OVERWRITE", ErrValidation)
	}

	description, _ := input["Description"].(string)

	var data *TerminologyData
	if td, ok := input["TerminologyData"].(map[string]any); ok {
		data = &TerminologyData{
			Format:         strField(td, "Format"),
			Directionality: strField(td, "Directionality"),
		}

		// TerminologyData.File is a blob on the wire: the real SDK
		// base64-encodes it before sending (see
		// awsAwsjson11_serializeDocumentTerminologyData in
		// aws-sdk-go-v2/service/translate/serializers.go), so it must be
		// decoded here rather than treated as literal text.
		if fileStr, fok := td["File"].(string); fok {
			decoded, decErr := base64.StdEncoding.DecodeString(fileStr)
			if decErr != nil {
				return nil, fmt.Errorf("%w: TerminologyData.File must be base64-encoded: %w", ErrValidation, decErr)
			}

			data.File = decoded
		}
	}

	if data == nil {
		data = &TerminologyData{Format: "CSV"}
	}

	encKey := extractEncryptionKey(input)
	tags := extractTags(input)

	term, err := h.Backend.ImportTerminology(name, description, data, encKey, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TerminologyProperties": terminologyToMap(term),
	}, nil
}

func (h *Handler) getTerminology(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	term, err := h.Backend.GetTerminology(name)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TerminologyProperties": terminologyToMap(term),
		"TerminologyDataLocation": map[string]any{
			"RepositoryType": "S3",
			"Location":       "s3://gopherstack-translate/terminology/" + name,
		},
	}, nil
}

func (h *Handler) deleteTerminology(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.DeleteTerminology(name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) listTerminologies(input map[string]any) (map[string]any, error) {
	maxResults := maxResultsField(input)
	nextToken, _ := input["NextToken"].(string)
	formatFilter, _ := input["TerminologyDataFormat"].(string)

	list, outToken := h.Backend.ListTerminologies(maxResults, nextToken)

	props := make([]map[string]any, 0, len(list))
	for _, t := range list {
		if formatFilter != "" && !strings.EqualFold(t.Format, formatFilter) {
			continue
		}

		props = append(props, terminologyToMap(t))
	}

	result := map[string]any{
		"TerminologyPropertiesList": props,
	}

	if outToken != "" {
		result["NextToken"] = outToken
	}

	return result, nil
}

func terminologyToMap(t *Terminology) map[string]any {
	targetCodes := t.TargetLanguages
	if targetCodes == nil {
		targetCodes = []string{}
	}

	m := map[string]any{
		"Arn":                  t.ARN,
		keyName:                t.Name,
		"Description":          t.Description,
		"Directionality":       t.Directionality,
		"Format":               t.Format,
		"SizeBytes":            t.SizeBytes,
		"TermCount":            t.TermCount,
		"CreatedAt":            awstime.Epoch(t.CreatedAt),
		"LastUpdatedAt":        awstime.Epoch(t.LastUpdatedAt),
		keySourceLanguageCode:  t.SourceLanguage,
		keyTargetLanguageCodes: targetCodes,
	}

	if t.EncryptionKey != nil {
		m["EncryptionKey"] = map[string]any{
			"Id":   t.EncryptionKey.ID,
			"Type": t.EncryptionKey.Type,
		}
	}

	return m
}
