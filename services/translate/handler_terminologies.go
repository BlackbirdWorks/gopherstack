package translate

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// terminologyFileMaxBytes is the maximum size of an imported custom
// terminology file: 10 MB (10,485,760 bytes), confirmed against the
// TerminologyFile blob shape's "max" in the smithy model
// (aws-sdk-go@v1.55.5/models/apis/translate/2017-07-01/api-2.json) and the
// Amazon Translate guidelines/quotas page's "Maximum custom terminology file
// size: 10 MB".
const terminologyFileMaxBytes = 10 * 1024 * 1024

// --- Terminology ---
//
// ImportTerminology, GetTerminology, DeleteTerminology, and ListTerminologies
// have no InvalidRequestException in their modeled error list (confirmed
// against api-2.json's per-operation "errors" arrays) -- only
// InvalidParameterValueException -- so request-validation failures on these
// four operations use ErrInvalidParameter, not ErrValidation.

// extractTerminologyData parses the request's TerminologyData member,
// decoding its File blob (base64 on the wire, see
// awsAwsjson11_serializeDocumentTerminologyData in
// aws-sdk-go-v2/service/translate/serializers.go) and enforcing the
// terminologyFileMaxBytes quota. A request that omits TerminologyData
// entirely returns (nil, nil): TerminologyData is a required top-level
// member of ImportTerminologyRequest (api-2.json), and leaving data nil here
// (rather than silently defaulting to an empty CSV terminology) lets the
// backend's existing "TerminologyData is required" check actually fire
// instead of being permanently unreachable.
func extractTerminologyData(input map[string]any) (*TerminologyData, error) {
	td, ok := input["TerminologyData"].(map[string]any)
	if !ok {
		return nil, nil //nolint:nilnil // absent-vs-invalid is a real distinction the caller acts on
	}

	data := &TerminologyData{
		Format:         strField(td, "Format"),
		Directionality: strField(td, "Directionality"),
	}

	fileStr, fok := td["File"].(string)
	if !fok {
		return data, nil
	}

	decoded, decErr := base64.StdEncoding.DecodeString(fileStr)
	if decErr != nil {
		return nil, fmt.Errorf("%w: TerminologyData.File must be base64-encoded: %w", ErrInvalidParameter, decErr)
	}

	if len(decoded) > terminologyFileMaxBytes {
		return nil, fmt.Errorf(
			"%w: TerminologyData.File exceeds the %d byte limit",
			ErrLimitExceeded,
			terminologyFileMaxBytes,
		)
	}

	data.File = decoded

	return data, nil
}

func (h *Handler) importTerminology(input map[string]any) (map[string]any, error) {
	name, _ := input[keyName].(string)
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	mergeStrategy, _ := input["MergeStrategy"].(string)
	if mergeStrategy != "" && mergeStrategy != "OVERWRITE" {
		return nil, fmt.Errorf("%w: MergeStrategy must be OVERWRITE", ErrInvalidParameter)
	}

	description, _ := input["Description"].(string)

	data, err := extractTerminologyData(input)
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
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
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
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
