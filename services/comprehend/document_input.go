package comprehend

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"slices"
	"unicode/utf8"
)

const fieldBytes = "Bytes"

//nolint:gochecknoglobals // static declarative tables of SDK enum values
var (
	documentReadActions = []string{"TEXTRACT_DETECT_DOCUMENT_TEXT", "TEXTRACT_ANALYZE_DOCUMENT"}
	documentReadModes   = []string{"SERVICE_DEFAULT", "FORCE_DOCUMENT_READ_ACTION"}
	documentFeatures    = []string{"TABLES", "FORMS"}

	binaryDocumentMagics = [][]byte{[]byte("%PDF"), []byte("PK\x03\x04"), {0x89, 'P', 'N', 'G'}, {0xff, 0xd8, 0xff}}
)

// documentBytesText decodes Bytes as a plain-text document; PDF, Word and image
// inputs need Amazon Textract and are rejected.
func documentBytesText(input map[string]any) (string, error) {
	if stringValue(input, fieldText, "") != "" {
		return "", fmt.Errorf("%w: do not use both Text and Bytes", ErrValidation)
	}

	decoded, err := base64.StdEncoding.DecodeString(stringValue(input, fieldBytes, ""))
	if err != nil {
		return "", fmt.Errorf("%w: Bytes is not valid base64: %w", ErrValidation, err)
	}

	for _, magic := range binaryDocumentMagics {
		if bytes.HasPrefix(decoded, magic) {
			return "", fmt.Errorf("%w: PDF, Word and image documents need Textract text extraction", ErrValidation)
		}
	}

	if !utf8.Valid(decoded) {
		return "", fmt.Errorf("%w: Bytes is not a UTF-8 text document", ErrValidation)
	}

	return string(decoded), nil
}

// validateDocumentReaderConfig checks types.DocumentReaderConfig.
func validateDocumentReaderConfig(input map[string]any) error {
	raw, ok := input["DocumentReaderConfig"]
	if !ok || raw == nil {
		return nil
	}

	cfg, isMap := raw.(map[string]any)
	if !isMap {
		return fmt.Errorf("%w: DocumentReaderConfig must be an object", ErrValidation)
	}

	action := stringValue(cfg, "DocumentReadAction", "")
	if action == "" {
		return fmt.Errorf("%w: DocumentReaderConfig.DocumentReadAction is required", ErrValidation)
	}

	if !slices.Contains(documentReadActions, action) {
		return fmt.Errorf("%w: DocumentReadAction %q is not valid", ErrValidation, action)
	}

	if mode := stringValue(cfg, "DocumentReadMode", ""); mode != "" && !slices.Contains(documentReadModes, mode) {
		return fmt.Errorf("%w: DocumentReadMode %q is not valid", ErrValidation, mode)
	}

	features, _ := cfg["FeatureTypes"].([]any)
	for _, f := range features {
		if s, isString := f.(string); !isString || !slices.Contains(documentFeatures, s) {
			return fmt.Errorf("%w: FeatureTypes contains invalid value %v", ErrValidation, f)
		}
	}

	if action == "TEXTRACT_ANALYZE_DOCUMENT" && len(features) == 0 {
		return fmt.Errorf("%w: FeatureTypes is required with TEXTRACT_ANALYZE_DOCUMENT", ErrValidation)
	}

	return nil
}
