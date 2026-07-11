package apigateway

// backend_destub.go implements real, stateful backend logic for the
// previously-stubbed operations GetSdkType(s), GetSdk, ImportApiKeys,
// ImportDocumentationParts, and UpdateUsage.

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// SdkType describes a code-generation target supported by GetSdk. AWS
// documents this fixed set directly on the GetSdk API ("Currently java,
// javascript, android, objectivec (for iOS), swift (for iOS), and ruby are
// supported.") — it is not a user-created resource, so the catalog is static.
type SdkType struct {
	ID           string
	FriendlyName string
}

// sdkTypeCatalog returns the fixed set of SDK types API Gateway supports.
// Built fresh on each call (rather than a package-level var) so callers can't
// mutate the shared catalog through the returned slice.
func sdkTypeCatalog() []SdkType {
	return []SdkType{
		{ID: "java", FriendlyName: "Java"},
		{ID: "javascript", FriendlyName: "JavaScript"},
		{ID: "android", FriendlyName: "Android"},
		{ID: "objectivec", FriendlyName: "iOS (Objective-C)"},
		{ID: "swift", FriendlyName: "iOS (Swift)"},
		{ID: "ruby", FriendlyName: "Ruby"},
	}
}

// GetSdkTypes returns the fixed catalog of SDK types.
func (b *InMemoryBackend) GetSdkTypes() []SdkType {
	return sdkTypeCatalog()
}

// GetSdkType looks up a single SDK type by ID from the fixed catalog.
func (b *InMemoryBackend) GetSdkType(id string) (*SdkType, error) {
	for _, t := range sdkTypeCatalog() {
		if t.ID == id {
			cp := t

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: SDK type %s not found", ErrNotFound, id)
}

// SdkExport is the binary SDK package returned by GetSdk.
type SdkExport struct {
	ContentType        string
	ContentDisposition string
	Body               []byte
}

// GetSdk generates an SDK package for the given REST API/stage/language.
// A full per-language code generator is out of scope for an emulator, so the
// package is a real ZIP archive containing the API's actual OpenAPI export
// (reusing the same generator as GetExport) plus a README describing the
// requested SDK type — real API configuration, packaged in the correct
// wire/container format, rather than a fabricated empty blob.
func (b *InMemoryBackend) GetSdk(restAPIID, stageName, sdkType string) (*SdkExport, error) {
	b.mu.RLock("GetSdk")
	defer b.mu.RUnlock()

	api, ok := b.restApis.Get(restAPIID)
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	if !b.stages.Has(stageKey(restAPIID, stageName)) {
		return nil, fmt.Errorf("%w: stage %s not found", ErrStageNotFound, stageName)
	}

	if _, err := b.GetSdkType(sdkType); err != nil {
		return nil, fmt.Errorf("%w: unsupported sdkType %q", ErrInvalidParameter, sdkType)
	}

	ctx := exportContext{b: b, restAPIID: restAPIID, apiName: api.Name}
	spec := buildOAS30Export(ctx, stageName)

	specJSON, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal OpenAPI spec: %w", err)
	}

	readme := fmt.Sprintf(
		"Generated %s SDK for REST API %s, stage %s.\n\n"+
			"This package contains the API's OpenAPI (Swagger) definition as swagger.json.\n",
		sdkType, restAPIID, stageName,
	)

	body, err := buildZipArchive(map[string][]byte{
		"README.txt":   []byte(readme),
		"swagger.json": specJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build SDK archive: %w", err)
	}

	return &SdkExport{
		ContentType:        "application/octet-stream",
		ContentDisposition: fmt.Sprintf(`attachment; filename="%s-%s-sdk.zip"`, restAPIID, sdkType),
		Body:               body,
	}, nil
}

// buildZipArchive packages the given named files into an in-memory ZIP archive.
func buildZipArchive(files map[string][]byte) ([]byte, error) {
	var buf bytes.Buffer

	w := zip.NewWriter(&buf)

	for name, content := range files {
		f, err := w.Create(name)
		if err != nil {
			return nil, err
		}

		if _, werr := f.Write(content); werr != nil {
			return nil, werr
		}
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// apiKeyCSVColumn identifies a recognized column in the AWS API key CSV file
// format.
type apiKeyCSVColumn int

const (
	apiKeyCSVColumnUnknown apiKeyCSVColumn = iota
	apiKeyCSVColumnName
	apiKeyCSVColumnKey
	apiKeyCSVColumnDescription
	apiKeyCSVColumnEnabled
)

// apiKeyCSVColumnFor maps a lowercased header cell to the column it
// represents. AWS's "key" column holds the secret value; "value" is accepted
// as an alias since that's the field name on the API key resource itself.
func apiKeyCSVColumnFor(header string) apiKeyCSVColumn {
	switch header {
	case "name":
		return apiKeyCSVColumnName
	case "key", "value":
		return apiKeyCSVColumnKey
	case "description":
		return apiKeyCSVColumnDescription
	case "enabled":
		return apiKeyCSVColumnEnabled
	default:
		return apiKeyCSVColumnUnknown
	}
}

// apiKeyCSVDataRowOffset is the 1-based line number of the first data row,
// used to report human-readable row numbers in warnings (row 1 is the header).
const apiKeyCSVDataRowOffset = 2

// apiKeyInputFromCSVRow builds a CreateAPIKeyInput from a single CSV data row,
// mapping cells to fields according to the header-derived columns slice.
func apiKeyInputFromCSVRow(rec []string, columns []apiKeyCSVColumn) CreateAPIKeyInput {
	input := CreateAPIKeyInput{Enabled: true}

	for col, cell := range rec {
		if col >= len(columns) {
			break
		}

		value := strings.TrimSpace(cell)

		switch columns[col] {
		case apiKeyCSVColumnName:
			input.Name = value
		case apiKeyCSVColumnKey:
			input.Value = value
		case apiKeyCSVColumnDescription:
			input.Description = value
		case apiKeyCSVColumnEnabled:
			input.Enabled = strings.EqualFold(value, "true")
		case apiKeyCSVColumnUnknown:
			// ignore unrecognized columns
		}
	}

	return input
}

// ImportAPIKeys parses a CSV payload of API keys in AWS's "API Key File
// Format": a header row naming columns (name, key, description, enabled),
// followed by one data row per key. Rows that fail to import are reported as
// warnings rather than aborting the whole batch; when failOnWarnings is true,
// any warning rolls the entire import back and returns an error instead.
func (b *InMemoryBackend) ImportAPIKeys(body []byte, format string, failOnWarnings bool) ([]string, []string, error) {
	if format != "" && !strings.EqualFold(format, "csv") {
		return nil, nil, fmt.Errorf("%w: unsupported format %q (only csv is supported)", ErrInvalidParameter, format)
	}

	reader := csv.NewReader(bytes.NewReader(body))
	reader.FieldsPerRecord = -1

	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to parse CSV: %w", ErrInvalidParameter, err)
	}

	if len(records) == 0 {
		return nil, nil, fmt.Errorf("%w: CSV payload has no header row", ErrInvalidParameter)
	}

	columns := make([]apiKeyCSVColumn, len(records[0]))
	for i, cell := range records[0] {
		columns[i] = apiKeyCSVColumnFor(strings.ToLower(strings.TrimSpace(cell)))
	}

	ids := make([]string, 0, len(records)-1)
	warnings := make([]string, 0)

	for i, rec := range records[1:] {
		input := apiKeyInputFromCSVRow(rec, columns)
		if input.Name == "" {
			continue
		}

		key, cerr := b.CreateAPIKey(input)
		if cerr != nil {
			warnings = append(warnings, "row "+strconv.Itoa(i+apiKeyCSVDataRowOffset)+": "+cerr.Error())

			continue
		}

		ids = append(ids, key.ID)
	}

	if failOnWarnings && len(warnings) > 0 {
		for _, id := range ids {
			_ = b.DeleteAPIKey(id)
		}

		return nil, warnings, fmt.Errorf(
			"%w: import failed with warnings: %s",
			ErrInvalidParameter,
			strings.Join(warnings, "; "),
		)
	}

	return ids, warnings, nil
}

// importDocumentationPartEntry mirrors a single entry of the
// "x-amazon-apigateway-documentation" documentationParts array.
type importDocumentationPartEntry struct {
	Location   DocumentationLocation `json:"location"`
	Properties json.RawMessage       `json:"properties"`
}

// importDocumentationPartsPayload is the expected JSON shape for
// ImportDocumentationParts: a documentationParts array, matching the
// "x-amazon-apigateway-documentation" OpenAPI extension object.
type importDocumentationPartsPayload struct {
	DocumentationParts []importDocumentationPartEntry `json:"documentationParts"`
}

// ImportDocumentationParts parses a documentationParts JSON payload and
// creates a real DocumentationPart for each entry. mode "overwrite" clears all
// existing documentation parts for the API before importing; any other value
// merges the new parts into the existing set. Failures are reported as
// warnings; when failOnWarnings is true, any warning rolls the whole import
// back and returns an error instead.
func (b *InMemoryBackend) ImportDocumentationParts(
	restAPIID string, body []byte, mode string, failOnWarnings bool,
) ([]string, []string, error) {
	var payload importDocumentationPartsPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, nil, fmt.Errorf("%w: failed to parse documentation parts payload: %w", ErrInvalidParameter, err)
	}

	b.mu.Lock("ImportDocumentationParts")

	if !b.restApis.Has(restAPIID) {
		b.mu.Unlock()

		return nil, nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	if strings.EqualFold(mode, "overwrite") {
		for _, p := range append([]*DocumentationPart{}, b.documentationPartsByAPI.Get(restAPIID)...) {
			b.documentationParts.Delete(documentationPartKeyFn(p))
		}
	}

	b.mu.Unlock()

	ids := make([]string, 0, len(payload.DocumentationParts))
	warnings := make([]string, 0)

	for i, entry := range payload.DocumentationParts {
		part, cerr := b.CreateDocumentationPart(CreateDocumentationPartInput{
			RestAPIID:  restAPIID,
			Location:   entry.Location,
			Properties: string(entry.Properties),
		})
		if cerr != nil {
			warnings = append(warnings, "part "+strconv.Itoa(i+1)+": "+cerr.Error())

			continue
		}

		ids = append(ids, part.ID)
	}

	if failOnWarnings && len(warnings) > 0 {
		for _, id := range ids {
			_ = b.DeleteDocumentationPart(restAPIID, id)
		}

		return nil, warnings, fmt.Errorf(
			"%w: import failed with warnings: %s",
			ErrInvalidParameter,
			strings.Join(warnings, "; "),
		)
	}

	return ids, warnings, nil
}

// UpdateUsage validates that the usage plan and API key association exist and
// records a remaining-quota override for that key, so a subsequent GetUsage
// call reflects the change. dateValues holds the request's RFC 6902 patch
// operations after handler.go's normalizePatchBody has already flattened them
// into plain "date -> new remaining quota" entries (mirroring the convention
// every other Update* patch handler in this package uses); non-integer values
// are ignored. Real API Gateway quota-consumption tracking (based on live
// request traffic) isn't modeled by this emulator — as with GetUsage's Items,
// which are always empty absent real traffic — but the override recorded here
// is genuinely read back by GetUsage.
func (b *InMemoryBackend) UpdateUsage(usagePlanID, keyID string, dateValues map[string]string) (*UsageData, error) {
	b.mu.Lock("UpdateUsage")

	if !b.usagePlans.Has(usagePlanID) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, usagePlanID)
	}

	if !b.usagePlanKeys.Has(usagePlanKeyKey(usagePlanID, keyID)) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: usage plan key %s not found", ErrUsagePlanKeyNotFound, keyID)
	}

	for _, v := range dateValues {
		remaining, perr := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if perr != nil {
			continue
		}

		if b.usageOverrides == nil {
			b.usageOverrides = make(map[string]map[string]int64)
		}

		if b.usageOverrides[usagePlanID] == nil {
			b.usageOverrides[usagePlanID] = make(map[string]int64)
		}

		b.usageOverrides[usagePlanID][keyID] = remaining
	}

	b.mu.Unlock()

	return b.GetUsage(GetUsageInput{UsagePlanID: usagePlanID})
}
