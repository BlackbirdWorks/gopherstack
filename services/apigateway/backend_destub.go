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

	data, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	if _, stageOK := data.stages[stageName]; !stageOK {
		return nil, fmt.Errorf("%w: stage %s not found", ErrStageNotFound, stageName)
	}

	if _, err := b.GetSdkType(sdkType); err != nil {
		return nil, fmt.Errorf("%w: unsupported sdkType %q", ErrInvalidParameter, sdkType)
	}

	spec := buildOAS30Export(data, stageName)

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

// ImportAPIKeys parses a CSV payload of API keys (AWS's "API Key File Format":
// one "name,value" pair per row, value optional) and creates a real APIKey for
// each row. Rows that fail to import are reported as warnings rather than
// aborting the whole batch; when failOnWarnings is true, any warning rolls the
// entire import back and returns an error instead.
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

	ids := make([]string, 0, len(records))
	warnings := make([]string, 0)

	for i, rec := range records {
		if len(rec) == 0 || strings.TrimSpace(rec[0]) == "" {
			continue
		}

		name := strings.TrimSpace(rec[0])

		value := ""
		if len(rec) > 1 {
			value = strings.TrimSpace(rec[1])
		}

		key, cerr := b.CreateAPIKey(CreateAPIKeyInput{Name: name, Value: value, Enabled: true})
		if cerr != nil {
			warnings = append(warnings, "row "+strconv.Itoa(i+1)+": "+cerr.Error())

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

	d, ok := b.apis[restAPIID]
	if !ok {
		b.mu.Unlock()

		return nil, nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	if strings.EqualFold(mode, "overwrite") {
		d.documentationParts = make(map[string]*DocumentationPart)
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

	if _, ok := b.usagePlans[usagePlanID]; !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, usagePlanID)
	}

	if _, ok := b.usagePlanKeys[usagePlanID][keyID]; !ok {
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
