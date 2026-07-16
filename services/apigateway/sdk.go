package apigateway

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
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
