package apigateway

// import_export.go implements ImportRestApi and PutRestApi: parsing an
// OpenAPI 3 / Swagger 2 document (JSON or YAML) and using it to create or
// update a real RestApi, including deriving its initial resource/method tree
// from the document's "paths" object.

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// openAPIHTTPMethod maps an OpenAPI/Swagger path-item key to the HTTP method
// name used by the corresponding API Gateway Method resource. Non-method
// path-item fields (e.g. "parameters", "summary", vendor extensions other
// than the ANY-method extension) are intentionally not recognized here and
// are skipped during parsing.
func openAPIHTTPMethod(key string) (string, bool) {
	switch strings.ToLower(key) {
	case "get":
		return "GET", true
	case "put":
		return "PUT", true
	case "post":
		return "POST", true
	case "delete":
		return "DELETE", true
	case "options":
		return "OPTIONS", true
	case "head":
		return "HEAD", true
	case "patch":
		return "PATCH", true
	case "trace":
		return "TRACE", true
	case "x-amazon-apigateway-any-method":
		return "ANY", true
	default:
		return "", false
	}
}

// openAPISpec is the subset of an OpenAPI 3 / Swagger 2 document this
// emulator derives RestApi metadata and a resource/method tree from.
type openAPISpec struct {
	// Paths maps a document path (e.g. "/pets/{petId}") to the set of HTTP
	// methods declared for it.
	Paths       map[string]map[string]bool
	Title       string
	Description string
}

// parseOpenAPISpec parses an OpenAPI or Swagger document in JSON or YAML
// form. JSON is a subset of YAML, so a single YAML decode handles both.
func parseOpenAPISpec(body []byte) (*openAPISpec, error) {
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil, fmt.Errorf("%w: API definition body is required", ErrInvalidParameter)
	}

	var raw map[string]any
	if err := yaml.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("%w: invalid API definition: %s", ErrInvalidParameter, err.Error())
	}

	spec := &openAPISpec{Paths: make(map[string]map[string]bool)}
	populateOpenAPIInfo(spec, raw)
	populateOpenAPIPaths(spec, raw)

	return spec, nil
}

// populateOpenAPIInfo extracts the title/description from the document's
// "info" object, when present.
func populateOpenAPIInfo(spec *openAPISpec, raw map[string]any) {
	info, isObj := raw["info"].(map[string]any)
	if !isObj {
		return
	}

	if title, isStr := info["title"].(string); isStr {
		spec.Title = title
	}

	if desc, isStr := info["description"].(string); isStr {
		spec.Description = desc
	}
}

// populateOpenAPIPaths extracts the set of HTTP methods declared per path
// from the document's "paths" object.
func populateOpenAPIPaths(spec *openAPISpec, raw map[string]any) {
	paths, isObj := raw["paths"].(map[string]any)
	if !isObj {
		return
	}

	for path, item := range paths {
		methods, itemIsObj := item.(map[string]any)
		if !itemIsObj {
			continue
		}

		set := make(map[string]bool)

		for key := range methods {
			if verb, known := openAPIHTTPMethod(key); known {
				set[verb] = true
			}
		}

		if len(set) > 0 {
			spec.Paths[path] = set
		}
	}
}

// ImportRestAPI creates a brand-new RestApi from an OpenAPI/Swagger document
// (JSON or YAML), taking its name/description from the document's info
// object and building its initial resource/method tree from the document's
// paths object. body is the raw document bytes; parsing happens here so that
// StorageBackend's method signature does not need to expose the internal
// openAPISpec representation to callers outside this package.
func (b *InMemoryBackend) ImportRestAPI(body []byte) (*RestAPI, error) {
	spec, err := parseOpenAPISpec(body)
	if err != nil {
		return nil, err
	}

	name := spec.Title
	if name == "" {
		name = "ImportedApi"
	}

	api, err := b.CreateRestAPI(CreateRestAPIInput{Name: name, Description: spec.Description})
	if err != nil {
		return nil, err
	}

	if applyErr := b.applyOpenAPIPaths(api.ID, spec, true); applyErr != nil {
		return nil, applyErr
	}

	return b.GetRestAPI(api.ID)
}

// PutRestAPI updates an existing RestApi from an OpenAPI/Swagger document
// (JSON or YAML). In "overwrite" mode the existing resource tree is replaced
// outright with the one derived from the document; in "merge" mode (the
// default, and any value other than "overwrite") new paths/methods from the
// document are added alongside the API's existing resources without removing
// anything. body is the raw document bytes; see ImportRestAPI for why
// parsing happens here rather than in the caller.
func (b *InMemoryBackend) PutRestAPI(restAPIID string, body []byte, mode string) (*RestAPI, error) {
	if restAPIID == "" {
		return nil, fmt.Errorf("%w: restApiId is required", ErrInvalidParameter)
	}

	spec, err := parseOpenAPISpec(body)
	if err != nil {
		return nil, err
	}

	overwrite := mode == "overwrite"

	var update UpdateRestAPIInput
	if spec.Title != "" {
		update.Name = spec.Title
	}

	if spec.Description != "" {
		update.Description = spec.Description
	}

	if _, updateErr := b.UpdateRestAPI(restAPIID, update); updateErr != nil {
		return nil, updateErr
	}

	if applyErr := b.applyOpenAPIPaths(restAPIID, spec, overwrite); applyErr != nil {
		return nil, applyErr
	}

	return b.GetRestAPI(restAPIID)
}

// applyOpenAPIPaths creates resources and methods for restAPIID from spec's
// paths. When overwrite is true, all non-root resources are removed first so
// the resulting tree exactly matches the document; otherwise existing
// resources/methods are left untouched and only new ones are added.
func (b *InMemoryBackend) applyOpenAPIPaths(restAPIID string, spec *openAPISpec, overwrite bool) error {
	b.mu.Lock("applyOpenAPIPaths")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	rootID := findRootResourceIDLocked(d)
	if rootID == "" {
		return fmt.Errorf("%w: REST API %s has no root resource", ErrRestAPINotFound, restAPIID)
	}

	if overwrite {
		for id := range d.resources {
			if id != rootID {
				delete(d.resources, id)
			}
		}
	}

	paths := make([]string, 0, len(spec.Paths))
	for p := range spec.Paths {
		paths = append(paths, p)
	}

	sort.Strings(paths)

	for _, path := range paths {
		applyOpenAPIPath(d, rootID, path, spec.Paths[path], overwrite)
	}

	return nil
}

// applyOpenAPIPath ensures the resource chain for path exists and attaches
// the given HTTP methods to its terminal resource.
func applyOpenAPIPath(d *apiData, rootID, path string, methods map[string]bool, overwrite bool) {
	resID := ensureResourcePathLocked(d, rootID, path)
	res := d.resources[resID]

	for verb := range methods {
		if _, exists := res.ResourceMethods[verb]; exists && !overwrite {
			continue
		}

		res.ResourceMethods[verb] = &Method{
			HTTPMethod:        verb,
			AuthorizationType: "NONE",
			RequestParameters: make(map[string]bool),
			MethodResponses:   make(map[string]*MethodResponse),
		}
	}
}

// findRootResourceIDLocked returns the ID of d's root resource (the one with
// no parent). Must be called with the backend lock held.
func findRootResourceIDLocked(d *apiData) string {
	for id, r := range d.resources {
		if r.ParentID == "" {
			return id
		}
	}

	return ""
}

// ensureResourcePathLocked walks (creating as needed) the resource chain for
// path (e.g. "/pets/{petId}") under rootID, reusing any resource that already
// has a matching parent and pathPart, and returns the ID of the resource at
// the end of the path. Must be called with the backend lock held.
func ensureResourcePathLocked(d *apiData, rootID, path string) string {
	parentID := rootID
	parentPath := d.resources[rootID].Path

	for part := range strings.SplitSeq(strings.Trim(path, "/"), "/") {
		if part == "" {
			continue
		}

		childID := findChildByPathPartLocked(d, parentID, part)
		if childID == "" {
			childID = randomID(resourceIDLength)
			d.resources[childID] = &Resource{
				ID:              childID,
				ParentID:        parentID,
				PathPart:        part,
				Path:            computePath(parentPath, part),
				RestAPIID:       d.api.ID,
				ResourceMethods: make(map[string]*Method),
			}
		}

		parentID = childID
		parentPath = d.resources[childID].Path
	}

	return parentID
}

// findChildByPathPartLocked returns the ID of parentID's child resource with
// the given pathPart, or "" if none exists. Must be called with the backend
// lock held.
func findChildByPathPartLocked(d *apiData, parentID, pathPart string) string {
	for id, r := range d.resources {
		if r.ParentID == parentID && r.PathPart == pathPart {
			return id
		}
	}

	return ""
}
