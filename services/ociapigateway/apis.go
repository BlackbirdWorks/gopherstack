package ociapigateway

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"

	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	entityAPI = "api"

	wrCreateAPI = "CREATE_API"
	wrUpdateAPI = "UPDATE_API"
	wrDeleteAPI = "DELETE_API"

	resultOK      = "OK"
	resultWarning = "WARNING"
	resultError   = "ERROR"
)

var errNotOpenAPI = errors.New("the API content is not an OpenAPI document")

// API is an API resource: a stored OpenAPI description.
type API struct {
	TimeCreated  time.Time  `json:"timeCreated"`
	TimeUpdated  time.Time  `json:"timeUpdated"`
	Tags         Tags       `json:"tags"`
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	ID           string     `json:"id"`
	Compartment  string     `json:"compartmentId"`
	DisplayName  string     `json:"displayName"`
	State        string     `json:"state"`
	Etag         string     `json:"etag"`
	Content      string     `json:"content,omitempty"`
	Locks        []Lock     `json:"locks,omitempty"`
}

func (i *createAPIIn) setRetryToken(t string) { i.RetryToken = t }

type createAPIIn struct {
	Tags
	RetryToken    string   `json:"-"`
	Content       *string  `json:"content"`
	DisplayName   string   `json:"displayName"`
	CompartmentID string   `json:"compartmentId"`
	Locks         []lockIn `json:"locks"`
}

type updateAPIIn struct {
	Tags
	DisplayName *string `json:"displayName"`
	Content     *string `json:"content"`
}

type apiValidationResult struct {
	Name   string `json:"name"`
	Result string `json:"result"`
}

type apiValidationDetail struct {
	Msg      string `json:"msg"`
	Severity string `json:"severity"`
}

type apiValidationDetails struct {
	Name    string                `json:"name"`
	Result  string                `json:"result"`
	Details []apiValidationDetail `json:"details,omitempty"`
}

// APIWire is the wire form of an API resource (also used for list items).
type APIWire struct {
	TimeCreated       time.Time                 `json:"timeCreated"`
	TimeUpdated       time.Time                 `json:"timeUpdated"`
	DefinedTags       map[string]map[string]any `json:"definedTags,omitempty"`
	FreeformTags      map[string]string         `json:"freeformTags,omitempty"`
	ID                string                    `json:"id"`
	CompartmentID     string                    `json:"compartmentId"`
	DisplayName       string                    `json:"displayName"`
	LifecycleState    string                    `json:"lifecycleState"`
	SpecificationType string                    `json:"specificationType,omitempty"`
	ValidationResults []apiValidationResult     `json:"validationResults,omitempty"`
	Locks             []Lock                    `json:"locks,omitempty"`
}

func apiView(a *API) APIWire {
	w := APIWire{
		TimeCreated: a.TimeCreated, TimeUpdated: a.TimeUpdated, DefinedTags: a.Tags.DefinedTags,
		FreeformTags: a.Tags.FreeformTags, ID: a.ID, CompartmentID: a.Compartment, DisplayName: a.DisplayName,
		LifecycleState: a.State, Locks: a.Locks,
	}

	if a.Content == "" {
		return w
	}

	doc, _ := parseOpenAPI(a.Content)
	w.SpecificationType = specType(doc)

	for _, v := range apiChecks(a.Content) {
		w.ValidationResults = append(w.ValidationResults, apiValidationResult{Name: v.Name, Result: v.Result})
	}

	return w
}

func parseOpenAPI(content string) (map[string]any, error) {
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(content), &doc); err != nil {
		return nil, fmt.Errorf("%w: %w", errNotOpenAPI, err)
	}

	if doc == nil {
		return nil, errNotOpenAPI
	}

	return doc, nil
}

func specType(doc map[string]any) string {
	if v, ok := doc["openapi"].(string); ok && v != "" {
		return "OpenAPI " + v
	}

	if v, ok := doc["swagger"].(string); ok && v != "" {
		return "Swagger " + v
	}

	return ""
}

func openAPIMethods() []string {
	return []string{"get", "put", "post", "delete", "options", "head", "patch"}
}

// apiRoutes turns the OpenAPI paths into deployment routes with stock-response backends.
func apiRoutes(doc map[string]any) []map[string]any {
	paths, _ := doc["paths"].(map[string]any)
	names := make([]string, 0, len(paths))

	for p := range paths {
		names = append(names, p)
	}

	slices.Sort(names)

	routes := []map[string]any{}

	for _, p := range names {
		item, _ := paths[p].(map[string]any)

		var methods []string

		for _, m := range openAPIMethods() {
			if _, ok := item[m]; ok {
				methods = append(methods, strings.ToUpper(m))
			}
		}

		if len(methods) == 0 {
			continue
		}

		routes = append(routes, map[string]any{
			"path": p, "methods": methods,
			"backend": map[string]any{keyType: backendStock, "status": http.StatusOK, "body": ""},
		})
	}

	return routes
}

func apiChecks(content string) []apiValidationDetails {
	doc, err := parseOpenAPI(content)
	if err != nil {
		return []apiValidationDetails{{
			Name: "OpenAPI syntax", Result: resultError,
			Details: []apiValidationDetail{{Msg: err.Error(), Severity: resultError}},
		}}
	}

	out := []apiValidationDetails{{Name: "OpenAPI syntax", Result: resultOK}}

	if specType(doc) == "" {
		out = append(out, apiValidationDetails{
			Name:   "OpenAPI version",
			Result: resultError,
			Details: []apiValidationDetail{
				{Msg: "the document declares neither openapi nor swagger", Severity: resultError},
			},
		})
	} else {
		out = append(out, apiValidationDetails{Name: "OpenAPI version", Result: resultOK})
	}

	if paths, _ := doc["paths"].(map[string]any); len(paths) == 0 {
		out = append(out, apiValidationDetails{
			Name: "Paths", Result: resultWarning,
			Details: []apiValidationDetail{{Msg: "the document defines no paths", Severity: resultWarning}},
		})
	} else {
		out = append(out, apiValidationDetails{Name: "Paths", Result: resultOK})
	}

	spec, _ := json.Marshal(map[string]any{"routes": apiRoutes(doc)}) //nolint:errchkjson // static shape
	if _, cerr := compileSpec(spec); cerr != nil {
		out = append(out, apiValidationDetails{
			Name: "Deployment specification", Result: resultError,
			Details: []apiValidationDetail{{Msg: cerr.Message, Severity: resultError}},
		})
	} else {
		out = append(out, apiValidationDetails{Name: "Deployment specification", Result: resultOK})
	}

	return out
}

func checkAPIContent(content *string) *APIError {
	if content == nil {
		return nil
	}

	if _, err := parseOpenAPI(*content); err != nil {
		return errParam("content is not a parseable OpenAPI (JSON or YAML) document")
	}

	return nil
}

// CreateAPI stores a new API resource.
func (b *InMemoryBackend) CreateAPI(in createAPIIn) (APIWire, string, string, error) {
	b.mu.Lock("CreateAPI")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("CreateAPI", in.RetryToken, b.nowUTC()); ok {
		id, wr := ociretry.Split(v)
		a, live := b.apis[id]
		if !live || a.State == stateDeleting || a.State == stateDeleted {
			return APIWire{}, "", "", errInvalidatedToken()
		}

		return apiView(a), wr, a.Etag, nil
	}

	if err := b.checkTags(in.Tags); err != nil {
		return APIWire{}, "", "", err
	}

	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return APIWire{}, "", "", err
	}

	if err := checkAPIContent(in.Content); err != nil {
		return APIWire{}, "", "", err
	}

	now := b.nowUTC()

	locks, lerr := newLocks(in.Locks, now)
	if lerr != nil {
		return APIWire{}, "", "", lerr
	}

	name := in.DisplayName
	if name == "" {
		name = "api" + now.Format("20060102150405")
	}

	a := &API{
		ID: newOCID("apigatewayapi"), Compartment: in.CompartmentID, DisplayName: name, Locks: locks,
		State: stateCreating, TransitionAt: b.transitionAt(), TimeCreated: now, TimeUpdated: now,
		Etag: newEtag(), Tags: in.Tags,
	}
	if in.Content != nil {
		a.Content = *in.Content
	}

	b.apis[a.ID] = a
	wr := b.newWR(wrCreateAPI, a.Compartment, entityAPI, a.ID, actionCreated, *a.TransitionAt)
	b.retry.Remember("CreateAPI", in.RetryToken, ociretry.Join(a.ID, wr), now)

	return apiView(a), wr, a.Etag, nil
}

func (b *InMemoryBackend) liveAPI(id string) (*API, *APIError) {
	a, ok := b.apis[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return a, nil
}

// GetAPI returns one API resource and its etag.
func (b *InMemoryBackend) GetAPI(id string) (APIWire, string, error) {
	b.mu.RLock("GetAPI")
	defer b.mu.RUnlock()

	a, err := b.liveAPI(id)
	if err != nil {
		return APIWire{}, "", err
	}

	return apiView(a), a.Etag, nil
}

// ListAPIs lists the API resources of a compartment.
func (b *InMemoryBackend) ListAPIs(compartment, name, state string, o ListOpts) (page.Page[APIWire], error) {
	if compartment == "" {
		return page.Page[APIWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListAPIs")
	defer b.mu.RUnlock()

	return listOf(b.apis, o, func(a *API) bool {
		return a.Compartment == compartment && stateMatches(a.State, state) &&
			(name == "" || strs.Equal(a.DisplayName, name))
	}, func(a *API) string { return a.DisplayName }, func(a *API) time.Time { return a.TimeCreated }, apiView), nil
}

func (b *InMemoryBackend) activeAPI(id, ifMatch string, deleting bool) (*API, *APIError) {
	a, err := b.liveAPI(id)
	if err != nil {
		return nil, err
	}

	if a.State != stateActive {
		return nil, errState("api " + id + " is in state " + a.State)
	}

	if err = checkEtag(a.Etag, ifMatch); err != nil {
		return nil, err
	}

	if lerr := checkLocked(a.Locks, deleting); lerr != nil {
		return nil, lerr
	}

	return a, nil
}

// UpdateAPI changes the name, tags or OpenAPI content of an API resource.
func (b *InMemoryBackend) UpdateAPI(id, ifMatch string, in updateAPIIn) (string, error) {
	b.mu.Lock("UpdateAPI")
	defer b.mu.Unlock()

	if err := b.checkTags(in.Tags); err != nil {
		return "", err
	}

	a, err := b.activeAPI(id, ifMatch, false)
	if err != nil {
		return "", err
	}

	if cerr := checkAPIContent(in.Content); cerr != nil {
		return "", cerr
	}

	if in.Content != nil {
		a.Content = *in.Content
	}

	if in.DisplayName != nil && *in.DisplayName != "" {
		a.DisplayName = *in.DisplayName
	}

	a.Tags.merge(in.Tags)
	a.State, a.TransitionAt = stateUpdating, b.transitionAt()
	a.TimeUpdated, a.Etag = b.nowUTC(), newEtag()

	return b.newWR(wrUpdateAPI, a.Compartment, entityAPI, id, actionUpdated, *a.TransitionAt), nil
}

// DeleteAPI starts deleting an API resource.
func (b *InMemoryBackend) DeleteAPI(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteAPI")
	defer b.mu.Unlock()

	a, err := b.activeAPI(id, ifMatch, true)
	if err != nil {
		return "", err
	}

	a.State, a.TransitionAt = stateDeleting, b.transitionAt()
	a.TimeUpdated = b.nowUTC()

	return b.newWR(wrDeleteAPI, a.Compartment, entityAPI, id, actionDeleted, *a.TransitionAt), nil
}

// ChangeAPICompartment moves an API resource.
func (b *InMemoryBackend) ChangeAPICompartment(id, ifMatch string, in changeCompartmentIn) (string, error) {
	b.mu.Lock("ChangeAPICompartment")
	defer b.mu.Unlock()

	a, err := b.liveAPI(id)
	if err != nil {
		return "", err
	}

	return b.move(moveSpec{
		id: id, op: wrUpdateAPI, entity: entityAPI, locks: a.Locks, compartment: &a.Compartment,
		etag: &a.Etag, updated: &a.TimeUpdated,
	}, ifMatch, in.CompartmentID)
}

// APIContent returns the stored OpenAPI document, its etag and base64 SHA-256.
func (b *InMemoryBackend) APIContent(id string) (string, string, string, error) {
	b.mu.RLock("APIContent")
	defer b.mu.RUnlock()

	a, err := b.liveAPI(id)
	if err != nil {
		return "", "", "", err
	}

	sum := sha256.Sum256([]byte(a.Content))

	return a.Content, a.Etag, base64.StdEncoding.EncodeToString(sum[:]), nil
}

// APIDeploymentSpecification generates a deployment specification from the stored OpenAPI paths.
func (b *InMemoryBackend) APIDeploymentSpecification(id string) (json.RawMessage, string, error) {
	b.mu.RLock("APIDeploymentSpecification")
	defer b.mu.RUnlock()

	a, err := b.liveAPI(id)
	if err != nil {
		return nil, "", err
	}

	doc, perr := parseOpenAPI(a.Content)
	if perr != nil {
		return nil, "", errState("api " + id + " has no valid OpenAPI content")
	}

	spec, merr := json.Marshal(map[string]any{"routes": apiRoutes(doc)})
	if merr != nil {
		return nil, "", merr
	}

	return spec, a.Etag, nil
}

// APIValidations reports the validation checks of the stored content.
func (b *InMemoryBackend) APIValidations(id string) (map[string]any, string, error) {
	b.mu.RLock("APIValidations")
	defer b.mu.RUnlock()

	a, err := b.liveAPI(id)
	if err != nil {
		return nil, "", err
	}

	checks := []apiValidationDetails{}
	if a.Content != "" {
		checks = apiChecks(a.Content)
	}

	return map[string]any{"validations": checks}, a.Etag, nil
}

func (h *Handler) createAPI(q *req) error {
	return created(q, "/apis/", func(w APIWire) string { return w.ID }, h.Backend.CreateAPI)
}

func (h *Handler) getAPI(q *req) error {
	w, etag, err := h.Backend.GetAPI(q.param("apiId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listAPIs(q *req) error {
	pg, err := h.Backend.ListAPIs(q.query("compartmentId"), q.query("displayName"),
		q.query("lifecycleState"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) updateAPI(q *req) error {
	return withBody(q, func(in updateAPIIn) (string, error) {
		return h.Backend.UpdateAPI(q.param("apiId"), q.ifMatch(), in)
	})
}

func (h *Handler) deleteAPI(q *req) error {
	wr, err := h.Backend.DeleteAPI(q.param("apiId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeAPICompartment(q *req) error {
	return withBody(q, func(in changeCompartmentIn) (string, error) {
		return h.Backend.ChangeAPICompartment(q.param("apiId"), q.ifMatch(), in)
	})
}

func (h *Handler) getAPIContent(q *req) error {
	content, etag, sum, err := h.Backend.APIContent(q.param("apiId"))
	if err != nil {
		return err
	}

	q.header("etag", etag)
	q.header("x-content-sha256", sum)
	q.header("Content-Type", "application/octet-stream")

	if strings.Contains(q.r.Header.Get("Range"), ",") {
		q.r.Header.Del("Range")
	}

	http.ServeContent(q.w, q.r, "", time.Time{}, strings.NewReader(content))

	return nil
}

func (h *Handler) getAPIDeploymentSpecification(q *req) error {
	spec, etag, err := h.Backend.APIDeploymentSpecification(q.param("apiId"))

	return reply(q, spec, etag, err)
}

func (h *Handler) getAPIValidations(q *req) error {
	v, etag, err := h.Backend.APIValidations(q.param("apiId"))

	return reply(q, v, etag, err)
}
