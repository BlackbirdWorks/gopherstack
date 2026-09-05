package eventbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// schemasRESTLimit parses the "limit" query param (schemas@v1.37.4
// serializers.go: encoder.SetQuery("limit").Integer(*v.Limit)). An absent or
// unparseable value returns 0, which paginateN treats as its default page size.
func schemasRESTLimit(q url.Values) int {
	n, _ := strconv.Atoi(q.Get("limit"))

	return n
}

// schemasRESTContentType is the real schemas@v1.37.4 REST-JSON1 wire content
// type (serializers.go: restEncoder.SetHeader("Content-Type").String("application/json")
// on every op that has a body).
const schemasRESTContentType = "application/json"

// Real Schemas operation names (schemas@v1.37.4), factored into constants so
// each name is a single source-level literal shared between the fabricated
// JSON-RPC dispatch table (handler_dispatch.go's GetSupportedOperations,
// handler_registries.go's registryActions, handler_schemas.go's
// schemaActions/schemaVersionActions/codeBindingActions) and this file's
// REST-JSON1 routing, rather than the same string repeated at each site
// (goconst).
const (
	opCreateRegistry   = "CreateRegistry"
	opDeleteRegistry   = "DeleteRegistry"
	opDescribeRegistry = "DescribeRegistry"
	opListRegistries   = "ListRegistries"
	opUpdateRegistry   = "UpdateRegistry"

	opCreateSchema   = "CreateSchema"
	opDeleteSchema   = "DeleteSchema"
	opDescribeSchema = "DescribeSchema"
	opListSchemas    = "ListSchemas"
	opSearchSchemas  = "SearchSchemas"
	opUpdateSchema   = "UpdateSchema"

	opListSchemaVersions  = "ListSchemaVersions"
	opDeleteSchemaVersion = "DeleteSchemaVersion"
	opGetDiscoveredSchema = "GetDiscoveredSchema"

	opPutCodeBinding       = "PutCodeBinding"
	opDescribeCodeBinding  = "DescribeCodeBinding"
	opGetCodeBindingSource = "GetCodeBindingSource"
)

// schemasRouteKind identifies which of the real Schemas REST path templates
// (schemas@v1.37.4 serializers.go's httpbinding.SplitURI arguments) a
// request's path matches, independent of HTTP method.
type schemasRouteKind int

const (
	schemasRouteNone schemasRouteKind = iota
	schemasRouteRegistries
	schemasRouteRegistry
	schemasRouteSchemas
	schemasRouteSchemasSearch
	schemasRouteSchema
	schemasRouteSchemaVersions
	schemasRouteSchemaVersion
	schemasRouteCodeBinding
	schemasRouteCodeBindingSrc
	schemasRouteDiscover
)

// schemasPathMatch is the result of matching a request path against the real
// Schemas REST path templates, carrying whichever URI-bound identifiers that
// template declares (schemas@v1.37.4 serializers.go's encoder.SetURI calls).
type schemasPathMatch struct {
	registryName  string
	schemaName    string
	schemaVersion string
	language      string
	kind          schemasRouteKind
}

func splitSchemasPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}

	return strings.Split(trimmed, "/")
}

// Path segment counts for each real Schemas REST template, e.g.
// "/v1/registries/name/{RegistryName}" splits to
// ["v1","registries","name","{RegistryName}"], 4 segments.
const (
	segCountRegistriesOrDiscover       = 2
	segCountRegistry                   = 4
	segCountSchemas                    = 5
	segCountSchemasSearch              = 6
	segCountSchema                     = 7
	segCountSchemaVersionsList         = 8
	segCountSchemaVersionOrCodeBinding = 9
	segCountCodeBindingSource          = 10
)

// parseSchemasPath matches path against the real schemas@v1.37.4 REST path
// templates for the 17 ops this file routes (see sdkRouteCases in
// handler_schemas_rest_route_table_test.go for the full template list cited
// against serializers.go). It does not itself consider HTTP method --
// schemasOpForRoute combines the returned kind with the method.
func parseSchemasPath(path string) (schemasPathMatch, bool) {
	segs := splitSchemasPath(path)
	if len(segs) < segCountRegistriesOrDiscover || segs[0] != "v1" {
		return schemasPathMatch{}, false
	}

	switch {
	case len(segs) == segCountRegistriesOrDiscover && segs[1] == "registries":
		return schemasPathMatch{kind: schemasRouteRegistries}, true
	case len(segs) == segCountRegistriesOrDiscover && segs[1] == "discover":
		return schemasPathMatch{kind: schemasRouteDiscover}, true
	case len(segs) >= segCountRegistry && segs[1] == "registries" && segs[2] == "name":
		return parseSchemasRegistryPath(segs)
	default:
		return schemasPathMatch{}, false
	}
}

func parseSchemasRegistryPath(segs []string) (schemasPathMatch, bool) {
	registryName := segs[3]
	if len(segs) == segCountRegistry {
		return schemasPathMatch{kind: schemasRouteRegistry, registryName: registryName}, true
	}

	if len(segs) >= segCountSchemas && segs[4] == "schemas" {
		return parseSchemasSchemasPath(segs, registryName)
	}

	return schemasPathMatch{}, false
}

func parseSchemasSchemasPath(segs []string, registryName string) (schemasPathMatch, bool) {
	if len(segs) == segCountSchemas {
		return schemasPathMatch{kind: schemasRouteSchemas, registryName: registryName}, true
	}

	if len(segs) == segCountSchemasSearch && segs[5] == "search" {
		return schemasPathMatch{kind: schemasRouteSchemasSearch, registryName: registryName}, true
	}

	if len(segs) >= segCountSchema && segs[5] == "name" {
		return parseSchemasSchemaPath(segs, registryName, segs[6])
	}

	return schemasPathMatch{}, false
}

func parseSchemasSchemaPath(
	segs []string,
	registryName, schemaName string,
) (schemasPathMatch, bool) {
	if len(segs) == segCountSchema {
		return schemasPathMatch{
			kind:         schemasRouteSchema,
			registryName: registryName,
			schemaName:   schemaName,
		}, true
	}

	if len(segs) == segCountSchemaVersionsList && segs[7] == "versions" {
		return schemasPathMatch{
			kind: schemasRouteSchemaVersions, registryName: registryName, schemaName: schemaName,
		}, true
	}

	if len(segs) == segCountSchemaVersionOrCodeBinding && segs[7] == "version" {
		return schemasPathMatch{
			kind: schemasRouteSchemaVersion, registryName: registryName, schemaName: schemaName,
			schemaVersion: segs[8],
		}, true
	}

	if len(segs) >= segCountSchemaVersionOrCodeBinding && segs[7] == "language" {
		return parseSchemasCodeBindingPath(segs, registryName, schemaName, segs[8])
	}

	return schemasPathMatch{}, false
}

// schemasPathSegSource is the literal final path segment of
// GetCodeBindingSource's URI template. Named to avoid a 3rd bare "source"
// literal alongside delivery.go's two (goconst counts by string value across
// the whole package, not by meaning).
const schemasPathSegSource = "source"

func parseSchemasCodeBindingPath(
	segs []string,
	registryName, schemaName, language string,
) (schemasPathMatch, bool) {
	if len(segs) == segCountSchemaVersionOrCodeBinding {
		return schemasPathMatch{
			kind: schemasRouteCodeBinding, registryName: registryName, schemaName: schemaName, language: language,
		}, true
	}

	if len(segs) == segCountCodeBindingSource && segs[9] == schemasPathSegSource {
		return schemasPathMatch{
			kind: schemasRouteCodeBindingSrc, registryName: registryName, schemaName: schemaName, language: language,
		}, true
	}

	return schemasPathMatch{}, false
}

// schemasOpForRoute resolves a (kind, method) pair to the real Schemas
// operation name, per schemas@v1.37.4 serializers.go's request.Method for
// each op sharing that kind's path template.
func schemasOpForRoute(kind schemasRouteKind, method string) string {
	switch kind {
	case schemasRouteRegistries:
		return schemasOpForRegistriesList(method)
	case schemasRouteRegistry:
		return schemasOpForRegistry(method)
	case schemasRouteSchemas:
		return schemasOpForSchemasList(method)
	case schemasRouteSchemasSearch:
		return schemasOpForSchemasSearch(method)
	case schemasRouteSchema:
		return schemasOpForSchema(method)
	case schemasRouteSchemaVersions:
		return schemasOpForSchemaVersionsList(method)
	case schemasRouteSchemaVersion:
		return schemasOpForSchemaVersion(method)
	case schemasRouteCodeBinding:
		return schemasOpForCodeBinding(method)
	case schemasRouteCodeBindingSrc:
		return schemasOpForCodeBindingSource(method)
	case schemasRouteDiscover:
		return schemasOpForDiscover(method)
	case schemasRouteNone:
		return ""
	default:
		return ""
	}
}

func schemasOpForRegistriesList(method string) string {
	if method == http.MethodGet {
		return opListRegistries
	}

	return ""
}

func schemasOpForRegistry(method string) string {
	switch method {
	case http.MethodPost:
		return opCreateRegistry
	case http.MethodDelete:
		return opDeleteRegistry
	case http.MethodGet:
		return opDescribeRegistry
	case http.MethodPut:
		return opUpdateRegistry
	default:
		return ""
	}
}

func schemasOpForSchemasList(method string) string {
	if method == http.MethodGet {
		return opListSchemas
	}

	return ""
}

func schemasOpForSchemasSearch(method string) string {
	if method == http.MethodGet {
		return opSearchSchemas
	}

	return ""
}

func schemasOpForSchema(method string) string {
	switch method {
	case http.MethodPost:
		return opCreateSchema
	case http.MethodDelete:
		return opDeleteSchema
	case http.MethodGet:
		return opDescribeSchema
	case http.MethodPut:
		return opUpdateSchema
	default:
		return ""
	}
}

func schemasOpForSchemaVersionsList(method string) string {
	if method == http.MethodGet {
		return opListSchemaVersions
	}

	return ""
}

func schemasOpForSchemaVersion(method string) string {
	if method == http.MethodDelete {
		return opDeleteSchemaVersion
	}

	return ""
}

func schemasOpForCodeBinding(method string) string {
	switch method {
	case http.MethodPost:
		return opPutCodeBinding
	case http.MethodGet:
		return opDescribeCodeBinding
	default:
		return ""
	}
}

func schemasOpForCodeBindingSource(method string) string {
	if method == http.MethodGet {
		return opGetCodeBindingSource
	}

	return ""
}

func schemasOpForDiscover(method string) string {
	if method == http.MethodPost {
		return opGetDiscoveredSchema
	}

	return ""
}

// schemasRESTOpForRequest returns the real Schemas operation name for a
// request's method+path, or "" if it matches none of the 17 REST templates
// this file routes. Used by RouteMatcher, ExtractOperation, and Handler().
func schemasRESTOpForRequest(r *http.Request) string {
	m, ok := parseSchemasPath(r.URL.Path)
	if !ok {
		return ""
	}

	return schemasOpForRoute(m.kind, r.Method)
}

// --- REST wire-shape request bodies ---
//
// These decode the real schemas@v1.37.4 JSON body field names (serializers.go's
// awsRestjson1_serializeOpDocument<Op>Input functions), notably "tags"
// lowercase -- NOT "Tags" -- unlike this package's own internal
// CreateRegistryInput/CreateSchemaInput wire, which existing fabricated-path
// tests already depend on using "Tags".

type schemasCreateRegistryBodyREST struct {
	Tags        map[string]string `json:"tags,omitempty"`
	Description string            `json:"Description,omitempty"`
}

type schemasUpdateRegistryBodyREST struct {
	Description string `json:"Description,omitempty"`
}

type schemasCreateSchemaBodyREST struct {
	Content     string            `json:"Content"`
	Description string            `json:"Description,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Type        string            `json:"Type"`
}

type schemasUpdateSchemaBodyREST struct {
	ClientTokenID string `json:"ClientTokenId,omitempty"`
	Content       string `json:"Content,omitempty"`
	Description   string `json:"Description,omitempty"`
	Type          string `json:"Type,omitempty"`
}

type schemasGetDiscoveredBodyREST struct {
	Type   string   `json:"Type"`
	Events []string `json:"Events"`
}

// --- REST wire-shape responses ---
//
// Field sets verified against schemas@v1.37.4 deserializers.go's
// awsRestjson1_deserializeOpDocument<Op>Output / ...DocumentTags /
// ...DocumentRegistrySummary / ...DocumentSchemaSummary /
// ...DocumentSchemaVersionSummary / ...DocumentSearchSchemaSummary /
// ...DocumentSearchSchemaVersionSummary functions -- each is a DIFFERENT,
// narrower shape than this package's own internal SchemaRegistry/Schema/
// SchemaVersion structs (which this REST path does not reuse for output,
// only as an intermediate value from the shared backend calls).

type registryRESTOutput struct {
	Tags         map[string]string `json:"tags,omitempty"`
	Description  string            `json:"Description,omitempty"`
	RegistryArn  string            `json:"RegistryArn"`
	RegistryName string            `json:"RegistryName"`
}

// registrySummaryRESTOutput is ListRegistries' item shape -- unlike
// registryRESTOutput, it has no Description field at all (RegistrySummary in
// types/types.go).
type registrySummaryRESTOutput struct {
	Tags         map[string]string `json:"tags,omitempty"`
	RegistryArn  string            `json:"RegistryArn"`
	RegistryName string            `json:"RegistryName"`
}

// schemaRESTOutput is DescribeSchema's response shape -- the only one of the
// three schema-mutating/reading ops whose output includes Content.
type schemaRESTOutput struct {
	LastModified       time.Time         `json:"LastModified"`
	VersionCreatedDate time.Time         `json:"VersionCreatedDate"`
	Tags               map[string]string `json:"tags,omitempty"`
	Content            string            `json:"Content,omitempty"`
	Description        string            `json:"Description,omitempty"`
	SchemaArn          string            `json:"SchemaArn"`
	SchemaName         string            `json:"SchemaName"`
	SchemaVersion      string            `json:"SchemaVersion"`
	Type               string            `json:"Type"`
}

// schemaRESTOutputNoContent is CreateSchema's and UpdateSchema's response
// shape (CreateSchemaOutput/UpdateSchemaOutput): field-for-field identical to
// schemaRESTOutput except there is no Content member at all.
type schemaRESTOutputNoContent struct {
	LastModified       time.Time         `json:"LastModified"`
	VersionCreatedDate time.Time         `json:"VersionCreatedDate"`
	Tags               map[string]string `json:"tags,omitempty"`
	Description        string            `json:"Description,omitempty"`
	SchemaArn          string            `json:"SchemaArn"`
	SchemaName         string            `json:"SchemaName"`
	SchemaVersion      string            `json:"SchemaVersion"`
	Type               string            `json:"Type"`
}

// schemaSummaryRESTOutput is ListSchemas' item shape (types.SchemaSummary):
// no Content, Type, SchemaVersion, RegistryName, or Description at all --
// only LastModified/SchemaArn/SchemaName/tags/VersionCount.
type schemaSummaryRESTOutput struct {
	LastModified time.Time         `json:"LastModified"`
	Tags         map[string]string `json:"tags,omitempty"`
	SchemaArn    string            `json:"SchemaArn"`
	SchemaName   string            `json:"SchemaName"`
	VersionCount int64             `json:"VersionCount"`
}

// schemaVersionSummaryRESTOutput is ListSchemaVersions' item shape
// (types.SchemaVersionSummary): no CreatedDate, unlike this package's own
// internal SchemaVersion.
type schemaVersionSummaryRESTOutput struct {
	SchemaArn     string `json:"SchemaArn"`
	SchemaName    string `json:"SchemaName"`
	SchemaVersion string `json:"SchemaVersion"`
	Type          string `json:"Type"`
}

// searchSchemaVersionSummaryRESTOutput is one entry of SearchSchemas' nested
// per-schema version list (types.SearchSchemaVersionSummary).
type searchSchemaVersionSummaryRESTOutput struct {
	CreatedDate   time.Time `json:"CreatedDate"`
	SchemaVersion string    `json:"SchemaVersion"`
	Type          string    `json:"Type"`
}

// searchSchemaSummaryRESTOutput is SearchSchemas' item shape
// (types.SearchSchemaSummary): grouped per schema with a nested version
// list, structurally unlike ListSchemas' flat schemaSummaryRESTOutput --
// this package's SearchSchemas backend call and ListSchemas backend call
// share a return type ([]Schema), but the real wire shapes for their two
// callers diverge, so this REST path builds two different response types
// from that one shared backend shape.
type searchSchemaSummaryRESTOutput struct {
	RegistryName   string                                 `json:"RegistryName"`
	SchemaArn      string                                 `json:"SchemaArn"`
	SchemaName     string                                 `json:"SchemaName"`
	SchemaVersions []searchSchemaVersionSummaryRESTOutput `json:"SchemaVersions"`
}

type codeBindingRESTOutput struct {
	CreationDate  time.Time `json:"CreationDate"`
	LastModified  time.Time `json:"LastModified"`
	SchemaVersion string    `json:"SchemaVersion"`
	Status        string    `json:"Status"`
}

func registryToREST(r *SchemaRegistry) registryRESTOutput {
	return registryRESTOutput{
		Description:  r.Description,
		RegistryArn:  r.RegistryArn,
		RegistryName: r.RegistryName,
		Tags:         r.Tags,
	}
}

func schemaToRESTWithContent(s *Schema) schemaRESTOutput {
	return schemaRESTOutput{
		Content:            s.Content,
		Description:        s.Description,
		LastModified:       s.LastModified,
		SchemaArn:          s.SchemaArn,
		SchemaName:         s.SchemaName,
		SchemaVersion:      s.SchemaVersion,
		Tags:               s.Tags,
		Type:               s.Type,
		VersionCreatedDate: s.VersionCreatedDate,
	}
}

func schemaToRESTNoContent(s *Schema) schemaRESTOutputNoContent {
	return schemaRESTOutputNoContent{
		Description:        s.Description,
		LastModified:       s.LastModified,
		SchemaArn:          s.SchemaArn,
		SchemaName:         s.SchemaName,
		SchemaVersion:      s.SchemaVersion,
		Tags:               s.Tags,
		Type:               s.Type,
		VersionCreatedDate: s.VersionCreatedDate,
	}
}

func codeBindingToREST(b *CodeBinding) codeBindingRESTOutput {
	return codeBindingRESTOutput{
		CreationDate:  b.CreationDate,
		LastModified:  b.LastModified,
		SchemaVersion: b.SchemaVersion,
		Status:        b.Status,
	}
}

// --- Dispatch ---

// handleSchemasREST serves one of the 17 real Schemas REST requests this
// file routes (see gopherstack-92ft). It reuses the SAME backend calls the
// fabricated JSON-RPC path (schemaActions/registryActions/
// schemaVersionActions/codeBindingActions in handler_schemas.go/
// handler_registries.go) already uses -- both paths are kept; a real client
// of either transport reaches the identical backend state.
func (h *Handler) handleSchemasREST(c *echo.Context, op string) error {
	m, ok := parseSchemasPath(c.Request().URL.Path)
	if !ok {
		return h.writeSchemasRESTError(
			c,
			fmt.Errorf("%w: unrecognized schemas path", ErrInvalidParameter),
		)
	}

	if fn, exists := h.schemasRESTOps()[op]; exists {
		return fn(c, m)
	}

	return h.writeSchemasRESTError(
		c,
		fmt.Errorf("%w: unknown operation %s", ErrInvalidParameter, op),
	)
}

type schemasRESTOpFunc func(*echo.Context, schemasPathMatch) error

func (h *Handler) schemasRESTOps() map[string]schemasRESTOpFunc {
	return map[string]schemasRESTOpFunc{
		opListRegistries:       func(c *echo.Context, _ schemasPathMatch) error { return h.schemasRESTListRegistries(c) },
		opCreateRegistry:       h.schemasRESTCreateRegistry,
		opDeleteRegistry:       h.schemasRESTDeleteRegistry,
		opDescribeRegistry:     h.schemasRESTDescribeRegistry,
		opUpdateRegistry:       h.schemasRESTUpdateRegistry,
		opListSchemas:          h.schemasRESTListSchemas,
		opSearchSchemas:        h.schemasRESTSearchSchemas,
		opCreateSchema:         h.schemasRESTCreateSchema,
		opDeleteSchema:         h.schemasRESTDeleteSchema,
		opDescribeSchema:       h.schemasRESTDescribeSchema,
		opUpdateSchema:         h.schemasRESTUpdateSchema,
		opListSchemaVersions:   h.schemasRESTListSchemaVersions,
		opDeleteSchemaVersion:  h.schemasRESTDeleteSchemaVersion,
		opPutCodeBinding:       h.schemasRESTPutCodeBinding,
		opDescribeCodeBinding:  h.schemasRESTDescribeCodeBinding,
		opGetCodeBindingSource: h.schemasRESTGetCodeBindingSource,
		opGetDiscoveredSchema: func(c *echo.Context, _ schemasPathMatch) error {
			return h.schemasRESTGetDiscoveredSchema(c)
		},
	}
}

func (h *Handler) writeSchemasREST(c *echo.Context, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	c.Response().Header().Set("Content-Type", schemasRESTContentType)

	return c.JSONBlob(http.StatusOK, payload)
}

// writeSchemasRESTError writes the real schemas@v1.37.4 REST-JSON1 error
// envelope: the error code travels in the X-Amzn-ErrorType header (per
// deserializers.go's awsRestjson1_deserializeOpError<Op> functions, which
// check that header before falling back to a body field), with a JSON body
// carrying a "message" field. This mirrors personalize's
// handleRuntimeRESTError (also REST-JSON1) rather than opensearch's
// JSON-RPC 1.0 awserr.Write, which is the wrong envelope for this protocol.
func (h *Handler) writeSchemasRESTError(c *echo.Context, err error) error {
	code := "InternalServerErrorException"
	status := http.StatusInternalServerError

	switch {
	case errors.Is(err, ErrNotFound):
		code, status = "NotFoundException", http.StatusNotFound
	case errors.Is(err, ErrAlreadyExists):
		code, status = "ConflictException", http.StatusConflict
	case errors.Is(err, ErrInvalidParameter):
		code, status = "BadRequestException", http.StatusBadRequest
	case errors.Is(err, ErrForbiddenOperation):
		code, status = "ForbiddenException", http.StatusForbidden
	}

	c.Response().Header().Set("Content-Type", schemasRESTContentType)
	c.Response().Header().Set("X-Amzn-Errortype", code)

	payload, marshalErr := json.Marshal(map[string]string{"message": err.Error()})
	if marshalErr != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.JSONBlob(status, payload)
}

func schemasRESTDecodeBody(r *http.Request, out any) error {
	body, err := httputils.ReadBody(r)
	if err != nil {
		return err
	}

	if len(body) == 0 {
		return nil
	}

	if uerr := json.Unmarshal(body, out); uerr != nil {
		return fmt.Errorf("%w: invalid JSON", ErrInvalidParameter)
	}

	return nil
}

// schemaVersionCount returns the total number of versions of a schema by
// walking ListSchemaVersions' pages -- the real ListSchemas response
// includes this count (types.SchemaSummary.VersionCount) but this package's
// backend has no direct accessor for it.
func (h *Handler) schemaVersionCount(ctx context.Context, registryName, schemaName string) int64 {
	var count int64

	token := ""
	for {
		versions, next, err := h.Backend.ListSchemaVersions(ctx, registryName, schemaName, token, 0)
		if err != nil {
			return count
		}

		count += int64(len(versions))
		if next == "" {
			return count
		}

		token = next
	}
}

// --- Registry ops ---

func (h *Handler) schemasRESTListRegistries(c *echo.Context) error {
	ctx := c.Request().Context()
	q := c.Request().URL.Query()

	regs, next, err := h.Backend.ListRegistries(
		ctx,
		q.Get("registryNamePrefix"),
		q.Get("nextToken"),
		schemasRESTLimit(q),
	)
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	summaries := make([]registrySummaryRESTOutput, 0, len(regs))
	for _, r := range regs {
		summaries = append(summaries, registrySummaryRESTOutput{
			RegistryArn:  r.RegistryArn,
			RegistryName: r.RegistryName,
			Tags:         r.Tags,
		})
	}

	return h.writeSchemasREST(c, struct {
		NextToken  string                      `json:"NextToken,omitempty"`
		Registries []registrySummaryRESTOutput `json:"Registries"`
	}{Registries: summaries, NextToken: next})
}

func (h *Handler) schemasRESTCreateRegistry(c *echo.Context, m schemasPathMatch) error {
	var in schemasCreateRegistryBodyREST
	if err := schemasRESTDecodeBody(c.Request(), &in); err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	reg, err := h.Backend.CreateRegistry(c.Request().Context(), CreateRegistryInput{
		RegistryName: m.registryName,
		Description:  in.Description,
		Tags:         in.Tags,
	})
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, registryToREST(reg))
}

func (h *Handler) schemasRESTDeleteRegistry(c *echo.Context, m schemasPathMatch) error {
	if err := h.Backend.DeleteRegistry(c.Request().Context(), m.registryName); err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, map[string]any{})
}

func (h *Handler) schemasRESTDescribeRegistry(c *echo.Context, m schemasPathMatch) error {
	reg, err := h.Backend.DescribeRegistry(c.Request().Context(), m.registryName)
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, registryToREST(reg))
}

func (h *Handler) schemasRESTUpdateRegistry(c *echo.Context, m schemasPathMatch) error {
	var in schemasUpdateRegistryBodyREST
	if err := schemasRESTDecodeBody(c.Request(), &in); err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	reg, err := h.Backend.UpdateRegistry(c.Request().Context(), UpdateRegistryInput{
		RegistryName: m.registryName,
		Description:  in.Description,
	})
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, registryToREST(reg))
}

// --- Schema ops ---

func (h *Handler) schemasRESTListSchemas(c *echo.Context, m schemasPathMatch) error {
	ctx := c.Request().Context()
	q := c.Request().URL.Query()

	schemas, next, err := h.Backend.ListSchemas(
		ctx, m.registryName, q.Get("schemaNamePrefix"), q.Get("nextToken"), schemasRESTLimit(q),
	)
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	summaries := make([]schemaSummaryRESTOutput, 0, len(schemas))
	for _, s := range schemas {
		summaries = append(summaries, schemaSummaryRESTOutput{
			LastModified: s.LastModified,
			SchemaArn:    s.SchemaArn,
			SchemaName:   s.SchemaName,
			Tags:         s.Tags,
			VersionCount: h.schemaVersionCount(ctx, m.registryName, s.SchemaName),
		})
	}

	return h.writeSchemasREST(c, struct {
		NextToken string                    `json:"NextToken,omitempty"`
		Schemas   []schemaSummaryRESTOutput `json:"Schemas"`
	}{Schemas: summaries, NextToken: next})
}

func (h *Handler) schemasRESTSearchSchemas(c *echo.Context, m schemasPathMatch) error {
	ctx := c.Request().Context()
	q := c.Request().URL.Query()

	schemas, next, err := h.Backend.SearchSchemas(
		ctx, m.registryName, q.Get("keywords"), q.Get("nextToken"), schemasRESTLimit(q),
	)
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	summaries := make([]searchSchemaSummaryRESTOutput, 0, len(schemas))
	for _, s := range schemas {
		versions, _, verr := h.Backend.ListSchemaVersions(ctx, m.registryName, s.SchemaName, "", 0)
		if verr != nil {
			// ListSchemaVersions legitimately declares NotFoundException, but
			// SearchSchemas' own deserializeOpError switch does not -- and
			// the only way this per-item lookup can fail here is a schema
			// deleted between SearchSchemas' own listing and this call
			// (both take/release the lock separately). Skip the vanished
			// entry rather than fail the whole search with a code
			// SearchSchemas can't declare (gopherstack-uox6 sweep).
			if errors.Is(verr, ErrNotFound) {
				continue
			}

			return h.writeSchemasRESTError(c, verr)
		}

		vsum := make([]searchSchemaVersionSummaryRESTOutput, 0, len(versions))
		for _, v := range versions {
			vsum = append(vsum, searchSchemaVersionSummaryRESTOutput{
				CreatedDate:   v.CreatedDate,
				SchemaVersion: v.SchemaVersion,
				Type:          v.Type,
			})
		}

		summaries = append(summaries, searchSchemaSummaryRESTOutput{
			RegistryName:   m.registryName,
			SchemaArn:      s.SchemaArn,
			SchemaName:     s.SchemaName,
			SchemaVersions: vsum,
		})
	}

	return h.writeSchemasREST(c, struct {
		NextToken string                          `json:"NextToken,omitempty"`
		Schemas   []searchSchemaSummaryRESTOutput `json:"Schemas"`
	}{Schemas: summaries, NextToken: next})
}

func (h *Handler) schemasRESTCreateSchema(c *echo.Context, m schemasPathMatch) error {
	var in schemasCreateSchemaBodyREST
	if err := schemasRESTDecodeBody(c.Request(), &in); err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	schema, err := h.Backend.CreateSchema(c.Request().Context(), CreateSchemaInput{
		RegistryName: m.registryName,
		SchemaName:   m.schemaName,
		Type:         in.Type,
		Content:      in.Content,
		Description:  in.Description,
		Tags:         in.Tags,
	})
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, schemaToRESTNoContent(schema))
}

func (h *Handler) schemasRESTDeleteSchema(c *echo.Context, m schemasPathMatch) error {
	if err := h.Backend.DeleteSchema(c.Request().Context(), m.registryName, m.schemaName); err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, map[string]any{})
}

func (h *Handler) schemasRESTDescribeSchema(c *echo.Context, m schemasPathMatch) error {
	schemaVersion := c.Request().URL.Query().Get("schemaVersion")

	schema, err := h.Backend.DescribeSchema(
		c.Request().Context(),
		m.registryName,
		m.schemaName,
		schemaVersion,
	)
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, schemaToRESTWithContent(schema))
}

func (h *Handler) schemasRESTUpdateSchema(c *echo.Context, m schemasPathMatch) error {
	var in schemasUpdateSchemaBodyREST
	if err := schemasRESTDecodeBody(c.Request(), &in); err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	schema, err := h.Backend.UpdateSchema(c.Request().Context(), UpdateSchemaInput{
		RegistryName:  m.registryName,
		SchemaName:    m.schemaName,
		Type:          in.Type,
		Content:       in.Content,
		Description:   in.Description,
		ClientTokenID: in.ClientTokenID,
	})
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, schemaToRESTNoContent(schema))
}

// --- Schema version ops ---

func (h *Handler) schemasRESTListSchemaVersions(c *echo.Context, m schemasPathMatch) error {
	ctx := c.Request().Context()
	q := c.Request().URL.Query()

	versions, next, err := h.Backend.ListSchemaVersions(
		ctx, m.registryName, m.schemaName, q.Get("nextToken"), schemasRESTLimit(q),
	)
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	summaries := make([]schemaVersionSummaryRESTOutput, 0, len(versions))
	for _, v := range versions {
		summaries = append(summaries, schemaVersionSummaryRESTOutput{
			SchemaArn:     v.SchemaArn,
			SchemaName:    v.SchemaName,
			SchemaVersion: v.SchemaVersion,
			Type:          v.Type,
		})
	}

	return h.writeSchemasREST(c, struct {
		NextToken      string                           `json:"NextToken,omitempty"`
		SchemaVersions []schemaVersionSummaryRESTOutput `json:"SchemaVersions"`
	}{SchemaVersions: summaries, NextToken: next})
}

func (h *Handler) schemasRESTDeleteSchemaVersion(c *echo.Context, m schemasPathMatch) error {
	err := h.Backend.DeleteSchemaVersion(
		c.Request().Context(),
		m.registryName,
		m.schemaName,
		m.schemaVersion,
	)
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, map[string]any{})
}

func (h *Handler) schemasRESTGetDiscoveredSchema(c *echo.Context) error {
	var in schemasGetDiscoveredBodyREST
	if err := schemasRESTDecodeBody(c.Request(), &in); err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	content, err := h.Backend.GetDiscoveredSchema(
		c.Request().Context(),
		GetDiscoveredSchemaInput(in),
	)
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, struct {
		Content string `json:"Content,omitempty"`
	}{Content: content})
}

// --- Code binding ops ---

func (h *Handler) schemasRESTPutCodeBinding(c *echo.Context, m schemasPathMatch) error {
	binding, err := h.Backend.PutCodeBinding(c.Request().Context(), PutCodeBindingInput{
		RegistryName:  m.registryName,
		SchemaName:    m.schemaName,
		Language:      m.language,
		SchemaVersion: c.Request().URL.Query().Get("schemaVersion"),
	})
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, codeBindingToREST(binding))
}

func (h *Handler) schemasRESTDescribeCodeBinding(c *echo.Context, m schemasPathMatch) error {
	binding, err := h.Backend.DescribeCodeBinding(c.Request().Context(), DescribeCodeBindingInput{
		RegistryName:  m.registryName,
		SchemaName:    m.schemaName,
		Language:      m.language,
		SchemaVersion: c.Request().URL.Query().Get("schemaVersion"),
	})
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return h.writeSchemasREST(c, codeBindingToREST(binding))
}

// schemasRESTGetCodeBindingSource serves GetCodeBindingSource's real wire
// shape: the response body IS the raw payload (GetCodeBindingSourceOutput.Body
// []byte, per deserializers.go's awsRestjson1_deserializeOpDocumentGetCodeBindingSourceOutput,
// which reads the HTTP body directly with no JSON envelope at all) -- not
// the {"Body": "..."} JSON wrapper the fabricated JSON-RPC path in
// handler_schemas.go uses.
func (h *Handler) schemasRESTGetCodeBindingSource(c *echo.Context, m schemasPathMatch) error {
	src, err := h.Backend.GetCodeBindingSource(
		c.Request().Context(), m.registryName, m.schemaName, m.language,
		c.Request().URL.Query().Get("schemaVersion"),
	)
	if err != nil {
		return h.writeSchemasRESTError(c, err)
	}

	return c.Blob(http.StatusOK, "application/octet-stream", []byte(src))
}
