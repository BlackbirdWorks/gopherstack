package azuretable

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// devstoreAccountName is Azurite's well-known development storage account
// name, used to build odata.type values in fullmetadata responses (e.g.
// "devstoreaccount1.Tables"). See AZURE.md section 5 and pkgs/azureauth.
const devstoreAccountName = "devstoreaccount1"

// entityTimeLayout formats an Edm.DateTime property/Timestamp value on the
// wire: a variable-precision (trailing zeros trimmed) RFC3339 string,
// matching aztables' own EDMDateTime.MarshalText layout exactly so its
// time.Parse round-trips cleanly.
const entityTimeLayout = "2006-01-02T15:04:05.9999999Z"

// maxQueryTop is the largest $top value queryEntities honors; a caller-
// supplied value beyond this is clamped, not rejected, since larger values
// are harmless (this backend already returns everything in one page -- see
// PARITY.md's pagination gap) but an absurd value should never be used to
// pre-size an allocation.
const maxQueryTop = 100000

// --- Path/key-predicate parsing ---

// unquoteODataString unquotes a single '...'-delimited OData string literal
// (with ” as an escaped single quote), such as the table-name literal in
// DELETE /<account>/Tables('foo'). Returns ("", false) for anything else
// (missing/mismatched quotes, an unescaped quote inside).
func unquoteODataString(s string) (string, bool) {
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return "", false
	}

	inner := s[1 : len(s)-1]

	var b strings.Builder

	i := 0
	for i < len(inner) {
		if inner[i] == '\'' {
			if i+1 < len(inner) && inner[i+1] == '\'' {
				b.WriteByte('\'')
				i += 2

				continue
			}

			return "", false
		}

		b.WriteByte(inner[i])
		i++
	}

	return b.String(), true
}

// escapeODataKey escapes a key value for embedding back into a
// single-quoted OData literal (the inverse of unquoteODataString).
func escapeODataKey(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// splitTopLevelCommas splits s on commas that are not inside a
// single-quoted literal. Quote-parity tracking (not full escape-aware
// parsing) is sufficient here: a doubled ” toggles parity twice, correctly
// leaving the parser "still inside" the literal it started in.
func splitTopLevelCommas(s string) []string {
	var parts []string

	var b strings.Builder

	inQuote := false

	for i := range len(s) {
		c := s[i]

		switch {
		case c == '\'':
			inQuote = !inQuote

			b.WriteByte(c)
		case c == ',' && !inQuote:
			parts = append(parts, b.String())
			b.Reset()
		default:
			b.WriteByte(c)
		}
	}

	parts = append(parts, b.String())

	return parts
}

// parseEntityKeyPredicate parses an entity key predicate
// ("PartitionKey='p',RowKey='r'", in either key order) into its two values.
// Returns ok=false for anything malformed.
func parseEntityKeyPredicate(predicate string) (string, string, bool) {
	parts := splitTopLevelCommas(predicate)
	if len(parts) != 2 { //nolint:mnd // exactly PartitionKey and RowKey
		return "", "", false
	}

	var partitionKey, rowKey string

	var havePK, haveRK bool

	for _, part := range parts {
		key, value, splitOK := strings.Cut(part, "=")
		if !splitOK {
			return "", "", false
		}

		key = strings.TrimSpace(key)

		unquoted, unquoteOK := unquoteODataString(strings.TrimSpace(value))
		if !unquoteOK {
			return "", "", false
		}

		switch key {
		case partitionKeyProperty:
			partitionKey, havePK = unquoted, true
		case rowKeyProperty:
			rowKey, haveRK = unquoted, true
		default:
			return "", "", false
		}
	}

	if !havePK || !haveRK {
		return "", "", false
	}

	return partitionKey, rowKey, true
}

// --- Entity body decode (request -> EntityProperty map) ---

// decodeEntityBody parses an entity JSON request body into its
// PartitionKey/RowKey (if present) and typed custom properties.
// "@odata.type"-annotated properties are decoded per their declared EDM
// type; unannotated ones are inferred the same way
// azure-sdk-for-go/sdk/data/aztables's own EDMEntity.UnmarshalJSON infers
// them client-side: try Edm.Int32 first, then fall back to the JSON value's
// natural type (float64 -> Edm.Double, bool -> Edm.Boolean, string ->
// Edm.String). "Timestamp" is silently ignored (server-managed; a client-
// supplied value never overwrites it -- the server always wins). Any
// "odata.*"/"@odata.type" metadata key is skipped.
func decodeEntityBody(body []byte) (string, string, bool, bool, map[string]EntityProperty, error) {
	var raw map[string]json.RawMessage

	if err := json.Unmarshal(body, &raw); err != nil {
		return "", "", false, false, nil, fmt.Errorf("%w: %w", ErrInvalidEntityProperty, err)
	}

	partitionKey, hasPK, err := decodeSystemKeyProperty(raw, partitionKeyProperty)
	if err != nil {
		return "", "", false, false, nil, err
	}

	rowKey, hasRK, err := decodeSystemKeyProperty(raw, rowKeyProperty)
	if err != nil {
		return "", "", false, false, nil, err
	}

	props, err := decodeCustomProperties(raw)
	if err != nil {
		return "", "", false, false, nil, err
	}

	return partitionKey, rowKey, hasPK, hasRK, props, nil
}

// decodeSystemKeyProperty extracts the string-valued system property name
// (PartitionKey or RowKey) from raw, if present.
func decodeSystemKeyProperty(raw map[string]json.RawMessage, name string) (string, bool, error) {
	rawVal, ok := raw[name]
	if !ok {
		return "", false, nil
	}

	var s string
	if err := json.Unmarshal(rawVal, &s); err != nil {
		return "", false, fmt.Errorf("%w: %s must be a string", ErrInvalidEntityProperty, name)
	}

	return s, true, nil
}

// isSystemOrMetadataKey reports whether key is a system property
// (PartitionKey/RowKey/Timestamp), an "@odata.type" annotation, or other
// "odata."-prefixed metadata -- none of which decodeCustomProperties treats
// as a user-defined entity property.
func isSystemOrMetadataKey(key string) bool {
	if strings.HasSuffix(key, "@odata.type") || strings.HasPrefix(key, "odata.") {
		return true
	}

	switch key {
	case partitionKeyProperty, rowKeyProperty, timestampProperty:
		return true
	default:
		return false
	}
}

// decodeCustomProperties decodes every user-defined (non-system,
// non-metadata) property in raw into its typed EntityProperty.
func decodeCustomProperties(raw map[string]json.RawMessage) (map[string]EntityProperty, error) {
	props := make(map[string]EntityProperty, len(raw))

	for key, rawVal := range raw {
		if isSystemOrMetadataKey(key) || string(rawVal) == "null" {
			continue
		}

		edmType := ""

		if annRaw, ok := raw[key+"@odata.type"]; ok {
			if err := json.Unmarshal(annRaw, &edmType); err != nil {
				return nil, fmt.Errorf("%w: malformed %s@odata.type", ErrInvalidEntityProperty, key)
			}
		}

		prop, err := decodeProperty(edmType, rawVal)
		if err != nil {
			return nil, err
		}

		props[key] = prop
	}

	return props, nil
}

// decodeProperty decodes raw into a typed EntityProperty per its (possibly
// empty) "@odata.type" annotation value edmType.
func decodeProperty(edmType string, raw json.RawMessage) (EntityProperty, error) {
	switch EdmType(edmType) {
	case EdmString, EdmInt32, EdmDouble, EdmBoolean:
		return decodeScalarProperty(EdmType(edmType), raw)
	case EdmInt64, EdmDateTime, EdmGUID, EdmBinary:
		return decodeStringEncodedProperty(EdmType(edmType), raw)
	case "":
		return decodeUnannotatedProperty(raw)
	default:
		return EntityProperty{}, fmt.Errorf("%w: unknown @odata.type %q", ErrInvalidEntityProperty, edmType)
	}
}

// decodeScalarProperty decodes the four EDM types whose wire representation
// is a bare, natively-typed JSON value (string/number/number/bool).
func decodeScalarProperty(edmType EdmType, raw json.RawMessage) (EntityProperty, error) {
	switch edmType {
	case EdmString:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return EntityProperty{}, fmt.Errorf("%w: not a string", ErrInvalidEntityProperty)
		}

		return EntityProperty{Type: EdmString, Value: s}, nil
	case EdmInt32:
		var n int32
		if err := json.Unmarshal(raw, &n); err != nil {
			return EntityProperty{}, fmt.Errorf("%w: not an Int32", ErrInvalidEntityProperty)
		}

		return EntityProperty{Type: EdmInt32, Value: n}, nil
	case EdmDouble:
		var f float64
		if err := json.Unmarshal(raw, &f); err != nil {
			return EntityProperty{}, fmt.Errorf("%w: not a Double", ErrInvalidEntityProperty)
		}

		return EntityProperty{Type: EdmDouble, Value: f}, nil
	case EdmBoolean:
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return EntityProperty{}, fmt.Errorf("%w: not a Boolean", ErrInvalidEntityProperty)
		}

		return EntityProperty{Type: EdmBoolean, Value: b}, nil
	default:
		return EntityProperty{}, fmt.Errorf("%w: unsupported scalar type %q", ErrInvalidEntityProperty, edmType)
	}
}

// decodeStringEncodedProperty decodes the four EDM types whose wire
// representation is always a JSON string carrying an encoded value (decimal
// digits, an RFC3339 timestamp, a UUID, or base64), requiring the
// "@odata.type" annotation to disambiguate from Edm.String.
func decodeStringEncodedProperty(edmType EdmType, raw json.RawMessage) (EntityProperty, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return EntityProperty{}, fmt.Errorf("%w: %s must be a string", ErrInvalidEntityProperty, edmType)
	}

	switch edmType {
	case EdmInt64:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return EntityProperty{}, fmt.Errorf("%w: invalid Int64 %q", ErrInvalidEntityProperty, s)
		}

		return EntityProperty{Type: EdmInt64, Value: n}, nil
	case EdmDateTime:
		t, err := time.Parse(entityTimeLayout, s)
		if err != nil {
			return EntityProperty{}, fmt.Errorf("%w: invalid DateTime %q", ErrInvalidEntityProperty, s)
		}

		return EntityProperty{Type: EdmDateTime, Value: t}, nil
	case EdmGUID:
		return EntityProperty{Type: EdmGUID, Value: s}, nil
	case EdmBinary:
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return EntityProperty{}, fmt.Errorf("%w: invalid base64 %q", ErrInvalidEntityProperty, s)
		}

		return EntityProperty{Type: EdmBinary, Value: b}, nil
	default:
		return EntityProperty{}, fmt.Errorf("%w: unsupported string-encoded type %q", ErrInvalidEntityProperty, edmType)
	}
}

// decodeUnannotatedProperty infers a bare (unannotated) property's EDM type,
// mirroring aztables' own client-side inference exactly: try Int32 first
// (so a decimal-point-free number becomes Edm.Int32), then fall back to the
// JSON value's natural Go type.
func decodeUnannotatedProperty(raw json.RawMessage) (EntityProperty, error) {
	var i32 int32
	if err := json.Unmarshal(raw, &i32); err == nil {
		return EntityProperty{Type: EdmInt32, Value: i32}, nil
	}

	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return EntityProperty{Type: EdmDouble, Value: f}, nil
	}

	var b bool
	if err := json.Unmarshal(raw, &b); err == nil {
		return EntityProperty{Type: EdmBoolean, Value: b}, nil
	}

	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return EntityProperty{Type: EdmString, Value: s}, nil
	}

	return EntityProperty{}, fmt.Errorf("%w: unsupported JSON value %s", ErrInvalidEntityProperty, string(raw))
}

// --- Entity body encode (EntityProperty map -> response) ---

// encodeEntity builds an entity's OData JSON response body at the given
// metadata level. select, if non-empty, is a comma-separated $select
// projection list; PartitionKey/RowKey/Timestamp are always included
// regardless of select (they're the entity's identity and are cheap/always
// safe to return -- see PARITY.md's $select note), while custom properties
// are filtered to the requested names.
func (h *Handler) encodeEntity(info EntityInfo, table, level, selectParam string) map[string]any {
	m := map[string]any{}

	if level != odataLevelNoMetadata {
		endpoint := h.serviceEndpoint()
		m["odata.metadata"] = endpoint + "/$metadata#" + table + "/@Element"
		m["odata.etag"] = info.ETag

		if level == odataLevelFullMetadata {
			keyPredicate := partitionKeyProperty + "='" + escapeODataKey(info.PartitionKey) +
				"'," + rowKeyProperty + "='" + escapeODataKey(info.RowKey) + "'"
			m["odata.type"] = devstoreAccountName + "." + table
			m["odata.id"] = endpoint + "/" + table + "(" + keyPredicate + ")"
			m["odata.editLink"] = table + "(" + keyPredicate + ")"
		}
	}

	m[partitionKeyProperty] = info.PartitionKey
	m[rowKeyProperty] = info.RowKey
	m[timestampProperty] = info.Timestamp.UTC().Format(entityTimeLayout)

	selected := selectSet(selectParam)
	for name, prop := range info.Properties {
		if selected != nil && !selected[name] {
			continue
		}

		encodePropertyInto(m, name, prop)
	}

	return m
}

func selectSet(selectParam string) map[string]bool {
	if selectParam == "" {
		return nil
	}

	names := strings.Split(selectParam, ",")
	set := make(map[string]bool, len(names))

	for _, n := range names {
		set[strings.TrimSpace(n)] = true
	}

	return set
}

//nolint:cyclop // per-EDM-type dispatch; splitting would obscure it
func encodePropertyInto(m map[string]any, name string, prop EntityProperty) {
	switch prop.Type {
	case EdmString:
		if s, ok := prop.Value.(string); ok {
			m[name] = s
		}
	case EdmInt32:
		if n, ok := prop.Value.(int32); ok {
			m[name] = n
		}
	case EdmDouble:
		if f, ok := prop.Value.(float64); ok {
			m[name] = f
		}
	case EdmBoolean:
		if b, ok := prop.Value.(bool); ok {
			m[name] = b
		}
	case EdmInt64:
		if n, ok := prop.Value.(int64); ok {
			m[name] = strconv.FormatInt(n, 10)
			m[name+"@odata.type"] = string(EdmInt64)
		}
	case EdmDateTime:
		if t, ok := prop.Value.(time.Time); ok {
			m[name] = t.UTC().Format(entityTimeLayout)
			m[name+"@odata.type"] = string(EdmDateTime)
		}
	case EdmGUID:
		if s, ok := prop.Value.(string); ok {
			m[name] = s
			m[name+"@odata.type"] = string(EdmGUID)
		}
	case EdmBinary:
		if b, ok := prop.Value.([]byte); ok {
			m[name] = base64.StdEncoding.EncodeToString(b)
			m[name+"@odata.type"] = string(EdmBinary)
		}
	}
}

// --- HTTP handlers ---

func (h *Handler) insertEntity(c *echo.Context, table string) error {
	r := c.Request()

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	partitionKey, rowKey, hasPK, hasRK, props, decErr := decodeEntityBody(body)
	if decErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInput", "One of the request inputs is not valid.")
	}

	if !hasPK || !hasRK {
		return h.writeError(c, http.StatusBadRequest, "InvalidInput",
			"The values are not specified for all the key properties of the entity.")
	}

	info, err := h.Backend.InsertEntity(table, partitionKey, rowKey, props)

	switch {
	case err == nil:
	case errors.Is(err, ErrTableNotFound):
		return h.writeTableNotFoundError(c)
	case errors.Is(err, ErrEntityAlreadyExists):
		return h.writeError(c, http.StatusConflict, "EntityAlreadyExists", "The specified entity already exists.")
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	c.Response().Header().Set("ETag", info.ETag)

	if r.Header.Get("Prefer") == preferReturnNoContent {
		c.Response().Header().Set("Preference-Applied", preferReturnNoContent)

		return c.NoContent(http.StatusNoContent)
	}

	level := odataLevelFromAccept(r.Header.Get("Accept"))

	return h.writeJSON(c, http.StatusCreated, h.encodeEntity(info, table, level, ""))
}

func (h *Handler) getEntity(c *echo.Context, table, partitionKey, rowKey string) error {
	info, err := h.Backend.GetEntity(table, partitionKey, rowKey)

	switch {
	case err == nil:
	case errors.Is(err, ErrTableNotFound):
		return h.writeTableNotFoundError(c)
	case errors.Is(err, ErrEntityNotFound):
		return h.writeResourceNotFoundError(c)
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	c.Response().Header().Set("ETag", info.ETag)

	level := odataLevelFromAccept(c.Request().Header.Get("Accept"))

	return h.writeJSON(c, http.StatusOK, h.encodeEntity(info, table, level, c.QueryParam("$select")))
}

func (h *Handler) queryEntities(c *echo.Context, table string) error {
	top, topErr := parseTop(c.QueryParam("$top"))
	if topErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInput", "The value for $top is invalid.")
	}

	var filter Node

	if filterParam := c.QueryParam("$filter"); filterParam != "" {
		node, parseErr := ParseFilter(filterParam)
		if parseErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInput", "The specified $filter is invalid.")
		}

		filter = node
	}

	infos, err := h.Backend.QueryEntities(table, filter, top)
	if err != nil {
		return h.writeTableNotFoundError(c)
	}

	level := odataLevelFromAccept(c.Request().Header.Get("Accept"))
	selectParam := c.QueryParam("$select")

	values := make([]map[string]any, 0, len(infos))
	for _, info := range infos {
		values = append(values, h.encodeEntity(info, table, level, selectParam))
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{"value": values})
}

// parseTop parses the $top query parameter, defaulting to 0 (unlimited)
// when absent and clamping (never erroring on) an oversized value, per
// maxQueryTop's doc comment. A negative value is rejected.
func parseTop(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, ErrInvalidEntityProperty
	}

	if n > maxQueryTop {
		n = maxQueryTop
	}

	return n, nil
}

func (h *Handler) replaceEntity(c *echo.Context, table, partitionKey, rowKey string) error {
	return h.putOrMergeEntity(c, table, partitionKey, rowKey, h.Backend.ReplaceEntity)
}

func (h *Handler) mergeEntity(c *echo.Context, table, partitionKey, rowKey string) error {
	return h.putOrMergeEntity(c, table, partitionKey, rowKey, h.Backend.MergeEntity)
}

// mutateEntityFunc is the shape ReplaceEntity/MergeEntity share, so
// putOrMergeEntity can dispatch to either one generically.
type mutateEntityFunc func(
	table, partitionKey, rowKey string, props map[string]EntityProperty, ifMatch string,
) (EntityInfo, error)

func (h *Handler) putOrMergeEntity(
	c *echo.Context, table, partitionKey, rowKey string, mutate mutateEntityFunc,
) error {
	r := c.Request()

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	_, _, _, _, props, decErr := decodeEntityBody(body)
	if decErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInput", "One of the request inputs is not valid.")
	}

	ifMatch := r.Header.Get("If-Match")

	info, mutateErr := mutate(table, partitionKey, rowKey, props, ifMatch)

	switch {
	case mutateErr == nil:
	case errors.Is(mutateErr, ErrTableNotFound):
		return h.writeTableNotFoundError(c)
	case errors.Is(mutateErr, ErrEntityNotFound):
		return h.writeResourceNotFoundError(c)
	case errors.Is(mutateErr, ErrETagMismatch):
		return h.writeError(c, http.StatusPreconditionFailed, "UpdateConditionNotSatisfied",
			"The update condition specified in the request was not satisfied.")
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", mutateErr.Error())
	}

	c.Response().Header().Set("ETag", info.ETag)

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) deleteEntity(c *echo.Context, table, partitionKey, rowKey string) error {
	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInput", "An If-Match header is required for delete.")
	}

	err := h.Backend.DeleteEntity(table, partitionKey, rowKey, ifMatch)

	switch {
	case err == nil:
		return c.NoContent(http.StatusNoContent)
	case errors.Is(err, ErrTableNotFound):
		return h.writeTableNotFoundError(c)
	case errors.Is(err, ErrEntityNotFound):
		return h.writeResourceNotFoundError(c)
	case errors.Is(err, ErrETagMismatch):
		return h.writeError(c, http.StatusPreconditionFailed, "UpdateConditionNotSatisfied",
			"The update condition specified in the request was not satisfied.")
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

// writeResourceNotFoundError maps a missing-entity StorageBackend error to
// the corresponding Azure error code/status.
func (h *Handler) writeResourceNotFoundError(c *echo.Context) error {
	return h.writeError(c, http.StatusNotFound, "ResourceNotFound", "The specified resource does not exist.")
}
