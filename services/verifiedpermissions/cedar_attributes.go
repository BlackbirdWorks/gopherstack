package verifiedpermissions

import (
	"encoding/json"
	"fmt"

	cedar "github.com/cedar-policy/cedar-go"
	cedartypes "github.com/cedar-policy/cedar-go/types"
)

// entityIdentifierJSON already exists in handler_authorization.go (entityType/entityId).

// attributeValueToCedar converts one AWS VerifiedPermissions AttributeValue-shaped
// JSON object into a cedar-go value. AWS's wire format is a discriminated
// single-key object -- {"boolean": true}, {"long": 5}, {"entityIdentifier": {...}},
// {"record": {...}}, {"set": [...]}, ... (verifiedpermissions@v1.36.4
// serializers.go's awsAwsjson10_serializeDocumentAttributeValue) -- which is NOT
// the same as cedar-go's own implicit JSON value format (types.UnmarshalJSON:
// bare JSON literals plus __entity/__extn tags), so this needs its own converter
// rather than reusing cedar-go's decoder.
func attributeValueToCedar(raw json.RawMessage) (cedar.Value, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%w: invalid attribute value: %w", errInvalidRequest, err)
	}

	if len(m) != 1 {
		return nil, fmt.Errorf("%w: attribute value must have exactly one type key, got %d", errInvalidRequest, len(m))
	}

	for key, v := range m {
		return attributeValueMemberToCedar(key, v)
	}

	return nil, fmt.Errorf("%w: empty attribute value", errInvalidRequest)
}

// per-member would scatter the union's shape across more functions than it clarifies.
//
//nolint:cyclop // one discriminated-union switch over AWS's 9 AttributeValue member types; decomposing
func attributeValueMemberToCedar(key string, v json.RawMessage) (cedar.Value, error) {
	switch key {
	case "boolean":
		var b bool
		if err := json.Unmarshal(v, &b); err != nil {
			return nil, fmt.Errorf("%w: invalid boolean attribute: %w", errInvalidRequest, err)
		}

		return cedar.Boolean(b), nil
	case "string":
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return nil, fmt.Errorf("%w: invalid string attribute: %w", errInvalidRequest, err)
		}

		return cedar.String(s), nil
	case "long":
		var n int64
		if err := json.Unmarshal(v, &n); err != nil {
			return nil, fmt.Errorf("%w: invalid long attribute: %w", errInvalidRequest, err)
		}

		return cedar.Long(n), nil
	case "decimal":
		return parseCedarStringAttr(v, cedartypes.ParseDecimal)
	case "datetime":
		return parseCedarStringAttr(v, cedartypes.ParseDatetime)
	case "duration":
		return parseCedarStringAttr(v, cedartypes.ParseDuration)
	case "ipaddr":
		return parseCedarStringAttr(v, cedartypes.ParseIPAddr)
	case "entityIdentifier":
		var eid entityIdentifierJSON
		if err := json.Unmarshal(v, &eid); err != nil {
			return nil, fmt.Errorf("%w: invalid entityIdentifier attribute: %w", errInvalidRequest, err)
		}

		return cedar.NewEntityUID(cedar.EntityType(eid.EntityType), cedar.String(eid.EntityID)), nil
	case "record":
		return recordAttributeToCedar(v)
	case "set":
		return setAttributeToCedar(v)
	default:
		return nil, fmt.Errorf("%w: unknown attribute value type %q", errInvalidRequest, key)
	}
}

func parseCedarStringAttr[T cedar.Value](raw json.RawMessage, parse func(string) (T, error)) (cedar.Value, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("%w: invalid string-encoded attribute: %w", errInvalidRequest, err)
	}

	val, err := parse(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return val, nil
}

func recordAttributeToCedar(raw json.RawMessage) (cedar.Value, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("%w: invalid record attribute: %w", errInvalidRequest, err)
	}

	rec := make(cedar.RecordMap, len(fields))

	for k, v := range fields {
		cv, err := attributeValueToCedar(v)
		if err != nil {
			return nil, err
		}

		rec[cedar.String(k)] = cv
	}

	return cedar.NewRecord(rec), nil
}

func setAttributeToCedar(raw json.RawMessage) (cedar.Value, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("%w: invalid set attribute: %w", errInvalidRequest, err)
	}

	vals := make([]cedar.Value, 0, len(items))

	for _, item := range items {
		cv, err := attributeValueToCedar(item)
		if err != nil {
			return nil, err
		}

		vals = append(vals, cv)
	}

	return cedar.NewSet(vals...), nil
}

// attributeMapToCedarRecord converts an AWS "attributes"/"contextMap" object
// (map of name -> AttributeValue) into a cedar-go Record.
func attributeMapToCedarRecord(m map[string]json.RawMessage) (cedar.Record, error) {
	rec := make(cedar.RecordMap, len(m))

	for k, v := range m {
		cv, err := attributeValueToCedar(v)
		if err != nil {
			return cedar.Record{}, err
		}

		rec[cedar.String(k)] = cv
	}

	return cedar.NewRecord(rec), nil
}

// entityItemJSON mirrors the real SDK's types.EntityItem wire shape
// (identifier/attributes/parents -- verifiedpermissions@v1.36.4 serializers.go's
// awsAwsjson10_serializeDocumentEntityItem). Cedar "tags" (EntityItem.Tags) are
// not modeled here -- see entitiesDefinitionJSON's doc comment.
type entityItemJSON struct {
	Identifier *entityIdentifierJSON      `json:"identifier,omitempty"`
	Attributes map[string]json.RawMessage `json:"attributes,omitempty"`
	Parents    []entityIdentifierJSON     `json:"parents,omitempty"`
}

// entitiesDefinitionJSON mirrors the real SDK's types.EntitiesDefinition union
// (verifiedpermissions@v1.36.4 serializers.go's
// awsAwsjson10_serializeDocumentEntitiesDefinition): exactly one of entityList
// (typed AttributeValue objects) or cedarJson (a literal Cedar JSON string,
// which happens to use cedar-go's own native entity JSON shape -- uid/attrs/
// parents -- verified against cedar-go@v1.8.0's types.EntityMap.UnmarshalJSON)
// is present. Cedar "tags" (EntityItem.Tags, added to the real API after this
// union was first modeled) are not converted -- cedar-go's own Entity.Tags
// field exists and could carry them, but wiring a second, tags-specific
// AttributeValue-shaped map through the same converter is left for a
// follow-up; every entity converted here has an empty Tags set, which is
// honest (no tag ever silently dropped from what a client can already send
// through the modeled attributes/parents path) rather than a corruption.
type entitiesDefinitionJSON struct {
	CedarJSON  string           `json:"cedarJson,omitempty"`
	EntityList []entityItemJSON `json:"entityList,omitempty"`
}

// contextDefinitionJSON mirrors the real SDK's types.ContextDefinition union
// (contextMap: typed AttributeValue objects: cedarJson: a literal Cedar JSON
// object using cedar-go's own native record JSON shape).
type contextDefinitionJSON struct {
	ContextMap map[string]json.RawMessage `json:"contextMap,omitempty"`
	CedarJSON  string                     `json:"cedarJson,omitempty"`
}

// entitiesToCedar converts a parsed entitiesDefinitionJSON into a cedar-go
// EntityMap. A nil def (the field was omitted on the wire) returns an empty
// EntityMap -- cedar.Authorize treats that the same as "no entities
// supplied", matching the real API's optional-entities behavior.
func entitiesToCedar(def *entitiesDefinitionJSON) (cedar.EntityMap, error) {
	if def == nil {
		return cedar.EntityMap{}, nil
	}

	if def.CedarJSON != "" {
		var em cedar.EntityMap
		if err := json.Unmarshal([]byte(def.CedarJSON), &em); err != nil {
			return nil, fmt.Errorf("%w: invalid entities cedarJson: %w", errInvalidRequest, err)
		}

		return em, nil
	}

	em := make(cedar.EntityMap, len(def.EntityList))

	for _, item := range def.EntityList {
		if item.Identifier == nil {
			return nil, fmt.Errorf("%w: entity list item missing identifier", errInvalidRequest)
		}

		attrs, err := attributeMapToCedarRecord(item.Attributes)
		if err != nil {
			return nil, err
		}

		parentUIDs := make([]cedar.EntityUID, 0, len(item.Parents))
		for _, p := range item.Parents {
			parentUID := cedar.NewEntityUID(cedar.EntityType(p.EntityType), cedar.String(p.EntityID))
			parentUIDs = append(parentUIDs, parentUID)
		}

		uid := cedar.NewEntityUID(cedar.EntityType(item.Identifier.EntityType), cedar.String(item.Identifier.EntityID))
		em[uid] = cedar.Entity{UID: uid, Parents: cedar.NewEntityUIDSet(parentUIDs...), Attributes: attrs}
	}

	return em, nil
}

// contextToCedar converts a parsed contextDefinitionJSON into a cedar-go
// Record. A nil def returns the zero Record (empty context), matching the
// real API's optional-context behavior.
func contextToCedar(def *contextDefinitionJSON) (cedar.Record, error) {
	if def == nil {
		return cedar.Record{}, nil
	}

	if def.CedarJSON != "" {
		var rec cedar.Record
		if err := json.Unmarshal([]byte(def.CedarJSON), &rec); err != nil {
			return cedar.Record{}, fmt.Errorf("%w: invalid context cedarJson: %w", errInvalidRequest, err)
		}

		return rec, nil
	}

	return attributeMapToCedarRecord(def.ContextMap)
}
