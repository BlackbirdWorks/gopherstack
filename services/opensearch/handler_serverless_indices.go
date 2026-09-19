package opensearch

import "fmt"

// Real AOSS index-family JSON-RPC handlers (opensearchserverless@v1.34.4
// api_op_{Create,Get,Update,Delete}Index.go). CreateIndexOutput/
// UpdateIndexOutput/DeleteIndexOutput carry no members besides
// ResultMetadata, so each returns an empty body; GetIndexOutput's only
// member is indexSchema (a smithy document, embedded as real JSON rather
// than a quoted string -- verified against deserializers.go's
// awsAwsjson10_deserializeOpDocumentGetIndexOutput).

// decodeIndexSchemaJR validates IndexSchema is a JSON object when present
// (document.Interface on the wire arrives as an already-decoded map, since
// the JSON-RPC layer decodes the whole request body generically before
// dispatch) and stores it verbatim. An absent schema decodes to an empty,
// non-nil map (rather than nil) purely so this never returns a (nil, nil)
// pair -- ServerlessIndex.IndexSchema's `omitempty` tag treats an empty map
// the same as a nil one on the wire, so this has no observable effect.
func decodeIndexSchemaJR(raw any) (map[string]any, error) {
	if raw == nil {
		return map[string]any{}, nil
	}

	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: IndexSchema must be a JSON object", ErrInvalidParameter)
	}

	return m, nil
}

func (h *Handler) jrCreateIndex(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)
	indexName, _ := input["indexName"].(string)

	schema, err := decodeIndexSchemaJR(input["indexSchema"])
	if err != nil {
		return nil, err
	}

	if _, createErr := h.Backend.CreateServerlessIndex(id, indexName, schema); createErr != nil {
		return nil, createErr
	}

	return map[string]any{}, nil
}

func (h *Handler) jrGetIndex(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)
	indexName, _ := input["indexName"].(string)

	idx, err := h.Backend.GetServerlessIndex(id, indexName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"indexSchema": idx.IndexSchema}, nil
}

func (h *Handler) jrUpdateIndex(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)
	indexName, _ := input["indexName"].(string)

	schema, err := decodeIndexSchemaJR(input["indexSchema"])
	if err != nil {
		return nil, err
	}

	if _, updateErr := h.Backend.UpdateServerlessIndex(id, indexName, schema); updateErr != nil {
		return nil, updateErr
	}

	return map[string]any{}, nil
}

func (h *Handler) jrDeleteIndex(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)
	indexName, _ := input["indexName"].(string)

	if err := h.Backend.DeleteServerlessIndex(id, indexName); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}
