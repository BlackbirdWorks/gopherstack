package personalize

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// --- Schema ---

func (h *Handler) createSchema(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	schema, _ := input["schema"].(string)
	domain, _ := input["domain"].(string)

	s, err := h.Backend.CreateSchema(name, schema, domain)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySchemaArn: s.SchemaArn}, nil
}

func (h *Handler) describeSchema(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["schemaArn"].(string)

	s, err := h.Backend.DescribeSchema(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"schema": schemaToMap(s)}, nil
}

func (h *Handler) deleteSchema(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["schemaArn"].(string)

	return map[string]any{}, h.Backend.DeleteSchema(nameOrArn)
}

func (h *Handler) listSchemas(input map[string]any) (map[string]any, error) {
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListSchemas(maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, s := range list {
		summaries = append(summaries, schemaSummaryToMap(s))
	}

	result := map[string]any{"schemas": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func schemaToMap(s *Schema) map[string]any {
	return map[string]any{
		keySchemaArn:           s.SchemaArn,
		keyName:                s.Name,
		"schema":               s.Schema,
		keyDomain:              s.Domain,
		keyCreationDateTime:    awstime.Epoch(s.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(s.LastUpdatedDateTime),
	}
}

// schemaSummaryToMap builds the types.DatasetSchemaSummary shape
// (types.go:1016) -- no schema body: ListSchemas never returns the full
// Avro text, only DescribeSchema does.
func schemaSummaryToMap(s *Schema) map[string]any {
	return map[string]any{
		keySchemaArn:           s.SchemaArn,
		keyName:                s.Name,
		keyDomain:              s.Domain,
		keyCreationDateTime:    awstime.Epoch(s.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(s.LastUpdatedDateTime),
	}
}
