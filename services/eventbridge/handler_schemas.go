package eventbridge

import (
	"context"
	"encoding/json"
)

func (h *Handler) schemaActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateSchema": func(ctx context.Context, b []byte) (any, error) {
			var input CreateSchemaInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateSchema(ctx, input)
		},
		"DeleteSchema": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
				SchemaName   string `json:"SchemaName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteSchema(ctx, input.RegistryName, input.SchemaName)
		},
		"DescribeSchema": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName  string `json:"RegistryName"`
				SchemaName    string `json:"SchemaName"`
				SchemaVersion string `json:"SchemaVersion"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeSchema(
				ctx,
				input.RegistryName,
				input.SchemaName,
				input.SchemaVersion,
			)
		},
		"ListSchemas": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName     string `json:"RegistryName"`
				SchemaNamePrefix string `json:"SchemaNamePrefix"`
				NextToken        string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			schemas, next, err := h.Backend.ListSchemas(
				ctx,
				input.RegistryName,
				input.SchemaNamePrefix,
				input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken string   `json:"NextToken,omitempty"`
				Schemas   []Schema `json:"Schemas"`
			}{Schemas: schemas, NextToken: next}, nil
		},
		"SearchSchemas": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
				Keywords     string `json:"Keywords"`
				NextToken    string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			schemas, next, err := h.Backend.SearchSchemas(
				ctx,
				input.RegistryName,
				input.Keywords,
				input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken string   `json:"NextToken,omitempty"`
				Schemas   []Schema `json:"Schemas"`
			}{Schemas: schemas, NextToken: next}, nil
		},
		"UpdateSchema": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateSchemaInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateSchema(ctx, input)
		},
	}
}

func (h *Handler) schemaVersionActions() map[string]actionFn {
	return map[string]actionFn{
		"ListSchemaVersions": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName string `json:"RegistryName"`
				SchemaName   string `json:"SchemaName"`
				NextToken    string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			versions, next, err := h.Backend.ListSchemaVersions(
				ctx,
				input.RegistryName,
				input.SchemaName,
				input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken      string          `json:"NextToken,omitempty"`
				SchemaVersions []SchemaVersion `json:"SchemaVersions"`
			}{SchemaVersions: versions, NextToken: next}, nil
		},
		"DescribeSchemaVersion": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName  string `json:"RegistryName"`
				SchemaName    string `json:"SchemaName"`
				SchemaVersion string `json:"SchemaVersion"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeSchemaVersion(
				ctx,
				input.RegistryName,
				input.SchemaName,
				input.SchemaVersion,
			)
		},
		"DeleteSchemaVersion": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName  string `json:"RegistryName"`
				SchemaName    string `json:"SchemaName"`
				SchemaVersion string `json:"SchemaVersion"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteSchemaVersion(
				ctx,
				input.RegistryName,
				input.SchemaName,
				input.SchemaVersion,
			)
		},
		"GetDiscoveredSchema": func(ctx context.Context, b []byte) (any, error) {
			var input GetDiscoveredSchemaInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			content, err := h.Backend.GetDiscoveredSchema(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				Content string `json:"Content"`
			}{Content: content}, nil
		},
	}
}

func (h *Handler) codeBindingActions() map[string]actionFn {
	return map[string]actionFn{
		"PutCodeBinding": func(ctx context.Context, b []byte) (any, error) {
			var input PutCodeBindingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutCodeBinding(ctx, input)
		},
		"DescribeCodeBinding": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeCodeBindingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeCodeBinding(ctx, input)
		},
		"ListCodeBindings": func(ctx context.Context, b []byte) (any, error) {
			var input ListCodeBindingsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bindings, next, err := h.Backend.ListCodeBindings(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken    string        `json:"NextToken,omitempty"`
				CodeBindings []CodeBinding `json:"CodeBindings"`
			}{CodeBindings: bindings, NextToken: next}, nil
		},
		"GetCodeBindingSource": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				RegistryName  string `json:"RegistryName"`
				SchemaName    string `json:"SchemaName"`
				Language      string `json:"Language"`
				SchemaVersion string `json:"SchemaVersion"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			src, err := h.Backend.GetCodeBindingSource(
				ctx,
				input.RegistryName, input.SchemaName, input.Language, input.SchemaVersion,
			)
			if err != nil {
				return nil, err
			}

			return &struct {
				Body string `json:"Body"`
			}{Body: src}, nil
		},
	}
}
