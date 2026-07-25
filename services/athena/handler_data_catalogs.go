package athena

import "encoding/json"

// dataCatalogRespKey is the JSON response key CreateDataCatalog,
// GetDataCatalog, and DeleteDataCatalog all wrap their DataCatalog payload
// in.
const dataCatalogRespKey = "DataCatalog"

type createDataCatalogInput struct {
	Parameters     map[string]string `json:"Parameters"`
	Name           string            `json:"Name"`
	Type           string            `json:"Type"`
	Description    string            `json:"Description"`
	ConnectionType string            `json:"ConnectionType"`
	Tags           []Tag             `json:"Tags"`
}

type listDataCatalogsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type getDataCatalogInput struct {
	Name string `json:"Name"`
}

type updateDataCatalogInput struct {
	Parameters     map[string]string `json:"Parameters"`
	Name           string            `json:"Name"`
	Type           string            `json:"Type"`
	Description    string            `json:"Description"`
	ConnectionType string            `json:"ConnectionType"`
}

type deleteDataCatalogInput struct {
	Name string `json:"Name"`
}

func (h *Handler) dataCatalogOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"CreateDataCatalog": func(b []byte) (any, error) {
			var input createDataCatalogInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			dc, err := h.Backend.CreateDataCatalog(
				input.Name,
				input.Type,
				input.Description,
				input.ConnectionType,
				input.Parameters,
				tagsFromSlice(input.Tags),
			)
			if err != nil {
				return nil, err
			}

			return map[string]any{dataCatalogRespKey: dc}, nil
		},
		"GetDataCatalog": func(b []byte) (any, error) {
			var input getDataCatalogInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			dc, err := h.Backend.GetDataCatalog(input.Name)
			if err != nil {
				return nil, err
			}

			return map[string]any{dataCatalogRespKey: dc}, nil
		},
		"ListDataCatalogs": func(b []byte) (any, error) {
			var input listDataCatalogsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			list, nextToken, err := h.Backend.ListDataCatalogs(input.NextToken, input.MaxResults)
			if err != nil {
				return nil, err
			}

			resp := map[string]any{"DataCatalogsSummary": list}
			if nextToken != "" {
				resp["NextToken"] = nextToken
			}

			return resp, nil
		},
		"UpdateDataCatalog": func(b []byte) (any, error) {
			var input updateDataCatalogInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateDataCatalog(
				input.Name, input.Type, input.Description, input.ConnectionType, input.Parameters,
			)
		},
		"DeleteDataCatalog": func(b []byte) (any, error) {
			var input deleteDataCatalogInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			dc, err := h.Backend.DeleteDataCatalog(input.Name)
			if err != nil {
				return nil, err
			}

			return map[string]any{dataCatalogRespKey: dc}, nil
		},
	}
}
