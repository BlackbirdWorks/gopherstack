package athena

import "encoding/json"

// createNotebookInput intentionally has no Tags field: the real
// CreateNotebookInput carries only Name/WorkGroup/ClientRequestToken -- see
// InMemoryBackend.CreateNotebook's doc comment.
type createNotebookInput struct {
	WorkGroup string `json:"WorkGroup"`
	Name      string `json:"Name"`
}

type createPresignedNotebookURLInput struct {
	SessionID string `json:"SessionId"`
}

type deleteNotebookInput struct {
	NotebookID string `json:"NotebookId"`
}

type exportNotebookInput struct {
	NotebookID string `json:"NotebookId"`
}

type notebookIDInput struct {
	NotebookID string `json:"NotebookId"`
}

type listNotebookMetadataInput struct {
	WorkGroup string                     `json:"WorkGroup"`
	Filters   listNotebookMetadataFilter `json:"Filters"`
}

type listNotebookMetadataFilter struct {
	Name string `json:"Name"`
}

type importNotebookInput struct {
	WorkGroup string `json:"WorkGroup"`
	Name      string `json:"Name"`
	Payload   string `json:"Payload"`
	Type      string `json:"Type"`
}

type updateNotebookInput struct {
	NotebookID string `json:"NotebookId"`
	Payload    string `json:"Payload"`
	Type       string `json:"Type"`
	SessionID  string `json:"SessionId"`
}

type updateNotebookMetadataInput struct {
	NotebookID string `json:"NotebookId"`
	Name       string `json:"Name"`
}

func (h *Handler) notebookOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"CreateNotebook": func(b []byte) (any, error) {
			var input createNotebookInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			id, err := h.Backend.CreateNotebook(input.WorkGroup, input.Name)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NotebookId": id}, nil
		},
		"CreatePresignedNotebookUrl": func(b []byte) (any, error) {
			var input createPresignedNotebookURLInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			url, authToken, authTokenExpiration, err := h.Backend.CreatePresignedNotebookURL(input.SessionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				"NotebookUrl":             url,
				"AuthToken":               authToken,
				"AuthTokenExpirationTime": authTokenExpiration,
			}, nil
		},
		"DeleteNotebook": func(b []byte) (any, error) {
			var input deleteNotebookInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteNotebook(input.NotebookID)
		},
		"ExportNotebook": func(b []byte) (any, error) {
			var input exportNotebookInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			metadata, content, err := h.Backend.ExportNotebook(input.NotebookID)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				"NotebookMetadata": metadata,
				"Payload":          content,
			}, nil
		},
	}
}

func (h *Handler) notebookExtraOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"GetNotebookMetadata": func(b []byte) (any, error) {
			var input notebookIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			meta, err := h.Backend.GetNotebookMetadata(input.NotebookID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NotebookMetadata": meta}, nil
		},
		"ListNotebookMetadata": func(b []byte) (any, error) {
			var input listNotebookMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			list, err := h.Backend.ListNotebookMetadata(input.WorkGroup, input.Filters.Name)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NotebookMetadataList": list}, nil
		},
		"ImportNotebook": func(b []byte) (any, error) {
			var input importNotebookInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			id, err := h.Backend.ImportNotebook(input.WorkGroup, input.Name, input.Payload, input.Type)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NotebookId": id}, nil
		},
		"UpdateNotebook": func(b []byte) (any, error) {
			var input updateNotebookInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateNotebook(input.NotebookID, input.Payload, input.Type, input.SessionID)
		},
		"UpdateNotebookMetadata": func(b []byte) (any, error) {
			var input updateNotebookMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateNotebookMetadata(input.NotebookID, input.Name)
		},
	}
}
