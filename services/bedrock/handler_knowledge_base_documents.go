package bedrock

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

const respDocumentDetails = "documentDetails"

// wireS3Location matches bedrockagent@v1.58.4 types.S3Location (uri only).
type wireS3Location struct {
	URI string `json:"uri"`
}

// wireCustomDocumentIdentifier matches types.CustomDocumentIdentifier.
type wireCustomDocumentIdentifier struct {
	ID string `json:"id"`
}

// wireDocumentIdentifier matches types.DocumentIdentifier: dataSourceType
// plus exactly one of s3/custom, used by GetKnowledgeBaseDocuments,
// DeleteKnowledgeBaseDocuments request input and KnowledgeBaseDocumentDetail
// response output (serializers.go:8192, deserializers.go's
// awsRestjson1_deserializeDocumentDocumentIdentifier).
type wireDocumentIdentifier struct {
	S3             *wireS3Location               `json:"s3,omitempty"`
	Custom         *wireCustomDocumentIdentifier `json:"custom,omitempty"`
	DataSourceType string                        `json:"dataSourceType"`
}

func (w wireDocumentIdentifier) toIdentifier() KBDocumentIdentifier {
	ident := KBDocumentIdentifier{DataSourceType: w.DataSourceType}
	if w.S3 != nil {
		ident.S3URI = w.S3.URI
	}

	if w.Custom != nil {
		ident.CustomID = w.Custom.ID
	}

	return ident
}

func documentIdentifierWire(doc *KnowledgeBaseDocument) wireDocumentIdentifier {
	ident := wireDocumentIdentifier{DataSourceType: doc.DataSourceType}
	if doc.DataSourceType == "CUSTOM" {
		ident.Custom = &wireCustomDocumentIdentifier{ID: doc.DocumentID}
	} else {
		ident.S3 = &wireS3Location{URI: doc.DocumentID}
	}

	return ident
}

// wireKnowledgeBaseDocumentDetail matches types.KnowledgeBaseDocumentDetail
// (deserializers.go:21354): dataSourceId, identifier, knowledgeBaseId,
// status, statusReason, updatedAt -- never the flat documentId/dataSourceType
// shape this package used to emit.
type wireKnowledgeBaseDocumentDetail struct {
	DataSourceID    string                 `json:"dataSourceId"`
	Identifier      wireDocumentIdentifier `json:"identifier"`
	KnowledgeBaseID string                 `json:"knowledgeBaseId"`
	Status          string                 `json:"status"`
	StatusReason    string                 `json:"statusReason,omitempty"`
	UpdatedAt       string                 `json:"updatedAt,omitempty"`
}

func documentDetailWire(doc *KnowledgeBaseDocument) wireKnowledgeBaseDocumentDetail {
	detail := wireKnowledgeBaseDocumentDetail{
		DataSourceID:    doc.DataSourceID,
		Identifier:      documentIdentifierWire(doc),
		KnowledgeBaseID: doc.KnowledgeBaseID,
		Status:          doc.Status,
		StatusReason:    doc.StatusReason,
	}

	if !doc.UpdatedAt.IsZero() {
		detail.UpdatedAt = doc.UpdatedAt.Format("2006-01-02T15:04:05.999999999Z")
	}

	return detail
}

func documentDetailsWire(docs []*KnowledgeBaseDocument) []wireKnowledgeBaseDocumentDetail {
	out := make([]wireKnowledgeBaseDocumentDetail, 0, len(docs))
	for _, doc := range docs {
		out = append(out, documentDetailWire(doc))
	}

	return out
}

// dispatchDocumentOps handles the .../documents collection path. The real
// bedrock-agent wire distinguishes IngestKnowledgeBaseDocuments and
// ListKnowledgeBaseDocuments by HTTP method alone (both share this exact
// path): Ingest is PUT, List is POST. GET is accepted too as harmless extra
// leniency (no real client sends it). GetKnowledgeBaseDocuments and
// DeleteKnowledgeBaseDocuments are real, but on the /getDocuments and
// /deleteDocuments sub-paths respectively -- both are carved out by the
// caller (dispatchDataSourceIDRoutes, handler_data_sources.go) before this
// function is ever reached, so dsSuffix here is always exactly "/documents".
func (h *AgentsHandler) dispatchDocumentOps(
	c *echo.Context, kbID, dsID, dsSuffix, method string, body []byte,
) error {
	if dsSuffix == suffixDocuments {
		switch method {
		case http.MethodPut:
			return h.handleIngestKBDocuments(c, kbID, dsID, body)
		case http.MethodPost, http.MethodGet:
			return h.handleListKBDocuments(c, kbID, dsID)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown documents operation"),
	)
}

// wireDocumentContent matches types.DocumentContent (serializers.go:8166):
// dataSourceType plus s3.s3Location.uri or custom.customDocumentIdentifier.id.
// InlineContent and non-URI custom sources are not modeled -- this backend
// never fetches document bytes.
type wireDocumentContent struct {
	S3             *wireS3Content     `json:"s3,omitempty"`
	Custom         *wireCustomContent `json:"custom,omitempty"`
	DataSourceType string             `json:"dataSourceType"`
}

type wireS3Content struct {
	S3Location wireS3Location `json:"s3Location"`
}

type wireCustomContent struct {
	CustomDocumentIdentifier wireCustomDocumentIdentifier `json:"customDocumentIdentifier"`
}

func (w wireDocumentContent) toIdentifier() KBDocumentIdentifier {
	ident := KBDocumentIdentifier{DataSourceType: w.DataSourceType}
	if w.S3 != nil {
		ident.S3URI = w.S3.S3Location.URI
	}

	if w.Custom != nil {
		ident.CustomID = w.Custom.CustomDocumentIdentifier.ID
	}

	return ident
}

func (h *AgentsHandler) handleIngestKBDocuments(
	c *echo.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		Documents []struct {
			Content wireDocumentContent `json:"content"`
		} `json:"documents"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	identifiers := make([]KBDocumentIdentifier, 0, len(req.Documents))
	for _, d := range req.Documents {
		identifiers = append(identifiers, d.Content.toIdentifier())
	}

	docs, err := h.Backend.IngestKnowledgeBaseDocuments(kbID, dsID, identifiers)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respDocumentDetails: documentDetailsWire(docs)})
}

func (h *AgentsHandler) handleListKBDocuments(c *echo.Context, kbID, dsID string) error {
	list, outToken := h.Backend.ListKnowledgeBaseDocuments(kbID, dsID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{respDocumentDetails: documentDetailsWire(list)}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleGetKBDocuments(
	c *echo.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		DocumentIdentifiers []wireDocumentIdentifier `json:"documentIdentifiers"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", "invalid request body"))
	}

	identifiers := make([]KBDocumentIdentifier, 0, len(req.DocumentIdentifiers))
	for _, w := range req.DocumentIdentifiers {
		identifiers = append(identifiers, w.toIdentifier())
	}

	docs, err := h.Backend.GetKnowledgeBaseDocuments(kbID, dsID, identifiers)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respDocumentDetails: documentDetailsWire(docs)})
}

func (h *AgentsHandler) handleDeleteKBDocuments(
	c *echo.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		DocumentIdentifiers []wireDocumentIdentifier `json:"documentIdentifiers"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	identifiers := make([]KBDocumentIdentifier, 0, len(req.DocumentIdentifiers))
	for _, w := range req.DocumentIdentifiers {
		identifiers = append(identifiers, w.toIdentifier())
	}

	if err := h.Backend.DeleteKnowledgeBaseDocuments(kbID, dsID, identifiers); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
