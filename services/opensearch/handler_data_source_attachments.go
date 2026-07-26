package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// Sub-path segments for the four data source attachment routes under
// /application/{id}/..., shared between routing (here and
// handler_applications.go/handler_operations.go) and operation-name
// extraction.
const (
	subPathAttachDataSource             = "attachDataSource"
	subPathDetachDataSource             = "detachDataSource"
	subPathDescribeDataSourceAttachment = "describeDataSourceAttachment"
	subPathListDataSourceAttachments    = "listDataSourceAttachments"
)

// dataSourceAttachmentJSON is the shared wire shape for
// Attach/Detach/DescribeDataSourceAttachment's outputs. Field-diffed against
// AttachDataSourceOutput/DetachDataSourceOutput/DescribeDataSourceAttachmentOutput,
// all three of which carry the identical {arn, attachmentId, dataSourceArn,
// id, status} shape. "arn" and "dataSourceArn" are documented with the exact
// same field description ("The Amazon Resource Name (ARN) of the domain") in
// the real API reference -- there is no other candidate value for "arn", so
// it mirrors dataSourceArn here rather than inventing a distinct meaning.
type dataSourceAttachmentJSON struct {
	Arn           string `json:"arn"`
	AttachmentID  string `json:"attachmentId"`
	DataSourceArn string `json:"dataSourceArn"`
	ID            string `json:"id"`
	Status        string `json:"status"`
}

func toDataSourceAttachmentJSON(att *DataSourceAttachment) dataSourceAttachmentJSON {
	return dataSourceAttachmentJSON{
		Arn:           att.DataSourceArn,
		AttachmentID:  att.AttachmentID,
		DataSourceArn: att.DataSourceArn,
		ID:            att.ApplicationID,
		Status:        att.Status,
	}
}

// dataSourceAttachmentRequest is the shared JSON request body for
// attach/detach/describe (each only needs dataSourceArn; AttachDataSource's
// optional workspaceConfiguration/workspaceId are accepted but not modeled --
// this backend has no workspace resource to create or link).
type dataSourceAttachmentRequest struct {
	ClientToken   string `json:"clientToken"`
	DataSourceArn string `json:"dataSourceArn"`
}

// handleDataSourceAttachmentRoutes handles the four
// /application/{id}/{attachDataSource,detachDataSource,
// describeDataSourceAttachment,listDataSourceAttachments} routes, all POST
// per the real API (confirmed via serializers.go: every one of these ops
// serializes as POST despite Describe/List being read-only).
func (h *Handler) handleDataSourceAttachmentRoutes(
	w http.ResponseWriter,
	r *http.Request,
	appID, subPath string,
) {
	if r.Method != http.MethodPost {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	switch subPath {
	case subPathAttachDataSource:
		h.handleAttachDataSource(w, r, appID)
	case subPathDetachDataSource:
		h.handleDetachDataSource(w, r, appID)
	case subPathDescribeDataSourceAttachment:
		h.handleDescribeDataSourceAttachment(w, r, appID)
	case subPathListDataSourceAttachments:
		h.handleListDataSourceAttachments(w, r, appID)
	}
}

func (h *Handler) readDataSourceAttachmentRequest(
	w http.ResponseWriter,
	r *http.Request,
) (dataSourceAttachmentRequest, bool) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return dataSourceAttachmentRequest{}, false
	}

	var req dataSourceAttachmentRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return dataSourceAttachmentRequest{}, false
		}
	}

	return req, true
}

func (h *Handler) handleAttachDataSource(w http.ResponseWriter, r *http.Request, appID string) {
	req, ok := h.readDataSourceAttachmentRequest(w, r)
	if !ok {
		return
	}

	att, err := h.Backend.AttachDataSource(appID, req.DataSourceArn)
	if err != nil {
		h.writeAttachmentError(r, w, err)

		return
	}

	h.writeJSON(r, w, toDataSourceAttachmentJSON(att))
}

func (h *Handler) handleDetachDataSource(w http.ResponseWriter, r *http.Request, appID string) {
	req, ok := h.readDataSourceAttachmentRequest(w, r)
	if !ok {
		return
	}

	att, err := h.Backend.DetachDataSource(appID, req.DataSourceArn)
	if err != nil {
		h.writeAttachmentError(r, w, err)

		return
	}

	h.writeJSON(r, w, toDataSourceAttachmentJSON(att))
}

func (h *Handler) handleDescribeDataSourceAttachment(w http.ResponseWriter, r *http.Request, appID string) {
	req, ok := h.readDataSourceAttachmentRequest(w, r)
	if !ok {
		return
	}

	att, err := h.Backend.DescribeDataSourceAttachment(appID, req.DataSourceArn)
	if err != nil {
		h.writeAttachmentError(r, w, err)

		return
	}

	h.writeJSON(r, w, toDataSourceAttachmentJSON(att))
}

func (h *Handler) handleListDataSourceAttachments(w http.ResponseWriter, r *http.Request, appID string) {
	attachments := h.Backend.ListDataSourceAttachments(appID)

	items := make([]dataSourceAttachmentSummaryJSON, 0, len(attachments))
	for _, att := range attachments {
		items = append(items, dataSourceAttachmentSummaryJSON{
			AttachmentID:  att.AttachmentID,
			DataSourceArn: att.DataSourceArn,
			Status:        att.Status,
		})
	}

	h.writeJSON(r, w, map[string]any{"attachments": items})
}

// dataSourceAttachmentSummaryJSON matches types.DataSourceAttachmentSummary
// (the element shape of ListDataSourceAttachmentsOutput.Attachments), which
// carries only attachmentId/dataSourceArn/status -- no "arn" or "id" field,
// unlike the single-attachment ops above.
type dataSourceAttachmentSummaryJSON struct {
	AttachmentID  string `json:"attachmentId"`
	DataSourceArn string `json:"dataSourceArn"`
	Status        string `json:"status"`
}

// writeAttachmentError maps the data source attachment family's backend
// errors to their documented HTTP status codes. This newer
// "OpenSearch application" API surface documents ResourceNotFoundException at
// HTTP 409 (confirmed against the live AWS API reference for AttachDataSource,
// RegisterCapability, StartMigration, and RollbackServiceSoftwareUpdate) --
// deliberately different from the classic domain-family convention (404)
// used elsewhere in this handler.
func (h *Handler) writeAttachmentError(r *http.Request, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrApplicationNotFound),
		errors.Is(err, ErrDataSourceNotFound),
		errors.Is(err, ErrAttachmentNotFound):
		h.writeError(r, w, http.StatusConflict, "ResourceNotFoundException", err.Error())
	case errors.Is(err, ErrAttachmentConflict):
		h.writeError(r, w, http.StatusConflict, "ConflictException", err.Error())
	default:
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
	}
}
