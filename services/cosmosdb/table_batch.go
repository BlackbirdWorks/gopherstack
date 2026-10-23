package cosmosdb

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
)

// handleTableBatch implements POST /$batch: one multipart/mixed changeset of insert, update, merge, delete and
// upsert operations on a single partition, applied atomically.
func (h *Handler) handleTableBatch(c *echo.Context) error {
	r := c.Request()
	if r.Method != http.MethodPost {
		return h.writeTableError(c, http.StatusMethodNotAllowed, "UnsupportedHttpVerb",
			"The resource doesn't support the specified HTTP verb.")
	}

	ops, parseErr := odatatable.ReadBatch(r, func(p string) string { return strings.Trim(p, "/") })
	if parseErr != nil {
		return h.writeTableError(c, http.StatusBadRequest, "InvalidInput", "99:"+parseErr.Error())
	}

	if idx, failure := odatatable.ValidateBatch(ops); failure != nil {
		return h.writeBatchFailure(c, idx, *failure)
	}

	batchOps := make([]odatatable.BatchOp, len(ops))
	for i, o := range ops {
		batchOps[i] = o.Op
	}

	infos, err := h.TableBackend.ExecuteBatch(ops[0].Table, batchOps)
	if err != nil {
		if be, ok := errors.AsType[*odatatable.BatchError](err); ok {
			return h.writeBatchFailure(c, be.Index, odatatable.BatchFailureFor(be.Err))
		}

		return h.writeBatchFailure(c, 0,
			odatatable.NewBatchFailure(http.StatusInternalServerError, "InternalError", err.Error()))
	}

	return h.writeBatchSuccess(c, ops, infos)
}

// writeBatchFailure answers 202 with a changeset holding only the failing operation's response.
func (h *Handler) writeBatchFailure(c *echo.Context, index int, f odatatable.BatchFailure) error {
	body, err := odatatable.FailureBody(index, f, newDocumentID(), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return h.writeTableError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	part := odatatable.BatchResponsePart{
		Status:  f.Status,
		Headers: [][2]string{{"Content-Type", "application/json;odata=minimalmetadata;streaming=true;charset=utf-8"}},
		Body:    body,
	}

	return h.writeBatchResponse(c, []odatatable.BatchResponsePart{part})
}

func (h *Handler) writeBatchSuccess(
	c *echo.Context, ops []odatatable.WireBatchOp, infos []odatatable.EntityInfo,
) error {
	r := c.Request()
	parts := make([]odatatable.BatchResponsePart, len(ops))

	for i, o := range ops {
		part := odatatable.BatchResponsePart{
			Status: http.StatusNoContent, ContentID: o.Headers.Get(odatatable.HeaderContentID),
		}

		if o.Op.Kind != odatatable.BatchDelete {
			part.Headers = append(part.Headers, [2]string{"ETag", infos[i].ETag})
		}

		if o.Op.Kind == odatatable.BatchInsert && o.Headers.Get("Prefer") != preferReturnNoContent {
			level := tableODataLevelFromAccept(o.Headers.Get("Accept"))

			payload, err := jsonMarshal(h.encodeTableEntity(infos[i], o.Table, level, "", r.Host))
			if err != nil {
				return h.writeTableError(c, http.StatusInternalServerError, "InternalError", err.Error())
			}

			part.Status, part.Body = http.StatusCreated, payload
			part.Headers = append(part.Headers, [2]string{
				"Content-Type", "application/json;odata=" + level + ";streaming=true;charset=utf-8",
			})
		}

		parts[i] = part
	}

	return h.writeBatchResponse(c, parts)
}

func (h *Handler) writeBatchResponse(c *echo.Context, parts []odatatable.BatchResponsePart) error {
	contentType, body := odatatable.RenderBatchResponse(parts, newDocumentID)

	return c.Blob(http.StatusAccepted, contentType, body)
}
