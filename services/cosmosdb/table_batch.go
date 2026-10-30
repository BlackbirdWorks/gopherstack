package cosmosdb

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
)

const (
	headerContentType    = "Content-Type"
	errorPartContentType = "application/json;odata=minimalmetadata;streaming=true;charset=utf-8"
)

// handleTableBatch implements POST /$batch: one multipart/mixed changeset of insert, update, merge, delete and
// upsert operations on a single partition, applied atomically.
func (h *Handler) handleTableBatch(c *echo.Context) error {
	r := c.Request()
	if r.Method != http.MethodPost {
		return h.writeTableError(c, http.StatusMethodNotAllowed, "UnsupportedHttpVerb",
			"The resource doesn't support the specified HTTP verb.")
	}

	ops, parseErr := odatatable.ReadBatchOrRetrieve(r, func(p string) string { return strings.Trim(p, "/") })
	if parseErr != nil {
		return h.writeTableError(c, http.StatusBadRequest, "InvalidInput", "99:"+parseErr.Error())
	}

	switch ops[0].Op.Kind {
	case odatatable.BatchRetrieve:
		return h.writeBatchRetrieve(c, ops[0])
	case odatatable.BatchQuery:
		return h.writeBatchQuery(c, ops[0])
	case odatatable.BatchInsert, odatatable.BatchReplace, odatatable.BatchMerge, odatatable.BatchDelete:
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

// writeBatchRetrieve answers 202 with the single retrieve's own response (200 and the entity, or its error).
func (h *Handler) writeBatchRetrieve(c *echo.Context, o odatatable.WireBatchOp) error {
	info, err := h.TableBackend.GetEntity(o.Table, o.Op.PartitionKey, o.Op.RowKey)
	if err != nil {
		f := odatatable.BatchFailureFor(err)

		body, bodyErr := odatatable.FailureBody(0, f, newDocumentID(), time.Now().UTC().Format(time.RFC3339Nano))
		if bodyErr != nil {
			return h.writeTableError(c, http.StatusInternalServerError, "InternalError", bodyErr.Error())
		}

		return h.writeRetrievePart(c, odatatable.BatchResponsePart{
			Status:  f.Status,
			Headers: [][2]string{{headerContentType, errorPartContentType}},
			Body:    body,
		})
	}

	level := tableODataLevelFromAccept(o.Headers.Get("Accept"))

	payload, err := jsonMarshal(h.encodeTableEntity(info, o.Table, level, "", c.Request().Host))
	if err != nil {
		return h.writeTableError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	return h.writeRetrievePart(c, odatatable.BatchResponsePart{
		Status: http.StatusOK,
		Headers: [][2]string{
			{"ETag", info.ETag},
			{headerContentType, "application/json;odata=" + level + ";streaming=true;charset=utf-8"},
		},
		Body: payload,
	})
}

// writeBatchQuery answers 202 with the single entity-collection query's own response (200 and the feed, or its error).
func (h *Handler) writeBatchQuery(c *echo.Context, o odatatable.WireBatchOp) error {
	accept := o.Headers.Get("Accept")

	feed, next, failure := h.entityFeed(o.Table, o.Query, accept, c.Request().Host)
	if failure != nil {
		body, bodyErr := odatatable.FailureBody(0, *failure, newDocumentID(), time.Now().UTC().Format(time.RFC3339Nano))
		if bodyErr != nil {
			return h.writeTableError(c, http.StatusInternalServerError, "InternalError", bodyErr.Error())
		}

		return h.writeRetrievePart(c, odatatable.BatchResponsePart{
			Status:  failure.Status,
			Headers: [][2]string{{headerContentType, errorPartContentType}},
			Body:    body,
		})
	}

	payload, err := jsonMarshal(feed)
	if err != nil {
		return h.writeTableError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	headers := make([][2]string, 0, 1+len(next))
	headers = append(headers, [2]string{
		headerContentType,
		"application/json;odata=" + tableODataLevelFromAccept(accept) + ";streaming=true;charset=utf-8",
	})

	return h.writeRetrievePart(c, odatatable.BatchResponsePart{
		Status: http.StatusOK, Headers: append(headers, next...), Body: payload,
	})
}

func (h *Handler) writeRetrievePart(c *echo.Context, part odatatable.BatchResponsePart) error {
	contentType, body := odatatable.RenderRetrieveResponse(part, newDocumentID)

	return c.Blob(http.StatusAccepted, contentType, body)
}

// writeBatchFailure answers 202 with a changeset holding only the failing operation's response.
func (h *Handler) writeBatchFailure(c *echo.Context, index int, f odatatable.BatchFailure) error {
	body, err := odatatable.FailureBody(index, f, newDocumentID(), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return h.writeTableError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	part := odatatable.BatchResponsePart{
		Status:  f.Status,
		Headers: [][2]string{{headerContentType, errorPartContentType}},
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
