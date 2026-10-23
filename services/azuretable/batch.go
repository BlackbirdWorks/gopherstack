package azuretable

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
)

// handleBatch serves POST /<account>/$batch: one multipart/mixed changeset applied atomically to one partition.
func (h *Handler) handleBatch(c *echo.Context) error {
	r := c.Request()
	if r.Method != http.MethodPost {
		return h.writeError(c, http.StatusMethodNotAllowed, "UnsupportedHttpVerb",
			"The resource doesn't support the specified HTTP verb.")
	}

	ops, parseErr := odatatable.ReadBatch(r, batchResource)
	if parseErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInput", "99:"+parseErr.Error())
	}

	if idx, failure := odatatable.ValidateBatch(ops); failure != nil {
		return h.writeBatchFailure(c, idx, *failure)
	}

	batchOps := make([]odatatable.BatchOp, len(ops))
	for i, o := range ops {
		batchOps[i] = o.Op
	}

	infos, err := h.Backend.ExecuteBatch(ops[0].Table, batchOps)
	if err != nil {
		if be, ok := errors.AsType[*odatatable.BatchError](err); ok {
			return h.writeBatchFailure(c, be.Index, odatatable.BatchFailureFor(be.Err))
		}

		return h.writeBatchFailure(c, 0,
			odatatable.NewBatchFailure(http.StatusInternalServerError, "InternalError", err.Error()))
	}

	return h.writeBatchSuccess(c, ops, infos)
}

// batchResource returns the resource segment of an embedded request path, dropping the account.
func batchResource(path string) string {
	_, resource := splitPath(path)

	return resource
}

func (h *Handler) writeBatchFailure(c *echo.Context, index int, f odatatable.BatchFailure) error {
	body, err := odatatable.FailureBody(index, f, newRequestID(), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
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
	parts := make([]odatatable.BatchResponsePart, len(ops))

	for i, o := range ops {
		part, err := h.batchSuccessPart(o, infos[i])
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
		}

		parts[i] = part
	}

	return h.writeBatchResponse(c, parts)
}

func (h *Handler) batchSuccessPart(
	o odatatable.WireBatchOp, info odatatable.EntityInfo,
) (odatatable.BatchResponsePart, error) {
	part := odatatable.BatchResponsePart{
		Status: http.StatusNoContent, ContentID: o.Headers.Get(odatatable.HeaderContentID),
	}

	if o.Op.Kind != odatatable.BatchDelete {
		part.Headers = append(part.Headers, [2]string{"ETag", info.ETag})
	}

	if o.Op.Kind != odatatable.BatchInsert || o.Headers.Get("Prefer") == preferReturnNoContent {
		return part, nil
	}

	level := odataLevelFromAccept(o.Headers.Get("Accept"))

	payload, err := json.Marshal(h.encodeEntity(info, o.Table, level, ""))
	if err != nil {
		return part, err
	}

	part.Status, part.Body = http.StatusCreated, payload
	part.Headers = append(part.Headers, [2]string{
		"Content-Type", "application/json;odata=" + level + ";streaming=true;charset=utf-8",
	})

	return part, nil
}

func (h *Handler) writeBatchResponse(c *echo.Context, parts []odatatable.BatchResponsePart) error {
	contentType, body := odatatable.RenderBatchResponse(parts, newRequestID)

	return c.Blob(http.StatusAccepted, contentType, body)
}
