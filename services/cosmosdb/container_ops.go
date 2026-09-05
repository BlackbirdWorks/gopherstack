package cosmosdb

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// partitionKeyDefSpec is the "partitionKey" sub-object shape a Create
// Container request body carries: {"paths":["/pk"],"kind":"Hash"}.
// Hierarchical (multi-path) partition keys are out of scope -- see
// ErrInvalidPartitionKeyPath.
type partitionKeyDefSpec struct {
	Kind  string   `json:"kind"`
	Paths []string `json:"paths"`
}

// createContainerBody is the request body shape for POST /dbs/{db}/colls.
type createContainerBody struct {
	ID           string              `json:"id"`
	PartitionKey partitionKeyDefSpec `json:"partitionKey"`
}

func (h *Handler) handleContainers(c *echo.Context, dbID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createContainer(c, dbID)
	case http.MethodGet:
		return h.listContainers(c, dbID)
	default:
		return h.writeError(
			c,
			http.StatusMethodNotAllowed,
			"MethodNotAllowed",
			"The resource doesn't support the specified HTTP verb.",
		)
	}
}

func (h *Handler) handleContainerItem(c *echo.Context, dbID, collID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getContainer(c, dbID, collID)
	case http.MethodDelete:
		return h.deleteContainer(c, dbID, collID)
	default:
		return h.writeError(
			c,
			http.StatusMethodNotAllowed,
			"MethodNotAllowed",
			"The resource doesn't support the specified HTTP verb.",
		)
	}
}

func (h *Handler) createContainer(c *echo.Context, dbID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var req createContainerBody
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")
	}

	if req.ID == "" {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "id must not be empty.")
	}

	if len(req.PartitionKey.Paths) != 1 {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", ErrInvalidPartitionKeyPath.Error())
	}

	spec := ContainerSpec{ID: req.ID, PartitionKeyPath: req.PartitionKey.Paths[0]}

	info, createErr := h.Backend.CreateContainer(dbID, spec)

	switch {
	case createErr == nil:
	case errors.Is(createErr, ErrDatabaseNotFound):
		return h.writeDatabaseNotFoundError(c)
	case errors.Is(createErr, ErrContainerAlreadyExists):
		return h.writeError(c, http.StatusConflict, "Conflict", "A container with the specified id already exists.")
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", createErr.Error())
	}

	c.Response().Header().Set("ETag", containerETag(info))

	return h.writeJSON(c, http.StatusCreated, containerBody(info))
}

func (h *Handler) listContainers(c *echo.Context, dbID string) error {
	infos, err := h.Backend.ListContainers(dbID)
	if err != nil {
		return h.writeDatabaseNotFoundError(c)
	}

	values := make([]map[string]any, 0, len(infos))
	for _, info := range infos {
		values = append(values, containerBody(info))
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{
		sysPropRID: "", "DocumentCollections": values, sysPropCount: len(values),
	})
}

func (h *Handler) getContainer(c *echo.Context, dbID, collID string) error {
	info, err := h.Backend.GetContainer(dbID, collID)
	if err != nil {
		return h.writeContainerNotFoundError(c)
	}

	c.Response().Header().Set("ETag", containerETag(info))

	return h.writeJSON(c, http.StatusOK, containerBody(info))
}

func (h *Handler) deleteContainer(c *echo.Context, dbID, collID string) error {
	if err := h.Backend.DeleteContainer(dbID, collID); err != nil {
		return h.writeContainerNotFoundError(c)
	}

	return c.NoContent(http.StatusNoContent)
}

// containerETag returns a static, non-versioned ETag for a container. See
// databaseETag's identical rationale -- there is no "update container"
// operation in this milestone.
func containerETag(info ContainerInfo) string {
	return `"` + info.RID + `"`
}

// partitionKeyDefVersion is the "version" field real Cosmos DB stamps on
// every container's partitionKey definition (version 2 is the modern,
// hash-v2 partitioning scheme every current SDK/emulator combination uses).
const partitionKeyDefVersion = 2

// containerBody builds a Cosmos DB container (DocumentCollection) resource's
// JSON response body.
func containerBody(info ContainerInfo) map[string]any {
	return map[string]any{
		"id":        info.ID,
		sysPropRID:  info.RID,
		sysPropSelf: collsSegment + "/" + info.RID + "/",
		sysPropETag: containerETag(info),
		sysPropTS:   1,
		"_docs":     "docs/",
		"partitionKey": map[string]any{
			"paths":   []string{info.PartitionKeyPath},
			"kind":    "Hash",
			"version": partitionKeyDefVersion,
		},
	}
}

// writeContainerNotFoundError maps a missing-container StorageBackend error
// to the corresponding Cosmos error code/status.
func (h *Handler) writeContainerNotFoundError(c *echo.Context) error {
	return h.writeError(c, http.StatusNotFound, "NotFound", "Owner resource does not exist")
}
