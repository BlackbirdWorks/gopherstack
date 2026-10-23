package cosmosdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// partitionKeyDefSpec is the "partitionKey" sub-object of a container definition:
// {"paths":["/pk"],"kind":"Hash"} or, hierarchical, {"paths":["/a","/b"],"kind":"MultiHash","version":2}.
type partitionKeyDefSpec struct {
	Kind  string   `json:"kind"`
	Paths []string `json:"paths"`
}

// containerBodyRequest is the request body shape for creating or replacing a container.
type containerBodyRequest struct {
	DefaultTTL     *int                `json:"defaultTtl"`
	ID             string              `json:"id"`
	PartitionKey   partitionKeyDefSpec `json:"partitionKey"`
	IndexingPolicy json.RawMessage     `json:"indexingPolicy"`

	ConflictResolutionPolicy json.RawMessage `json:"conflictResolutionPolicy"`

	ContainerPolicies
}

const partitionKindMultiHash = "MultiHash"

func (r containerBodyRequest) spec() ContainerSpec {
	spec := ContainerSpec{
		ID: r.ID, DefaultTTL: r.DefaultTTL, PartitionKeyPaths: r.PartitionKey.Paths, IndexingPolicy: r.IndexingPolicy,
		ConflictResolutionPolicy: r.ConflictResolutionPolicy, ContainerPolicies: r.ContainerPolicies,
	}
	if len(r.PartitionKey.Paths) > 0 {
		spec.PartitionKeyPath = r.PartitionKey.Paths[0]
	}

	return spec
}

// validate rejects an empty id/path list, and several paths declared with a kind other than MultiHash.
func (r containerBodyRequest) validate() error {
	if r.ID == "" {
		return errEmptyID
	}

	if len(r.PartitionKey.Paths) == 0 {
		return ErrInvalidPartitionKeyPath
	}

	if len(r.PartitionKey.Paths) > 1 && r.PartitionKey.Kind != partitionKindMultiHash {
		return fmt.Errorf("%w: several paths require kind %q", ErrInvalidPartitionKeyPath, partitionKindMultiHash)
	}

	return nil
}

func (h *Handler) handleContainers(c *echo.Context, dbID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		if isQueryRequest(c.Request()) {
			return h.queryContainers(c, dbID)
		}

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
	case http.MethodPut:
		return h.replaceContainer(c, dbID, collID)
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

// readContainerRequest decodes and validates the body; on failure it has already written the error response.
func (h *Handler) readContainerRequest(c *echo.Context) (containerBodyRequest, bool) {
	var req containerBodyRequest

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		_ = h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")

		return req, false
	}

	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		_ = h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")

		return req, false
	}

	if valErr := req.validate(); valErr != nil {
		_ = h.writeError(c, http.StatusBadRequest, "BadRequest", valErr.Error())

		return req, false
	}

	return req, true
}

func (h *Handler) createContainer(c *echo.Context, dbID string) error {
	req, ok := h.readContainerRequest(c)
	if !ok {
		return nil
	}

	spec, hasOffer, specOK := h.checkCreateThroughput(c, true)
	if !specOK {
		return nil
	}

	info, createErr := h.Backend.CreateContainer(dbID, req.spec())
	if createErr != nil {
		return h.writeContainerError(c, createErr)
	}

	if hasOffer {
		if offerErr := h.provisionOffer(c, dbID, info.ID, spec); offerErr != nil {
			return offerErr
		}
	}

	c.Response().Header().Set("ETag", containerETag(info))
	setCharge(c, ruMetadata)

	return h.writeJSON(c, http.StatusCreated, containerBody(info))
}

func (h *Handler) replaceContainer(c *echo.Context, dbID, collID string) error {
	req, ok := h.readContainerRequest(c)
	if !ok {
		return nil
	}

	if req.ID != collID {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The container id does not match the request path.")
	}

	replacer, canReplace := h.Backend.(interface {
		ReplaceContainer(dbID string, spec ContainerSpec) (ContainerInfo, error)
	})
	if !canReplace {
		return h.writeError(
			c,
			http.StatusMethodNotAllowed,
			"MethodNotAllowed",
			"The backend cannot replace containers.",
		)
	}

	info, replaceErr := replacer.ReplaceContainer(dbID, req.spec())
	if replaceErr != nil {
		return h.writeContainerError(c, replaceErr)
	}

	c.Response().Header().Set("ETag", containerETag(info))
	setCharge(c, ruMetadata)

	return h.writeJSON(c, http.StatusOK, containerBody(info))
}

func (h *Handler) writeContainerError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrDatabaseNotFound):
		return h.writeDatabaseNotFoundError(c)
	case errors.Is(err, ErrContainerNotFound):
		return h.writeContainerNotFoundError(c)
	case errors.Is(err, ErrContainerAlreadyExists):
		return h.writeError(c, http.StatusConflict, "Conflict", "A container with the specified id already exists.")
	case errors.Is(err, ErrInvalidPartitionKeyPath), errors.Is(err, ErrInvalidDefaultTTL),
		errors.Is(err, ErrPartitionKeyImmutable), errors.Is(err, ErrInvalidContainerPolicy),
		errors.Is(err, ErrUniqueKeyPolicyImmutable):
		return h.writeError(c, http.StatusBadRequest, "BadRequest", err.Error())
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

func (h *Handler) queryContainers(c *echo.Context, dbID string) error {
	infos, err := h.Backend.ListContainers(dbID)
	if err != nil {
		return h.writeDatabaseNotFoundError(c)
	}

	bodies := make([]map[string]any, 0, len(infos))
	for _, info := range infos {
		bodies = append(bodies, containerBody(info))
	}

	return h.queryResources(c, "DocumentCollections", bodies)
}

func (h *Handler) listContainers(c *echo.Context, dbID string) error {
	token, limit, reqErr := pageRequest(c.Request())
	if reqErr != nil {
		return h.writeContinuationError(c)
	}

	infos, err := h.Backend.ListContainers(dbID)
	if err != nil {
		return h.writeDatabaseNotFoundError(c)
	}

	pg := paginate(infos, token, limit)

	values := make([]map[string]any, 0, len(pg.Data))
	for _, info := range pg.Data {
		values = append(values, containerBody(info))
	}

	setPageHeaders(c, len(values), pg.Next)
	setCharge(c, ruMetadata)

	return h.writeJSON(c, http.StatusOK, map[string]any{
		sysPropRID: "", "DocumentCollections": values, sysPropCount: len(values),
	})
}

func (h *Handler) getContainer(c *echo.Context, dbID, collID string) error {
	info, err := h.Backend.GetContainer(dbID, collID)
	if err != nil {
		return h.writeContainerNotFoundError(c)
	}

	setCharge(c, ruMetadata)

	c.Response().Header().Set("ETag", containerETag(info))

	return h.writeJSON(c, http.StatusOK, containerBody(info))
}

func (h *Handler) deleteContainer(c *echo.Context, dbID, collID string) error {
	if err := h.Backend.DeleteContainer(dbID, collID); err != nil {
		return h.writeContainerNotFoundError(c)
	}

	return c.NoContent(http.StatusNoContent)
}

// containerETag is derived from the container's modification stamp, so a replace changes it.
func containerETag(info ContainerInfo) string {
	return resourceETag(info.Modified, info.RID)
}

// resourceETag formats a modification stamp as an ETag; a resource restored without one keeps its RID-based tag.
func resourceETag(modified time.Time, rid string) string {
	if modified.IsZero() {
		return `"` + rid + `"`
	}

	return etagFor(modified)
}

func resourceTS(modified time.Time) int64 {
	if modified.IsZero() {
		return 1
	}

	return modified.Unix()
}

// echoOrDefault returns raw, or def when raw is unset.
func echoOrDefault(raw json.RawMessage, def string) json.RawMessage {
	if len(raw) > 0 {
		return raw
	}

	return json.RawMessage(def)
}

// addPolicyFields echoes the container policies; unset unique-key and geospatial settings show their defaults.
func addPolicyFields(body map[string]any, p ContainerPolicies) {
	body["uniqueKeyPolicy"] = echoOrDefault(p.UniqueKeyPolicy, defaultUniqueKeyPol)
	body["geospatialConfig"] = echoOrDefault(p.GeospatialConfig, defaultGeoConfig)

	for key, raw := range map[string]json.RawMessage{
		"vectorEmbeddingPolicy": p.VectorEmbeddingPolicy, "fullTextPolicy": p.FullTextPolicy,
		"computedProperties": p.ComputedProperties,
	} {
		if len(raw) > 0 {
			body[key] = raw
		}
	}
}

// partitionKeyDefVersion is the "version" field real Cosmos DB stamps on
// every container's partitionKey definition (version 2 is the modern,
// hash-v2 partitioning scheme every current SDK/emulator combination uses).
const partitionKeyDefVersion = 2

// defaultIndexingPolicy is what the service reports for a container created without one.
const defaultIndexingPolicy = `{"indexingMode":"consistent","automatic":true,"includedPaths":[{"path":"/*"}],` +
	`"excludedPaths":[{"path":"/\"_etag\"/?"}]}`

// defaultConflictResolutionPolicy is reported for a container created without one (last-writer-wins on _ts).
const defaultConflictResolutionPolicy = `{"mode":"LastWriterWins","conflictResolutionPath":"/_ts",` +
	`"conflictResolutionProcedure":""}`

// containerBody builds a Cosmos DB container (DocumentCollection) resource's JSON response body.
func containerBody(info ContainerInfo) map[string]any {
	kind := "Hash"
	if len(info.PartitionKeyPaths) > 1 {
		kind = partitionKindMultiHash
	}

	indexing := json.RawMessage(defaultIndexingPolicy)
	if len(info.IndexingPolicy) > 0 {
		indexing = info.IndexingPolicy
	}

	body := map[string]any{
		"id":        info.ID,
		sysPropRID:  info.RID,
		sysPropSelf: collsSegment + "/" + info.RID + "/",
		sysPropETag: containerETag(info),
		sysPropTS:   resourceTS(info.Modified),
		"_docs":     "docs/",
		"partitionKey": map[string]any{
			"paths":   info.PartitionKeyPaths,
			"kind":    kind,
			"version": partitionKeyDefVersion,
		},
		"indexingPolicy": indexing,
	}

	if info.DefaultTTL != nil {
		body["defaultTtl"] = *info.DefaultTTL
	}

	addPolicyFields(body, info.ContainerPolicies)

	body["conflictResolutionPolicy"] = json.RawMessage(defaultConflictResolutionPolicy)
	if len(info.ConflictResolutionPolicy) > 0 {
		body["conflictResolutionPolicy"] = info.ConflictResolutionPolicy
	}

	return body
}

// writeContainerNotFoundError maps a missing-container StorageBackend error
// to the corresponding Cosmos error code/status.
func (h *Handler) writeContainerNotFoundError(c *echo.Context) error {
	return h.writeError(c, http.StatusNotFound, "NotFound", "Owner resource does not exist")
}
