package cosmosdb

import (
	"errors"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
)

// encodeTableEntity builds an entity's OData JSON response body, delegating
// to pkgs/odatatable's shared codec -- see that package's EncodeEntity for
// the full contract. Mirrors services/azuretable/entity_ops.go's identical
// (Handler-bound) encodeEntity wrapper.
func (h *Handler) encodeTableEntity(info odatatable.EntityInfo, table, level, selectParam, host string) map[string]any {
	m := odatatable.EncodeEntity(info, table, level, selectParam, tableServiceEndpoint(host), tableAPIAccountName)

	if selected := odatatable.SelectSet(selectParam); selected != nil {
		keys := [...]string{odatatable.PartitionKeyProperty, odatatable.RowKeyProperty, odatatable.TimestampProperty}
		for _, key := range keys {
			if !selected[key] {
				delete(m, key)
			}
		}
	}

	return m
}

func (h *Handler) insertEntity(c *echo.Context, table string) error {
	r := c.Request()

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeTableError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	partitionKey, rowKey, hasPK, hasRK, props, decErr := odatatable.DecodeEntityBody(body)
	if decErr != nil {
		return h.writeTableError(c, http.StatusBadRequest, "InvalidInput", "One of the request inputs is not valid.")
	}

	if !hasPK || !hasRK {
		return h.writeTableError(c, http.StatusBadRequest, "InvalidInput",
			"The values are not specified for all the key properties of the entity.")
	}

	info, err := h.TableBackend.InsertEntity(table, partitionKey, rowKey, props)

	switch {
	case err == nil:
	case errors.Is(err, odatatable.ErrTableNotFound):
		return h.writeTableNotFoundError(c)
	case errors.Is(err, odatatable.ErrEntityAlreadyExists):
		return h.writeTableError(c, http.StatusConflict, "EntityAlreadyExists", "The specified entity already exists.")
	default:
		return h.writeTableError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	c.Response().Header().Set("ETag", info.ETag)

	if r.Header.Get("Prefer") == preferReturnNoContent {
		c.Response().Header().Set("Preference-Applied", preferReturnNoContent)

		return c.NoContent(http.StatusNoContent)
	}

	level := tableODataLevelFromAccept(r.Header.Get("Accept"))

	return h.writeTableJSON(c, http.StatusCreated, h.encodeTableEntity(info, table, level, "", r.Host))
}

func (h *Handler) getEntity(c *echo.Context, table, partitionKey, rowKey string) error {
	info, err := h.TableBackend.GetEntity(table, partitionKey, rowKey)

	switch {
	case err == nil:
	case errors.Is(err, odatatable.ErrTableNotFound):
		return h.writeTableNotFoundError(c)
	case errors.Is(err, odatatable.ErrEntityNotFound):
		return h.writeTableResourceNotFoundError(c)
	default:
		return h.writeTableError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	c.Response().Header().Set("ETag", info.ETag)

	r := c.Request()
	level := tableODataLevelFromAccept(r.Header.Get("Accept"))

	return h.writeTableJSON(c, http.StatusOK, h.encodeTableEntity(info, table, level, c.QueryParam("$select"), r.Host))
}

func (h *Handler) queryEntities(c *echo.Context, table string) error {
	r := c.Request()

	feed, next, failure := h.entityFeed(table, r.URL.Query(), r.Header.Get("Accept"), r.Host)
	if failure != nil {
		return h.writeTableError(c, failure.Status, failure.Code, failure.Message)
	}

	for _, kv := range next {
		c.Response().Header().Set(kv[0], kv[1])
	}

	return h.writeTableJSON(c, http.StatusOK, feed)
}

// Table service continuation headers and query parameters (Query Entities, "Response headers").
const (
	headerNextTableName = "X-Ms-Continuation-Nexttablename"
)

// entityFeed answers an entity-collection query (the $top, $filter and $select of q) as one page of the OData feed
// body, with the continuation headers when more entities match. A page holds at most 1,000 entities.
func (h *Handler) entityFeed(
	table string, q url.Values, accept, host string,
) (map[string]any, [][2]string, *odatatable.BatchFailure) {
	top, topErr := odatatable.ParseTop(q.Get("$top"))
	if topErr != nil {
		f := odatatable.NewBatchFailure(http.StatusBadRequest, "InvalidInput", "The value for $top is invalid.")

		return nil, nil, &f
	}

	var filter odatatable.Node

	if filterParam := q.Get("$filter"); filterParam != "" {
		node, parseErr := odatatable.ParseFilter(filterParam)
		if parseErr != nil {
			f := odatatable.NewBatchFailure(http.StatusBadRequest, "InvalidInput", "The specified $filter is invalid.")

			return nil, nil, &f
		}

		filter = node
	}

	page, err := h.TableBackend.QueryEntitiesPage(table, filter, top, q.Get("NextPartitionKey"), q.Get("NextRowKey"))
	if err != nil {
		f := odatatable.BatchFailureFor(odatatable.ErrTableNotFound)

		return nil, nil, &f
	}

	level := tableODataLevelFromAccept(accept)
	selectParam := q.Get("$select")

	values := make([]map[string]any, 0, len(page.Entities))
	for _, info := range page.Entities {
		values = append(values, h.encodeTableEntity(info, table, level, selectParam, host))
	}

	var next [][2]string
	if page.HasMore {
		next = [][2]string{{"x-ms-continuation-NextPartitionKey", page.NextPartitionKey},
			{"x-ms-continuation-NextRowKey", page.NextRowKey}}
	}

	return map[string]any{"value": values}, next, nil
}

func (h *Handler) replaceEntity(c *echo.Context, table, partitionKey, rowKey string) error {
	return h.putOrMergeEntity(c, table, partitionKey, rowKey, h.TableBackend.ReplaceEntity)
}

func (h *Handler) mergeEntity(c *echo.Context, table, partitionKey, rowKey string) error {
	return h.putOrMergeEntity(c, table, partitionKey, rowKey, h.TableBackend.MergeEntity)
}

// mutateTableEntityFunc is the shape ReplaceEntity/MergeEntity share, so
// putOrMergeEntity can dispatch to either one generically. Mirrors
// services/azuretable/entity_ops.go's identical mutateEntityFunc.
type mutateTableEntityFunc func(
	table, partitionKey, rowKey string, props map[string]odatatable.EntityProperty, ifMatch string,
) (odatatable.EntityInfo, error)

func (h *Handler) putOrMergeEntity(
	c *echo.Context, table, partitionKey, rowKey string, mutate mutateTableEntityFunc,
) error {
	r := c.Request()

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeTableError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	_, _, _, _, props, decErr := odatatable.DecodeEntityBody(body)
	if decErr != nil {
		return h.writeTableError(c, http.StatusBadRequest, "InvalidInput", "One of the request inputs is not valid.")
	}

	ifMatch := r.Header.Get("If-Match")

	info, mutateErr := mutate(table, partitionKey, rowKey, props, ifMatch)

	switch {
	case mutateErr == nil:
	case errors.Is(mutateErr, odatatable.ErrTableNotFound):
		return h.writeTableNotFoundError(c)
	case errors.Is(mutateErr, odatatable.ErrEntityNotFound):
		return h.writeTableResourceNotFoundError(c)
	case errors.Is(mutateErr, odatatable.ErrETagMismatch):
		return h.writeTableError(c, http.StatusPreconditionFailed, "UpdateConditionNotSatisfied",
			"The update condition specified in the request was not satisfied.")
	default:
		return h.writeTableError(c, http.StatusInternalServerError, "InternalError", mutateErr.Error())
	}

	c.Response().Header().Set("ETag", info.ETag)

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) deleteEntity(c *echo.Context, table, partitionKey, rowKey string) error {
	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" {
		return h.writeTableError(c, http.StatusBadRequest, "InvalidInput", "An If-Match header is required for delete.")
	}

	err := h.TableBackend.DeleteEntity(table, partitionKey, rowKey, ifMatch)

	switch {
	case err == nil:
		return c.NoContent(http.StatusNoContent)
	case errors.Is(err, odatatable.ErrTableNotFound):
		return h.writeTableNotFoundError(c)
	case errors.Is(err, odatatable.ErrEntityNotFound):
		return h.writeTableResourceNotFoundError(c)
	case errors.Is(err, odatatable.ErrETagMismatch):
		return h.writeTableError(c, http.StatusPreconditionFailed, "UpdateConditionNotSatisfied",
			"The update condition specified in the request was not satisfied.")
	default:
		return h.writeTableError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

// writeTableResourceNotFoundError maps a missing-entity TableBackend error to
// the corresponding Azure error code/status.
func (h *Handler) writeTableResourceNotFoundError(c *echo.Context) error {
	return h.writeTableError(c, http.StatusNotFound, "ResourceNotFound", "The specified resource does not exist.")
}
