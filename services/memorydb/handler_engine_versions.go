package memorydb

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDescribeEngineVersions(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeEngineVersionsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	versions, err := h.Backend.DescribeEngineVersions(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]engineVersionObject, 0, len(versions))

	for _, ev := range versions {
		objs = append(objs, engineVersionObject{
			Engine:               ev.Engine,
			EngineVersion:        ev.EngineVersion,
			EnginePatchVersion:   ev.EnginePatchVersion,
			ParameterGroupFamily: ev.ParameterGroupFamily,
			Description:          ev.Description,
		})
	}

	return c.JSON(http.StatusOK, describeEngineVersionsResponse{EngineVersions: objs})
}

// -- Event handlers --------------------------------------------------------------
