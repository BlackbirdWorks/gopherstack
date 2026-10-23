package azurearm

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	redisActionRegenerate = "regenerateKey"
	redisActionReboot     = "forceReboot"
	redisActionImport     = "import"
	redisActionExport     = "export"
	redisOpsAsync         = "asyncOperations"
	redisOpsResults       = "operationResults"
	redisNsIdx            = 5
	redisTypeIdx          = 6
	redisCacheSegs        = 8
	redisSubSegs          = 8
	redisChildSegs        = 9
	redisLocationsIdx     = 4
	redisOpTypeIdx        = 6
	redisProvidersIdx     = 2
	redisOpNsIdx          = 3
)

func isRedisCacheScope(segs []string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, redisNsIdx, namespaceMicrosoftCache) && segAt(segs, redisTypeIdx, redisType)
}

func isRedisOperation(segs []string) bool {
	return len(segs) == redisSubSegs && isSubscriptionScoped(segs) &&
		segAt(segs, redisProvidersIdx, providersSegment) && segAt(segs, redisOpNsIdx, namespaceMicrosoftCache) &&
		segAt(segs, redisLocationsIdx, "locations") &&
		(segAt(segs, redisOpTypeIdx, redisOpsAsync) || segAt(segs, redisOpTypeIdx, redisOpsResults))
}

func isRedisAction(segs []string, r *http.Request, action string) bool {
	return r.Method == http.MethodPost && isRedisCacheScope(segs, redisChildSegs) &&
		strings.EqualFold(segs[len(segs)-1], action)
}

func isRedisChildList(segs []string, r *http.Request) bool {
	if r.Method != http.MethodGet || !isRedisCacheScope(segs, redisChildSegs) {
		return false
	}

	switch strings.ToLower(segs[len(segs)-1]) {
	case strings.ToLower(redisFirewallType), strings.ToLower(redisPatchType), strings.ToLower(redisLinkedType):
		return true
	}

	return false
}

//nolint:gochecknoglobals // static route table, read-only after init
var redisRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isRedisOperation(segs)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleRedisOperation(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isRedisCacheScope(segs, redisCacheSegs) &&
				(r.Method == http.MethodPut || r.Method == http.MethodPatch || r.Method == http.MethodDelete)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleRedisCache(c, segs) },
	},
	{
		match:  isRedisChildList,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleRedisChildList(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool { return isRedisAction(segs, r, redisActionRegenerate) },
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleRedisAction(c, segs, redisActionRegenerate)
		},
	},
	{
		match: func(segs []string, r *http.Request) bool { return isRedisAction(segs, r, redisActionReboot) },
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleRedisAction(c, segs, redisActionReboot)
		},
	},
	{
		match: func(segs []string, r *http.Request) bool { return isRedisAction(segs, r, redisActionImport) },
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleRedisAction(c, segs, redisActionImport)
		},
	},
	{
		match: func(segs []string, r *http.Request) bool { return isRedisAction(segs, r, redisActionExport) },
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleRedisAction(c, segs, redisActionExport)
		},
	},
}

func (h *Handler) redisFor(c *echo.Context, segs []string) (*RedisProvider, ResourceID, error) {
	p, ok := h.redisProvider()
	if !ok {
		return nil, ResourceID{}, h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs))
	if err != nil {
		return nil, ResourceID{}, h.writeAPIError(c, err)
	}

	return p, id, nil
}

// setRedisOperation points the response's Azure-AsyncOperation and Location at a status resource that completes
// at readyAt.
func (h *Handler) setRedisOperation(c *echo.Context, p *RedisProvider, id ResourceID, readyAt time.Time) {
	opID := p.CreateOperation(readyAt)
	base := h.baseURLFor(
		c.Request(),
	) + "/subscriptions/" + id.SubscriptionID + "/providers/" + namespaceMicrosoftCache +
		"/locations/" + DefaultLocation + "/"
	query := "?api-version=" + c.Request().URL.Query().Get("api-version")

	hdr := c.Response().Header()
	hdr.Set("Azure-Asyncoperation", base+redisOpsAsync+"/"+opID+query)
	hdr.Set("Location", base+redisOpsResults+"/"+opID+query)
	hdr.Set("Retry-After", "1")
}

func (h *Handler) handleRedisOperation(c *echo.Context, segs []string) error {
	p, ok := h.redisProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	opID := segs[len(segs)-1]
	done := p.OperationDone(opID)
	async := segAt(segs, redisOpTypeIdx, redisOpsAsync)

	switch {
	case async && done:
		return h.writeJSON(c, http.StatusOK,
			map[string]any{fieldName: opID, redisPropStatus: provisioningStateSucceeded})
	case async:
		return h.writeJSON(c, http.StatusOK, map[string]any{fieldName: opID, redisPropStatus: "InProgress"})
	case done:
		return h.writeJSON(c, http.StatusOK, map[string]any{})
	}

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleRedisCache(c *echo.Context, segs []string) error {
	p, id, err := h.redisFor(c, segs)
	if p == nil {
		return err
	}

	switch c.Request().Method {
	case http.MethodDelete:
		return h.deleteRedis(c, p, id)
	case http.MethodPatch:
		return h.writeRedisBody(c, p, id, http.StatusOK, p.Patch)
	}

	_, getErr := p.Get(c.Request().Context(), id)
	status := http.StatusOK

	if getErr != nil {
		status = http.StatusCreated
	}

	return h.writeRedisBody(c, p, id, status, func(id ResourceID, body map[string]any) (map[string]any, error) {
		return p.Put(c.Request().Context(), id, body)
	})
}

func (h *Handler) writeRedisBody(
	c *echo.Context, p *RedisProvider, id ResourceID, status int,
	apply func(ResourceID, map[string]any) (map[string]any, error),
) error {
	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := apply(id, body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	h.setRedisOperation(c, p, id, p.ReadyAt(id))

	return h.writeJSON(c, status, out)
}

func (h *Handler) deleteRedis(c *echo.Context, p *RedisProvider, id ResourceID) error {
	if err := p.Delete(c.Request().Context(), id); err != nil {
		if errors.Is(err, ErrRedisNotFound) || isNotFoundOnDelete(err) {
			return c.NoContent(http.StatusNoContent)
		}

		return h.writeAPIError(c, err)
	}

	h.setRedisOperation(c, p, id, p.now())

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleRedisChildList(c *echo.Context, segs []string) error {
	p, id, err := h.redisFor(c, segs[:len(segs)-1])
	if p == nil {
		return err
	}

	values, err := p.ListChildren(id, segs[len(segs)-1])
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: values})
}

func (h *Handler) handleRedisAction(c *echo.Context, segs []string, action string) error {
	p, id, err := h.redisFor(c, segs[:len(segs)-1])
	if p == nil {
		return err
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	switch action {
	case redisActionRegenerate:
		keyType, _ := body["keyType"].(string)

		return h.writeRedisResult(
			c,
			http.StatusOK,
			func() (map[string]any, error) { return p.RegenerateKey(id, keyType) },
		)
	case redisActionReboot:
		return h.writeRedisResult(c, http.StatusOK, func() (map[string]any, error) { return p.ForceReboot(id, body) })
	case redisActionImport:
		return h.writeRedisLRO(c, p, id, func() error { return p.ImportData(id, body) })
	}

	return h.writeRedisLRO(c, p, id, func() error { return p.ExportData(id, body) })
}

func (h *Handler) writeRedisResult(c *echo.Context, status int, f func() (map[string]any, error)) error {
	out, err := f()
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, status, out)
}

// writeRedisLRO runs a data-moving action to completion and answers 202 with an already-finished operation.
func (h *Handler) writeRedisLRO(c *echo.Context, p *RedisProvider, id ResourceID, run func() error) error {
	if err := run(); err != nil {
		return h.writeAPIError(c, err)
	}

	h.setRedisOperation(c, p, id, p.now())

	return c.NoContent(http.StatusAccepted)
}
