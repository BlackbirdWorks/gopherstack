package gcpstorage

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

func errRequired(param string) *APIError {
	return newErr(http.StatusBadRequest, "required", "Required parameter: "+param)
}

func (h *Handler) listBuckets(c *echo.Context, _ route) error {
	q := c.QueryParams()

	project := q.Get("project")
	if project == "" {
		return errRequired("project")
	}

	limit, _ := strconv.Atoi(q.Get("maxResults"))
	pg := h.Backend.ListBuckets(project, q.Get("prefix"), q.Get("pageToken"), limit)
	base := baseURL(c.Request())
	items := make([]map[string]any, 0, len(pg.Data))

	for _, b := range pg.Data {
		items = append(items, bucketResource(base, b))
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyKind: "storage#buckets", "items": items, "nextPageToken": pg.Next,
	})
}

func (h *Handler) insertBucket(c *echo.Context, _ route) error {
	project := c.QueryParam("project")
	if project == "" {
		return errRequired("project")
	}

	p, err := readPatch(c)
	if err != nil {
		return err
	}

	p = withACLParams(c, p, keyPredefined, keyPredefDef)

	b, err := h.Backend.CreateBucket(project, p)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, bucketResource(baseURL(c.Request()), b))
}

func (h *Handler) getBucket(c *echo.Context, rt route) error {
	pre, apiErr := parsePreconds(c.QueryParams(), "if")
	if apiErr != nil {
		return apiErr
	}

	b, err := h.Backend.GetBucket(rt.bucket, pre)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, bucketResource(baseURL(c.Request()), b))
}

func (h *Handler) updateBucket(c *echo.Context, rt route) error {
	pre, apiErr := parsePreconds(c.QueryParams(), "if")
	if apiErr != nil {
		return apiErr
	}

	p, err := readPatch(c)
	if err != nil {
		return err
	}

	p = withACLParams(c, p, keyPredefined, keyPredefDef)

	b, err := h.Backend.UpdateBucket(rt.bucket, p, c.Request().Method == http.MethodPut, pre)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, bucketResource(baseURL(c.Request()), b))
}

func (h *Handler) deleteBucket(c *echo.Context, rt route) error {
	pre, apiErr := parsePreconds(c.QueryParams(), "if")
	if apiErr != nil {
		return apiErr
	}

	if err := h.Backend.DeleteBucket(rt.bucket, pre); err != nil {
		return err
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) getIamPolicy(c *echo.Context, rt route) error {
	pol, err := h.Backend.GetIAMPolicy(rt.bucket, requestedPolicyVersion(c))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, policyResource(rt.bucket, pol))
}

func (h *Handler) setIamPolicy(c *echo.Context, rt route) error {
	body, err := readBody(c)
	if err != nil {
		return err
	}

	var in IAMPolicy
	if jsonErr := json.Unmarshal(body, &in); jsonErr != nil {
		return errInvalid("Invalid JSON payload received.")
	}

	pol, err := h.Backend.SetIAMPolicy(rt.bucket, in)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, policyResource(rt.bucket, pol))
}

// testIamPermissions reports the requested storage permissions as granted: the
// emulator does not authenticate callers.
func (h *Handler) testIamPermissions(c *echo.Context, rt route) error {
	if _, err := h.Backend.GetBucket(rt.bucket, Preconds{}); err != nil {
		return err
	}

	perms := gcpiampolicy.Granted(c.QueryParams()["permissions"], "storage.")

	return c.JSON(http.StatusOK, map[string]any{
		keyKind: "storage#testIamPermissionsResponse", "permissions": perms,
	})
}

func requestedPolicyVersion(c *echo.Context) int {
	v, err := strconv.Atoi(c.QueryParam("optionsRequestedPolicyVersion"))
	if err != nil || v < 1 {
		return 1
	}

	return v
}
