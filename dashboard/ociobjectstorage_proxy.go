package dashboard

import (
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"

	osbackend "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

const (
	ociObjectStoragePrefix = ociProxyPrefix + "/objectstorage"
	ociObjectDefaultLimit  = 200
)

type ociObjectJSON struct {
	TimeCreated  time.Time         `json:"timeCreated"`
	TimeModified time.Time         `json:"timeModified"`
	Metadata     map[string]string `json:"metadata"`
	Name         string            `json:"name"`
	Etag         string            `json:"etag"`
	MD5          string            `json:"md5"`
	VersionID    string            `json:"versionId"`
	ContentType  string            `json:"contentType"`
	StorageTier  string            `json:"storageTier"`
	Archival     string            `json:"archivalState"`
	Size         int               `json:"size"`
}

type ociObjectStorageAPI struct {
	b *osbackend.InMemoryBackend
}

// registerOCIObjectStorageRoutes serves the Object Storage page from the backend in-process.
func (h *DashboardHandler) registerOCIObjectStorageRoutes() {
	api := &ociObjectStorageAPI{}
	if h.config.OCIObjectStorageOps != nil {
		api.b = h.config.OCIObjectStorageOps.Backend
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return ociGuard(api.b != nil, "OCI Object Storage", next)
	}

	r := h.SubRouter
	p := ociObjectStoragePrefix
	r.Add(http.MethodGet, p+"/namespace", guard(api.namespace))
	r.Add(http.MethodGet, p+"/buckets", guard(api.buckets))
	r.Add(http.MethodGet, p+"/buckets/:bucket", guard(api.bucket))
	r.Add(http.MethodDelete, p+"/buckets/:bucket", guard(api.deleteBucket))
	r.Add(http.MethodGet, p+"/buckets/:bucket/objects", guard(api.objects))
	r.Add(http.MethodGet, p+"/buckets/:bucket/object", guard(api.object))
	r.Add(http.MethodGet, p+"/buckets/:bucket/object/content", guard(api.content))
	r.Add(http.MethodDelete, p+"/buckets/:bucket/object", guard(api.deleteObject))
	r.Add(http.MethodGet, p+"/buckets/:bucket/pars", guard(api.pars))
	r.Add(http.MethodGet, p+"/buckets/:bucket/lifecycle", guard(api.lifecycle))
	r.Add(http.MethodGet, p+"/buckets/:bucket/retention", guard(api.retention))
}

func (a *ociObjectStorageAPI) ns() string { return a.b.Namespace() }

func (a *ociObjectStorageAPI) namespace(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{ociNamespaceKey: a.ns()})
}

func (a *ociObjectStorageAPI) buckets(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"namespace": a.ns(), "buckets": a.b.AllBuckets()})
}

func (a *ociObjectStorageAPI) bucket(c *echo.Context) error {
	bk, err := a.b.GetBucket(
		a.ns(),
		c.Param("bucket"),
		[]string{"approximateCount", "approximateSize"},
		osbackend.Preconds{},
	)
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, bk)
}

func (a *ociObjectStorageAPI) deleteBucket(c *echo.Context) error {
	if err := a.b.DeleteBucket(a.ns(), c.Param("bucket"), osbackend.Preconds{}); err != nil {
		return ociFail(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func ociObjectView(o osbackend.ObjectInfo) ociObjectJSON {
	return ociObjectJSON{
		Name: o.Name, Etag: o.Etag, MD5: o.MD5, VersionID: o.VersionID, Size: len(o.Data),
		ContentType: o.Meta.ContentType, StorageTier: o.Meta.StorageTier, Archival: o.Archival,
		TimeCreated: o.TimeCreated, TimeModified: o.TimeModified, Metadata: o.Meta.Metadata,
	}
}

func (a *ociObjectStorageAPI) objects(c *echo.Context) error {
	limit, _ := strconv.Atoi(c.QueryParam("limit"))
	if limit <= 0 {
		limit = ociObjectDefaultLimit
	}

	objs, prefixes, next, err := a.b.ListObjects(a.ns(), c.Param("bucket"), osbackend.ListOptions{
		Prefix: c.QueryParam(
			"prefix",
		), Delimiter: c.QueryParam("delimiter"), Start: c.QueryParam("start"), Limit: limit,
	})
	if err != nil {
		return ociFail(c, err)
	}

	items := make([]ociObjectJSON, 0, len(objs))
	for _, o := range objs {
		items = append(items, ociObjectView(o))
	}

	if prefixes == nil {
		prefixes = []string{}
	}

	return c.JSON(http.StatusOK, map[string]any{ociKeyObjects: items, "prefixes": prefixes, "nextStartWith": next})
}

func (a *ociObjectStorageAPI) lookup(c *echo.Context, body bool) (osbackend.ObjectInfo, bool) {
	name := c.QueryParam("name")
	if name == "" {
		_ = cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "name is required")

		return osbackend.ObjectInfo{}, false
	}

	o, err := a.b.GetObject(a.ns(), c.Param("bucket"), name, c.QueryParam("versionId"), osbackend.Preconds{}, body)
	if err != nil {
		_ = ociFail(c, err)

		return osbackend.ObjectInfo{}, false
	}

	return o, true
}

func (a *ociObjectStorageAPI) object(c *echo.Context) error {
	o, ok := a.lookup(c, false)
	if !ok {
		return nil
	}

	return c.JSON(http.StatusOK, ociObjectView(o))
}

func (a *ociObjectStorageAPI) content(c *echo.Context) error {
	o, ok := a.lookup(c, true)
	if !ok {
		return nil
	}

	ctype := o.Meta.ContentType
	if ctype == "" {
		ctype = ociMimeOctet
	}

	h := c.Response().Header()
	h.Set("Content-Disposition", "attachment; filename*=UTF-8''"+url.PathEscape(path.Base(o.Name)))
	h.Set("X-Content-Type-Options", "nosniff")

	return c.Blob(http.StatusOK, ctype, o.Data)
}

func (a *ociObjectStorageAPI) deleteObject(c *echo.Context) error {
	name := c.QueryParam("name")
	if name == "" {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "name is required")
	}

	if _, err := a.b.DeleteObject(a.ns(), c.Param("bucket"), name, c.QueryParam("versionId"), ""); err != nil {
		return ociFail(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (a *ociObjectStorageAPI) pars(c *echo.Context) error {
	items, err := ociCollect(func(token string) (page.Page[osbackend.PARWire], error) {
		return a.b.ListPARs(a.ns(), c.Param("bucket"), "", token, ociListLimit)
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"pars": items})
}

func (a *ociObjectStorageAPI) lifecycle(c *echo.Context) error {
	lc, _, err := a.b.GetLifecycle(a.ns(), c.Param("bucket"))
	if err != nil {
		if _, code, _, ok := ociAPIError(err); ok && code == "LifecyclePolicyNotFound" {
			return c.JSON(http.StatusOK, map[string]any{"configured": false, ociKeyRules: []any{}})
		}

		return ociFail(c, err)
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{"configured": true, "timeCreated": lc.TimeCreated, ociKeyRules: lc.Items},
	)
}

func (a *ociObjectStorageAPI) retention(c *echo.Context) error {
	items, err := ociCollect(func(token string) (page.Page[osbackend.RetentionWire], error) {
		return a.b.ListRetention(a.ns(), c.Param("bucket"), token)
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{ociKeyRules: items})
}
