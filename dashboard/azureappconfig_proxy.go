package dashboard

import (
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	appconfigbackend "github.com/blackbirdworks/gopherstack/services/azureappconfig"
)

const (
	azureAppConfigProxyPrefix = "/dashboard/api/v1/azure/appconfig"
	appConfigMaxValueBytes    = 1 << 20
)

type appConfigKVJSON struct {
	LastModified time.Time         `json:"lastModified"`
	Tags         map[string]string `json:"tags,omitempty"`
	Key          string            `json:"key"`
	Label        string            `json:"label"`
	Value        string            `json:"value"`
	ContentType  string            `json:"contentType"`
	ETag         string            `json:"etag"`
	Locked       bool              `json:"locked"`
}

type appConfigPutJSON struct {
	Key         string `json:"key"`
	Label       string `json:"label"`
	Value       string `json:"value"`
	ContentType string `json:"contentType"`
}

// registerAzureAppConfigRoutes serves the App Configuration page from the backend
// in-process, bypassing the HMAC data plane; stores are created through ARM.
func (h *DashboardHandler) registerAzureAppConfigRoutes() {
	api := &azureAppConfigAPI{}
	if h.config.AzureAppConfigOps != nil {
		api.b = h.config.AzureAppConfigOps.Backend
	}

	r := h.SubRouter
	r.Add(http.MethodGet, azureAppConfigProxyPrefix+"/stores", api.guard(api.stores))
	r.Add(http.MethodGet, azureAppConfigProxyPrefix+"/stores/:store/kv", api.guard(api.list))
	r.Add(http.MethodGet, azureAppConfigProxyPrefix+"/stores/:store/labels", api.guard(api.labels))
	r.Add(http.MethodPut, azureAppConfigProxyPrefix+"/stores/:store/kv", api.guard(api.put))
	r.Add(http.MethodDelete, azureAppConfigProxyPrefix+"/stores/:store/kv", api.guard(api.remove))
}

type azureAppConfigAPI struct {
	b *appconfigbackend.InMemoryBackend
}

func (a *azureAppConfigAPI) guard(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if a.b == nil {
			return cloudUnavailable(c, "Azure App Configuration")
		}

		return next(c)
	}
}

func (a *azureAppConfigAPI) fail(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, appconfigbackend.ErrStoreNotFound), errors.Is(err, appconfigbackend.ErrKeyNotFound):
		return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, err.Error())
	case errors.Is(err, appconfigbackend.ErrLocked):
		return cloudJSONFail(c, http.StatusConflict, "FAILED_PRECONDITION", err.Error())
	default:
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, err.Error())
	}
}

func appConfigKV(kv appconfigbackend.KeyValue) appConfigKVJSON {
	out := appConfigKVJSON{
		Key: kv.Key, Label: kv.Label, ETag: kv.ETag, Locked: kv.Locked, Tags: kv.Tags, LastModified: kv.LastModified,
	}
	if kv.Value != nil {
		out.Value = *kv.Value
	}

	if kv.ContentType != nil {
		out.ContentType = *kv.ContentType
	}

	return out
}

func (a *azureAppConfigAPI) stores(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"stores": a.b.StoreNames()})
}

// list filters by key and label using App Configuration filter syntax (e.g. "app*", "prod").
func (a *azureAppConfigAPI) list(c *echo.Context) error {
	sel, err := appconfigbackend.NewSelector(c.QueryParam("key"), c.QueryParam("label"), nil)
	if err != nil {
		return a.fail(c, err)
	}

	kvs, err := a.b.ListKeyValues(c.Param("store"), sel, time.Time{})
	if err != nil {
		return a.fail(c, err)
	}

	items := make([]appConfigKVJSON, 0, len(kvs))
	for _, kv := range kvs {
		items = append(items, appConfigKV(kv))
	}

	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func (a *azureAppConfigAPI) labels(c *echo.Context) error {
	labels, err := a.b.ListLabelNames(c.Param("store"), "", time.Time{})
	if err != nil {
		return a.fail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"labels": labels})
}

func (a *azureAppConfigAPI) put(c *echo.Context) error {
	var in appConfigPutJSON
	if !cloudJSONDecode(c, &in) {
		return nil
	}

	if len(in.Value) > appConfigMaxValueBytes {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "value is too large")
	}

	input := appconfigbackend.KVInput{Value: &in.Value}
	if in.ContentType != "" {
		input.ContentType = &in.ContentType
	}

	kv, err := a.b.SetKeyValue(c.Param("store"), in.Key, in.Label, input, appconfigbackend.Precondition{})
	if err != nil {
		return a.fail(c, err)
	}

	return c.JSON(http.StatusOK, appConfigKV(kv))
}

func (a *azureAppConfigAPI) remove(c *echo.Context) error {
	_, err := a.b.DeleteKeyValue(
		c.Param("store"), c.QueryParam("key"), c.QueryParam("label"), appconfigbackend.Precondition{},
	)
	if err != nil {
		return a.fail(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
