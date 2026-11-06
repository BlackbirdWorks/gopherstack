package dashboard

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	appconfigbackend "github.com/blackbirdworks/gopherstack/services/azureappconfig"
)

const (
	appConfigFlagPrefix  = ".appconfig.featureflag/"
	appConfigFlagType    = "application/vnd.microsoft.appconfig.ff+json;charset=utf-8"
	appConfigMaxRevision = 200
	appConfigMaxFlagID   = 256
	appConfigMask        = "••••••••"
)

type appConfigSnapshotJSON struct {
	Created     time.Time  `json:"created"`
	Expires     *time.Time `json:"expires,omitempty"`
	Name        string     `json:"name"`
	Status      string     `json:"status"`
	Composition string     `json:"compositionType"`
	Filters     []string   `json:"filters"`
	Retention   int64      `json:"retentionSeconds"`
	Items       int64      `json:"itemCount"`
}

type appConfigFlagJSON struct {
	LastModified time.Time `json:"lastModified"`
	ID           string    `json:"id"`
	Description  string    `json:"description"`
	Label        string    `json:"label"`
	ETag         string    `json:"etag"`
	Enabled      bool      `json:"enabled"`
	Locked       bool      `json:"locked"`
	Filters      int       `json:"filterCount"`
}

type appConfigKeyJSON struct {
	LastModified time.Time `json:"lastModified"`
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Masked       string    `json:"masked"`
	ReadOnly     bool      `json:"readOnly"`
}

type appConfigToggleJSON struct {
	ID      string `json:"id"`
	Label   string `json:"label"`
	Enabled bool   `json:"enabled"`
}

type appConfigRevealJSON struct {
	ID string `json:"id"`
}

type appConfigFlagBody struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	Conditions  struct {
		ClientFilters []json.RawMessage `json:"client_filters"`
	} `json:"conditions"`
	Enabled bool `json:"enabled"`
}

func (a *azureAppConfigAPI) registerExtra(r *echo.Echo) {
	p := azureAppConfigProxyPrefix + "/stores/:store"
	r.Add(http.MethodGet, p+"/snapshots", a.guard(a.snapshots))
	r.Add(http.MethodGet, p+"/snapshots/:name/kv", a.guard(a.snapshotItems))
	r.Add(http.MethodGet, p+"/revisions", a.guard(a.revisions))
	r.Add(http.MethodGet, p+"/features", a.guard(a.features))
	r.Add(http.MethodPost, p+"/features/toggle", a.guard(a.toggleFeature))
	r.Add(http.MethodGet, p+"/keys", a.guard(a.accessKeys))
	r.Add(http.MethodPost, p+"/keys/reveal", a.guard(a.revealKey))
}

func appConfigKVs(kvs []appconfigbackend.KeyValue) []appConfigKVJSON {
	items := make([]appConfigKVJSON, 0, len(kvs))
	for _, kv := range kvs {
		items = append(items, appConfigKV(kv))
	}

	return items
}

func (a *azureAppConfigAPI) snapshots(c *echo.Context) error {
	snaps, err := a.b.ListSnapshots(c.Param("store"), "", nil)
	if err != nil {
		return a.fail(c, err)
	}

	items := make([]appConfigSnapshotJSON, 0, len(snaps))

	for _, s := range snaps {
		filters := make([]string, 0, len(s.Filters))
		for _, f := range s.Filters {
			filters = append(filters, "key="+f.Key+" label="+f.Label)
		}

		items = append(items, appConfigSnapshotJSON{
			Name: s.Name, Status: s.Status, Composition: s.Composition, Created: s.Created, Expires: s.Expires,
			Filters: filters, Retention: s.Retention, Items: int64(len(s.Items)),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"snapshots": items})
}

func (a *azureAppConfigAPI) snapshotItems(c *echo.Context) error {
	sel, err := appconfigbackend.NewSelector("", "", nil)
	if err != nil {
		return a.fail(c, err)
	}

	kvs, err := a.b.SnapshotKeyValues(c.Param("store"), c.Param("name"), sel)
	if err != nil {
		return a.fail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"items": appConfigKVs(kvs)})
}

// revisions returns the newest recorded states of the matching key-values, newest first.
func (a *azureAppConfigAPI) revisions(c *echo.Context) error {
	sel, err := appconfigbackend.NewSelector(c.QueryParam("key"), c.QueryParam("label"), nil)
	if err != nil {
		return a.fail(c, err)
	}

	kvs, err := a.b.ListRevisions(c.Param("store"), sel, time.Time{})
	if err != nil {
		return a.fail(c, err)
	}

	items := appConfigKVs(kvs)
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}

	if len(items) > appConfigMaxRevision {
		items = items[:appConfigMaxRevision]
	}

	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func (a *azureAppConfigAPI) features(c *echo.Context) error {
	sel, err := appconfigbackend.NewSelector(appConfigFlagPrefix+"*", "", nil)
	if err != nil {
		return a.fail(c, err)
	}

	kvs, err := a.b.ListKeyValues(c.Param("store"), sel, time.Time{})
	if err != nil {
		return a.fail(c, err)
	}

	flags := make([]appConfigFlagJSON, 0, len(kvs))

	for _, kv := range kvs {
		var body appConfigFlagBody
		if kv.Value != nil {
			_ = json.Unmarshal([]byte(*kv.Value), &body)
		}

		flags = append(flags, appConfigFlagJSON{
			ID: strings.TrimPrefix(kv.Key, appConfigFlagPrefix), Description: body.Description, Label: kv.Label,
			Enabled: body.Enabled, Locked: kv.Locked, ETag: kv.ETag, LastModified: kv.LastModified,
			Filters: len(body.Conditions.ClientFilters),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"features": flags})
}

// toggleFeature flips one flag's enabled field; every other field of the stored document is kept.
func (a *azureAppConfigAPI) toggleFeature(c *echo.Context) error {
	var in appConfigToggleJSON
	if !cloudJSONDecode(c, &in) {
		return nil
	}

	if in.ID == "" || len(in.ID) > appConfigMaxFlagID {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "invalid feature flag id")
	}

	store, key := c.Param("store"), appConfigFlagPrefix+in.ID

	kv, err := a.b.GetKeyValue(store, key, in.Label, time.Time{})
	if err != nil {
		return a.fail(c, err)
	}

	doc := map[string]json.RawMessage{}
	if kv.Value != nil {
		if err = json.Unmarshal([]byte(*kv.Value), &doc); err != nil {
			return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "stored feature flag is not valid JSON")
		}
	}

	doc["enabled"], _ = json.Marshal(in.Enabled)

	raw, err := json.Marshal(doc)
	if err != nil {
		return a.fail(c, err)
	}

	value, ct := string(raw), appConfigFlagType
	if kv.ContentType != nil {
		ct = *kv.ContentType
	}

	updated, err := a.b.SetKeyValue(store, key, in.Label,
		appconfigbackend.KVInput{Value: &value, ContentType: &ct, Tags: kv.Tags},
		appconfigbackend.Precondition{IfMatch: kv.ETag})
	if err != nil {
		return a.fail(c, err)
	}

	return c.JSON(http.StatusOK, appConfigKV(updated))
}

// accessKeys lists the store's credentials without their secrets.
func (a *azureAppConfigAPI) accessKeys(c *echo.Context) error {
	keys, err := a.b.AccessKeys(c.Param("store"))
	if err != nil {
		return a.fail(c, err)
	}

	items := make([]appConfigKeyJSON, 0, len(keys))
	for _, k := range keys {
		items = append(items, appConfigKeyJSON{
			ID: k.ID, Name: k.Name, ReadOnly: k.ReadOnly, LastModified: k.LastModified, Masked: appConfigMask,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"keys": items})
}

// revealKey is the only route that sends a credential secret out.
func (a *azureAppConfigAPI) revealKey(c *echo.Context) error {
	var in appConfigRevealJSON
	if !cloudJSONDecode(c, &in) {
		return nil
	}

	keys, err := a.b.AccessKeys(c.Param("store"))
	if err != nil {
		return a.fail(c, err)
	}

	for _, k := range keys {
		if k.ID == in.ID {
			c.Response().Header().Set("Cache-Control", "no-store")

			return c.JSON(http.StatusOK, map[string]string{"id": k.ID, "secret": k.Secret})
		}
	}

	return a.fail(c, appconfigbackend.ErrKeyIDNotFound)
}
