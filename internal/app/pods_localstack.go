package app

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/pods"
)

const (
	lsPodsPath        = "/_localstack/pods"
	lsStateExportPath = "/_localstack/state/export"
	lsStateImportPath = "/_localstack/state/import"
	lsKeyPodName      = "pod_name"
)

type lsPodRequest struct {
	PodName  string   `json:"pod_name"`
	Message  string   `json:"message"`
	Services []string `json:"services"`
	Version  int      `json:"version"`
	Merge    bool     `json:"merge"`
}

func lsBindPod(c *echo.Context) (lsPodRequest, error) {
	var req lsPodRequest
	if c.Request().ContentLength != 0 {
		if err := c.Bind(&req); err != nil {
			return req, echo.NewHTTPError(http.StatusBadRequest, "invalid request body: "+err.Error())
		}
	}

	if req.PodName == "" {
		req.PodName = c.QueryParam(lsKeyPodName)
	}

	if !pods.ValidName(req.PodName) {
		return req, echo.NewHTTPError(http.StatusBadRequest, "invalid or missing pod_name")
	}

	return req, nil
}

func lsPodSummary(m pods.Manifest) map[string]any {
	return map[string]any{lsKeyPodName: m.Name, podKeyVersion: m.Version, "message": m.Message, "services": m.Services}
}

func (a *podAPI) lsSave(c *echo.Context) error {
	req, err := lsBindPod(c)
	if err != nil {
		return err
	}

	snaps := a.manager.ExportAll()
	if snaps, err = pickServices(snaps, req.Services); err != nil {
		return err
	}

	m, err := a.svc.Save(c.Request().Context(), req.PodName, req.Message, snaps)
	if err != nil {
		return podHTTPError(err)
	}

	return c.JSON(http.StatusCreated, lsPodSummary(m))
}

func (a *podAPI) lsLoad(c *echo.Context) error {
	req, err := lsBindPod(c)
	if err != nil {
		return err
	}

	ctx := c.Request().Context()

	m, snaps, err := a.svc.Load(ctx, req.PodName, max(req.Version, 0), req.Services)
	if err != nil {
		return podHTTPError(err)
	}

	strategy := podStrategyOverwrite
	if req.Merge {
		strategy = podStrategyServiceMerge
	}

	if err = a.apply(ctx, strategy, req.Merge, snaps); err != nil {
		return podApplyHTTPError(err)
	}

	return c.JSON(http.StatusOK, lsPodSummary(m))
}

func (a *podAPI) lsList(c *echo.Context) error {
	l, err := a.svc.List(c.Request().Context())
	if err != nil {
		return podHTTPError(err)
	}

	out := make([]map[string]any, 0, len(l))
	for _, s := range l {
		out = append(out, map[string]any{lsKeyPodName: s.Name, "max_version": s.Latest, "last_change": s.LatestAt})
	}

	return c.JSON(http.StatusOK, map[string]any{"cloudpods": out})
}

func (a *podAPI) lsDelete(c *echo.Context) error {
	name := c.Param(podKeyName)

	if err := a.svc.Delete(c.Request().Context(), name, 0); err != nil {
		return podHTTPError(err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (a *podAPI) lsStateExport(c *echo.Context) error {
	raw := a.manager.ExportAll()
	services := make(map[string]json.RawMessage, len(raw))

	for n, d := range raw {
		services[n] = json.RawMessage(d)
	}

	return c.JSON(http.StatusOK, snapshotResponse{
		Format: snapshotBundleFormat, Services: services, Exported: len(services), Status: "ok",
	})
}

func (a *podAPI) lsStateImport(c *echo.Context) error {
	var bundle snapshotBundle
	if err := json.NewDecoder(c.Request().Body).Decode(&bundle); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid snapshot bundle: "+err.Error())
	}

	snaps := make(map[string][]byte, len(bundle.Services))
	for n, d := range bundle.Services {
		snaps[n] = []byte(d)
	}

	merge := podMergeParam(c)

	strategy := podStrategyOverwrite
	if merge {
		strategy = podStrategyServiceMerge
	}

	if err := a.apply(c.Request().Context(), strategy, merge, snaps); err != nil {
		return podApplyHTTPError(err)
	}

	return c.JSON(http.StatusOK, loadResponse{
		Status: "ok", Loaded: len(snaps), Message: fmt.Sprintf("restored %d service(s)", len(snaps)),
	})
}

// registerLocalstackAliases mounts the LocalStack-style pod and state endpoints.
func (a *podAPI) registerLocalstackAliases(e *echo.Echo) {
	e.GET(lsPodsPath, a.lsList)
	e.POST(lsPodsPath, a.lsSave)
	e.PUT(lsPodsPath, a.lsSave)
	e.PATCH(lsPodsPath, a.lsLoad)
	e.DELETE(lsPodsPath+"/:name", a.lsDelete)
	e.GET(lsStateExportPath, a.lsStateExport)
	e.POST(lsStateExportPath, a.lsStateExport)
	e.POST(lsStateImportPath, a.lsStateImport)
}
