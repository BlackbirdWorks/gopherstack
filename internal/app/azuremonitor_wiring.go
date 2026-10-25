package app

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azuremonitor"
)

// wireAzureMonitor lets ARM's Microsoft.OperationalInsights and Microsoft.Insights providers keep workspaces,
// tables and data collection endpoints and rules in the Azure Monitor Logs backend.
func wireAzureMonitor(byName map[string]service.Registerable) {
	monH, ok := byName["AzureMonitor"].(*azuremonitor.Handler)
	if !ok {
		return
	}

	if armH, armOk := byName["AzureARM"].(*azurearm.Handler); armOk {
		armH.Registry.SetMonitorLogs(&azureMonitorAdapter{backend: monH.Backend})
	}
}

// azureMonitorAdapter adapts services/azuremonitor's backend to services/azurearm.MonitorLogs.
type azureMonitorAdapter struct {
	backend *azuremonitor.Backend
}

func monitorRef(r azurearm.MonitorRequest) azuremonitor.Ref {
	return azuremonitor.Ref{
		Host: r.Host, Sub: r.Sub, RG: r.RG, Namespace: r.Namespace, Types: r.Types, Names: r.Names,
	}
}

func monitorErr(err error) error {
	if err == nil {
		return nil
	}

	msg := func(sentinel error) string { return strings.TrimPrefix(err.Error(), sentinel.Error()+": ") }

	switch {
	case errors.Is(err, azuremonitor.ErrNotFound):
		return fmt.Errorf("%w: %s", azurearm.ErrResourceNotFound, msg(azuremonitor.ErrNotFound))
	case errors.Is(err, azuremonitor.ErrUnsupported):
		return fmt.Errorf("%w: %s", azurearm.ErrResourceNotFound, msg(azuremonitor.ErrUnsupported))
	case errors.Is(err, azuremonitor.ErrConflict):
		return &azurearm.MonitorError{Code: "Conflict", Message: msg(azuremonitor.ErrConflict), Status: http.StatusConflict}
	case errors.Is(err, azuremonitor.ErrInvalid):
		m := msg(azuremonitor.ErrInvalid)
		if rest, found := strings.CutPrefix(m, "InvalidOutputTable: "); found {
			return &azurearm.MonitorError{Code: "InvalidOutputTable", Message: rest, Status: http.StatusBadRequest}
		}

		return &azurearm.MonitorError{Code: "InvalidParameter", Message: m, Status: http.StatusBadRequest}
	}

	return err
}

func (a *azureMonitorAdapter) Put(r azurearm.MonitorRequest, body map[string]any) (map[string]any, error) {
	out, err := a.backend.Put(monitorRef(r), body)

	return out, monitorErr(err)
}

func (a *azureMonitorAdapter) Get(r azurearm.MonitorRequest) (map[string]any, error) {
	out, err := a.backend.Get(monitorRef(r))

	return out, monitorErr(err)
}

func (a *azureMonitorAdapter) Patch(r azurearm.MonitorRequest, body map[string]any) (map[string]any, error) {
	out, err := a.backend.Patch(monitorRef(r), body)

	return out, monitorErr(err)
}

func (a *azureMonitorAdapter) Delete(r azurearm.MonitorRequest) error {
	return monitorErr(a.backend.Delete(monitorRef(r)))
}

func (a *azureMonitorAdapter) List(r azurearm.MonitorRequest) ([]map[string]any, error) {
	out, err := a.backend.List(monitorRef(r))

	return out, monitorErr(err)
}

func (a *azureMonitorAdapter) Action(
	r azurearm.MonitorRequest, action string, body map[string]any,
) (map[string]any, error) {
	out, err := a.backend.Action(monitorRef(r), action, body)

	return out, monitorErr(err)
}

func (a *azureMonitorAdapter) DeleteInGroup(rg string) { a.backend.DeleteInGroup(rg) }

func (a *azureMonitorAdapter) Reset() { a.backend.Reset() }
