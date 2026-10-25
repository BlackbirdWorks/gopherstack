package azurearm

import (
	"errors"
	"fmt"
	"net/http"
)

// MonitorRequest addresses a Microsoft.OperationalInsights or Microsoft.Insights resource.
type MonitorRequest struct {
	Host      string
	Sub       string
	RG        string
	Namespace string
	Types     []string
	Names     []string
}

// MonitorLogs is the seam the Microsoft.OperationalInsights and Microsoft.Insights providers use to keep
// workspaces, tables, data collection endpoints and rules in the Azure Monitor Logs data plane. Errors wrap
// ErrResourceNotFound or are *MonitorError values.
type MonitorLogs interface {
	Put(req MonitorRequest, body map[string]any) (map[string]any, error)
	Get(req MonitorRequest) (map[string]any, error)
	Patch(req MonitorRequest, body map[string]any) (map[string]any, error)
	Delete(req MonitorRequest) error
	List(req MonitorRequest) ([]map[string]any, error)
	Action(req MonitorRequest, action string, body map[string]any) (map[string]any, error)
	DeleteInGroup(rg string)
	Reset()
}

// MonitorError is a data-plane failure carried to the ARM error envelope unchanged.
type MonitorError struct {
	Code    string
	Message string
	Status  int
}

func (e *MonitorError) Error() string { return e.Code + ": " + e.Message }

// ErrMonitorUnavailable is returned when no Azure Monitor Logs data plane is wired.
var ErrMonitorUnavailable = &MonitorError{
	Code: "ServiceUnavailable", Message: "The Azure Monitor Logs data plane is not available.",
	Status: http.StatusServiceUnavailable,
}

func monitorErrorDetails(err error) (errorEntry, bool) {
	var me *MonitorError
	if errors.As(err, &me) {
		return errorEntry{me.Code, me.Message, me.Status}, true
	}

	return errorEntry{}, false
}

type noopMonitorLogs struct{}

func (noopMonitorLogs) Put(MonitorRequest, map[string]any) (map[string]any, error) {
	return nil, ErrMonitorUnavailable
}

func (noopMonitorLogs) Get(MonitorRequest) (map[string]any, error) {
	return nil, fmt.Errorf("%w: Azure Monitor Logs is not available", ErrResourceNotFound)
}

func (noopMonitorLogs) Patch(MonitorRequest, map[string]any) (map[string]any, error) {
	return nil, ErrMonitorUnavailable
}
func (noopMonitorLogs) Delete(MonitorRequest) error { return nil }
func (noopMonitorLogs) List(MonitorRequest) ([]map[string]any, error) {
	return []map[string]any{}, nil
}

func (noopMonitorLogs) Action(MonitorRequest, string, map[string]any) (map[string]any, error) {
	return nil, ErrMonitorUnavailable
}
func (noopMonitorLogs) DeleteInGroup(string) {}
func (noopMonitorLogs) Reset()               {}

var _ MonitorLogs = noopMonitorLogs{}

// SetMonitorLogs gives the registered Microsoft.OperationalInsights and Microsoft.Insights providers (if any)
// a real data-plane adapter, replacing their no-op default.
func (r *Registry) SetMonitorLogs(m MonitorLogs) {
	for _, ns := range []string{namespaceMicrosoftOperationalInsights, namespaceMicrosoftInsights} {
		if p, ok := r.ProviderNamed(ns); ok {
			if mp, mpOk := p.(*MonitorProvider); mpOk {
				mp.dataPlane = m
			}
		}
	}
}
