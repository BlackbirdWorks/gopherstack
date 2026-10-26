package azurearm

import (
	"context"
	"fmt"
	"strings"
)

const (
	namespaceMicrosoftOperationalInsights = "Microsoft.OperationalInsights"
	namespaceMicrosoftInsights            = "Microsoft.Insights"
	monitorWorkspacesType                 = "workspaces"
	monitorTablesType                     = "tables"
	monitorDCEType                        = "dataCollectionEndpoints"
	monitorDCRType                        = "dataCollectionRules"
)

// MonitorProvider implements ResourceProvider for Microsoft.OperationalInsights (workspaces, tables) and
// Microsoft.Insights (data collection endpoints and rules). Every other type under those namespaces keeps the
// generic metadata-only store.
type MonitorProvider struct {
	dataPlane MonitorLogs
	backend   *InMemoryBackend
	namespace string
	types     []string
}

// NewMonitorProvider creates the provider for one namespace; dataPlane may be nil.
func NewMonitorProvider(namespace string, backend *InMemoryBackend, dataPlane MonitorLogs) *MonitorProvider {
	if dataPlane == nil {
		dataPlane = noopMonitorLogs{}
	}

	types := []string{monitorWorkspacesType, monitorWorkspacesType + "/" + monitorTablesType}
	if namespace == namespaceMicrosoftInsights {
		types = []string{monitorDCEType, monitorDCRType}
	}

	return &MonitorProvider{namespace: namespace, backend: backend, dataPlane: dataPlane, types: types}
}

var _ ResourceProvider = (*MonitorProvider)(nil)

// Namespace implements ResourceProvider.
func (p *MonitorProvider) Namespace() string { return p.namespace }

// ResourceTypes implements ResourceProvider.
func (p *MonitorProvider) ResourceTypes() []ResourceTypeDef {
	versions := []string{"2023-09-01", "2022-10-01"}
	if p.namespace == namespaceMicrosoftInsights {
		versions = []string{"2023-03-11", "2022-06-01"}
	}

	out := make([]ResourceTypeDef, len(p.types))
	for i, t := range p.types {
		out[i] = ResourceTypeDef{Type: t, APIVersions: versions, HasChildren: t == monitorWorkspacesType}
	}

	return out
}

func (p *MonitorProvider) handles(id ResourceID) bool {
	t := strings.ToLower(strings.Join(id.Types, "/"))

	for _, h := range p.types {
		if strings.ToLower(h) == t {
			return true
		}
	}

	return false
}

func (p *MonitorProvider) request(ctx context.Context, id ResourceID) MonitorRequest {
	return MonitorRequest{
		Host: RequestHostFromContext(ctx), Sub: id.SubscriptionID, RG: id.ResourceGroup,
		Namespace: id.Namespace, Types: id.Types, Names: id.Names,
	}
}

// Put implements ResourceProvider.
func (p *MonitorProvider) Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	if !p.handles(id) {
		out, _, err := p.backend.putGenericResource(id, body)

		return out, err
	}

	return p.dataPlane.Put(p.request(ctx, id), body)
}

// Get implements ResourceProvider.
func (p *MonitorProvider) Get(ctx context.Context, id ResourceID) (map[string]any, error) {
	if !p.handles(id) {
		return p.backend.getGenericResource(id)
	}

	return p.dataPlane.Get(p.request(ctx, id))
}

// Patch updates tags and mutable properties.
func (p *MonitorProvider) Patch(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	if !p.handles(id) {
		return nil, fmt.Errorf("%w: PATCH is not supported for %s", ErrResourceNotFound, id.ResourceType())
	}

	return p.dataPlane.Patch(p.request(ctx, id), body)
}

// Delete implements ResourceProvider.
func (p *MonitorProvider) Delete(ctx context.Context, id ResourceID) error {
	if !p.handles(id) {
		return p.backend.deleteGenericResource(id)
	}

	return p.dataPlane.Delete(p.request(ctx, id))
}

// List implements ResourceProvider.
func (p *MonitorProvider) List(ctx context.Context, id ResourceID) ([]map[string]any, error) {
	if !p.handles(id) {
		return p.backend.listGenericResources(id), nil
	}

	return p.dataPlane.List(p.request(ctx, id))
}

// ListKeys implements ResourceProvider; only workspaces have keys.
func (p *MonitorProvider) ListKeys(ctx context.Context, id ResourceID) (map[string]any, error) {
	if !p.handles(id) || id.LeafType() != monitorWorkspacesType {
		return nil, ErrResourceNotFound
	}

	return p.dataPlane.Action(p.request(ctx, id), "listKeys", nil)
}

// Action runs a POST action on a workspace.
func (p *MonitorProvider) Action(
	ctx context.Context, id ResourceID, action string, body map[string]any,
) (map[string]any, error) {
	return p.dataPlane.Action(p.request(ctx, id), action, body)
}

// Reset implements ResourceProvider.
func (p *MonitorProvider) Reset() { p.dataPlane.Reset() }

// DeleteResourcesInGroup implements ResourceProvider.
func (p *MonitorProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.dataPlane.DeleteInGroup(resourceGroup)
}
