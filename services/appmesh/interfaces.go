package appmesh

import (
	"encoding/json"
	"time"
)

// StorageBackend is the storage interface for the App Mesh service.
type StorageBackend interface {
	// Mesh operations
	CreateMesh(name string, spec json.RawMessage, tags map[string]string) (*Mesh, error)
	DescribeMesh(name string) (*Mesh, error)
	UpdateMesh(name string, spec json.RawMessage) (*Mesh, error)
	DeleteMesh(name string) (*Mesh, error)
	ListMeshes(maxResults int32, nextToken string) ([]*MeshSummary, string, error)

	// VirtualNode operations
	CreateVirtualNode(meshName, name string, spec json.RawMessage, tags map[string]string) (*VirtualNode, error)
	DescribeVirtualNode(meshName, name string) (*VirtualNode, error)
	UpdateVirtualNode(meshName, name string, spec json.RawMessage) (*VirtualNode, error)
	DeleteVirtualNode(meshName, name string) (*VirtualNode, error)
	ListVirtualNodes(meshName string, maxResults int32, nextToken string) ([]*VirtualNodeSummary, string, error)

	// VirtualRouter operations
	CreateVirtualRouter(meshName, name string, spec json.RawMessage, tags map[string]string) (*VirtualRouter, error)
	DescribeVirtualRouter(meshName, name string) (*VirtualRouter, error)
	UpdateVirtualRouter(meshName, name string, spec json.RawMessage) (*VirtualRouter, error)
	DeleteVirtualRouter(meshName, name string) (*VirtualRouter, error)
	ListVirtualRouters(meshName string, maxResults int32, nextToken string) ([]*VirtualRouterSummary, string, error)

	// Route operations (nested under virtual router)
	CreateRoute(meshName, virtualRouterName, routeName string, spec json.RawMessage, tags map[string]string) (*Route, error)
	DescribeRoute(meshName, virtualRouterName, routeName string) (*Route, error)
	UpdateRoute(meshName, virtualRouterName, routeName string, spec json.RawMessage) (*Route, error)
	DeleteRoute(meshName, virtualRouterName, routeName string) (*Route, error)
	ListRoutes(meshName, virtualRouterName string, maxResults int32, nextToken string) ([]*RouteSummary, string, error)

	// VirtualService operations
	CreateVirtualService(meshName, name string, spec json.RawMessage, tags map[string]string) (*VirtualService, error)
	DescribeVirtualService(meshName, name string) (*VirtualService, error)
	UpdateVirtualService(meshName, name string, spec json.RawMessage) (*VirtualService, error)
	DeleteVirtualService(meshName, name string) (*VirtualService, error)
	ListVirtualServices(meshName string, maxResults int32, nextToken string) ([]*VirtualServiceSummary, string, error)

	// VirtualGateway operations
	CreateVirtualGateway(meshName, name string, spec json.RawMessage, tags map[string]string) (*VirtualGateway, error)
	DescribeVirtualGateway(meshName, name string) (*VirtualGateway, error)
	UpdateVirtualGateway(meshName, name string, spec json.RawMessage) (*VirtualGateway, error)
	DeleteVirtualGateway(meshName, name string) (*VirtualGateway, error)
	ListVirtualGateways(meshName string, maxResults int32, nextToken string) ([]*VirtualGatewaySummary, string, error)

	// GatewayRoute operations (nested under virtual gateway)
	CreateGatewayRoute(meshName, virtualGatewayName, routeName string, spec json.RawMessage, tags map[string]string) (*GatewayRoute, error)
	DescribeGatewayRoute(meshName, virtualGatewayName, routeName string) (*GatewayRoute, error)
	UpdateGatewayRoute(meshName, virtualGatewayName, routeName string, spec json.RawMessage) (*GatewayRoute, error)
	DeleteGatewayRoute(meshName, virtualGatewayName, routeName string) (*GatewayRoute, error)
	ListGatewayRoutes(meshName, virtualGatewayName string, maxResults int32, nextToken string) ([]*GatewayRouteSummary, string, error)

	// Tag operations
	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, keys []string) error
	ListTagsForResource(arn string, maxResults int32, nextToken string) ([]TagRef, string, error)

	AccountID() string
	Region() string
	Reset()
}

// ResourceMeta holds common metadata for App Mesh resources.
// CreatedAt is first to reduce GC pointer bytes for the non-pointer time prefix.
type ResourceMeta struct {
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Arn           string
	UID           string
	MeshOwner     string
	ResourceOwner string
	Version       int64
}

// Mesh is an App Mesh service mesh.
type Mesh struct {
	Meta     ResourceMeta
	Name     string
	Spec     json.RawMessage
	Status   string
}

// MeshSummary is a mesh entry in a list response.
type MeshSummary struct {
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Arn           string
	Name          string
	MeshOwner     string
	ResourceOwner string
	Version       int64
}

// VirtualNode is an App Mesh virtual node.
type VirtualNode struct {
	Meta             ResourceMeta
	MeshName         string
	VirtualNodeName  string
	Spec             json.RawMessage
	Status           string
}

// VirtualNodeSummary is a virtual node entry in a list response.
type VirtualNodeSummary struct {
	CreatedAt       time.Time
	UpdatedAt       time.Time
	Arn             string
	MeshName        string
	VirtualNodeName string
	MeshOwner       string
	ResourceOwner   string
	Version         int64
}

// VirtualRouter is an App Mesh virtual router.
type VirtualRouter struct {
	Meta               ResourceMeta
	MeshName           string
	VirtualRouterName  string
	Spec               json.RawMessage
	Status             string
}

// VirtualRouterSummary is a virtual router entry in a list response.
type VirtualRouterSummary struct {
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Arn               string
	MeshName          string
	VirtualRouterName string
	MeshOwner         string
	ResourceOwner     string
	Version           int64
}

// Route is an App Mesh route under a virtual router.
type Route struct {
	Meta              ResourceMeta
	MeshName          string
	VirtualRouterName string
	RouteName         string
	Spec              json.RawMessage
	Status            string
}

// RouteSummary is a route entry in a list response.
type RouteSummary struct {
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Arn               string
	MeshName          string
	VirtualRouterName string
	RouteName         string
	MeshOwner         string
	ResourceOwner     string
	Version           int64
}

// VirtualService is an App Mesh virtual service.
type VirtualService struct {
	Meta               ResourceMeta
	MeshName           string
	VirtualServiceName string
	Spec               json.RawMessage
	Status             string
}

// VirtualServiceSummary is a virtual service entry in a list response.
type VirtualServiceSummary struct {
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Arn                string
	MeshName           string
	VirtualServiceName string
	MeshOwner          string
	ResourceOwner      string
	Version            int64
}

// VirtualGateway is an App Mesh virtual gateway.
type VirtualGateway struct {
	Meta               ResourceMeta
	MeshName           string
	VirtualGatewayName string
	Spec               json.RawMessage
	Status             string
}

// VirtualGatewaySummary is a virtual gateway entry in a list response.
type VirtualGatewaySummary struct {
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Arn                string
	MeshName           string
	VirtualGatewayName string
	MeshOwner          string
	ResourceOwner      string
	Version            int64
}

// GatewayRoute is an App Mesh gateway route under a virtual gateway.
type GatewayRoute struct {
	Meta               ResourceMeta
	MeshName           string
	VirtualGatewayName string
	GatewayRouteName   string
	Spec               json.RawMessage
	Status             string
}

// GatewayRouteSummary is a gateway route entry in a list response.
type GatewayRouteSummary struct {
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Arn                string
	MeshName           string
	VirtualGatewayName string
	GatewayRouteName   string
	MeshOwner          string
	ResourceOwner      string
	Version            int64
}

// TagRef is a key-value tag pair returned in list responses.
type TagRef struct {
	Key   string
	Value string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
