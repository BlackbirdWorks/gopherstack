package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/blackbirdworks/gopherstack/services/gcpcontainer"
)

const (
	gcpContainerProxyPrefix = "/dashboard/api/v1/gcp/container"

	gkeLocationsIdx = 2
	gkeKindIdx      = 4
	gkeColl         = 5
	gkeItem         = 6
	gkePoolColl     = 7
	gkePoolItem     = 8
	gkeClusters     = "clusters"
	gkeOperations   = "operations"
	gkeNodePools    = "nodePools"
	gkeVerbSetSize  = "setSize"
	gkeVerbSizes    = "poolSizes"
)

// registerGCPContainerRoutes exposes the Kubernetes Engine API to the SPA as REST JSON, calling the backend
// in-process. Only reads, node pool resize and cluster deletion are reachable; cluster credentials are removed.
func (h *DashboardHandler) registerGCPContainerRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPContainerOps; ops != nil {
		route = (&gcpContainerAPI{b: ops.Backend}).route
	}

	h.registerGCPRESTProxy(gcpContainerProxyPrefix, route)
}

type gcpContainerAPI struct {
	b *gcpcontainer.InMemoryBackend
}

func (a *gcpContainerAPI) route(_ context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < gkeColl || n > gkePoolItem || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[gkeLocationsIdx] != schedLocationsKind {
		return nil, errGCPUnsupported()
	}

	switch r.segs[gkeKindIdx] {
	case gkeClusters:
		return a.clusters(r)
	case gkeOperations:
		return a.operations(r)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpContainerAPI) operations(r gcpRESTCall) (proto.Message, error) {
	if r.method != http.MethodGet || r.verb != "" {
		return nil, errGCPUnsupported()
	}

	switch len(r.segs) {
	case gkeColl:
		return a.b.ListOperations(&containerpb.ListOperationsRequest{Parent: r.name(gkeKindIdx)})
	case gkeItem:
		return a.b.GetOperation(&containerpb.GetOperationRequest{Name: r.name(gkeItem)})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpContainerAPI) clusters(r gcpRESTCall) (proto.Message, error) {
	switch len(r.segs) {
	case gkeColl:
		if r.method == http.MethodGet && r.verb == "" {
			out, err := a.b.ListClusters(&containerpb.ListClustersRequest{Parent: r.name(gkeKindIdx)})
			if err != nil {
				return nil, err
			}

			for _, c := range out.GetClusters() {
				scrubGKECluster(c)
			}

			return out, nil
		}
	case gkeItem:
		return a.cluster(r)
	case gkePoolColl:
		if r.method == http.MethodGet && r.verb == "" && r.segs[gkeItem] == gkeNodePools {
			return a.b.ListNodePools(&containerpb.ListNodePoolsRequest{Parent: r.name(gkeItem)})
		}
	case gkePoolItem:
		return a.nodePool(r)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpContainerAPI) cluster(r gcpRESTCall) (proto.Message, error) {
	name := r.name(gkeItem)

	switch {
	case r.method == http.MethodGet && r.verb == "":
		c, err := a.b.GetCluster(&containerpb.GetClusterRequest{Name: name})
		if err != nil {
			return nil, err
		}

		scrubGKECluster(c)

		return c, nil
	case r.method == http.MethodGet && r.verb == gkeVerbSizes:
		sizes, err := a.b.NodePoolSizes(name)
		if err != nil {
			return nil, err
		}

		fields := make(map[string]any, len(sizes))
		for pool, size := range sizes {
			fields[pool] = float64(size)
		}

		return structpb.NewStruct(fields)
	case r.method == http.MethodDelete && r.verb == "":
		return a.b.DeleteCluster(&containerpb.DeleteClusterRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpContainerAPI) nodePool(r gcpRESTCall) (proto.Message, error) {
	name := r.name(gkePoolItem)

	if r.segs[gkeItem] != gkeNodePools {
		return nil, errGCPUnsupported()
	}

	switch {
	case r.method == http.MethodGet && r.verb == "":
		return a.b.GetNodePool(&containerpb.GetNodePoolRequest{Name: name})
	case r.method == http.MethodPost && r.verb == gkeVerbSetSize:
		in := &containerpb.SetNodePoolSizeRequest{}
		if err := r.unmarshal(in); err != nil {
			return nil, err
		}

		in.Name = name

		return a.b.SetNodePoolSize(in)
	}

	return nil, errGCPUnsupported()
}

// scrubGKECluster keeps only the public CA certificate of the cluster's master auth.
func scrubGKECluster(c *containerpb.Cluster) {
	if c.GetMasterAuth() == nil {
		return
	}

	c.MasterAuth = &containerpb.MasterAuth{ClusterCaCertificate: c.GetMasterAuth().GetClusterCaCertificate()}
}
