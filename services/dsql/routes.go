package dsql

import (
	"net/http"
	"net/url"
	"strings"
)

const (
	pathClusterRoot    = "/cluster"
	pathClusterPrefix  = "/cluster/"
	pathClustersPrefix = "/clusters/" // plural: GetVpcEndpointServiceName only
	pathTagsPrefix     = "/tags/"
	pathStreamPrefix   = "/stream/"

	policySuffix                 = "/policy"
	vpcEndpointServiceNameSuffix = "/vpc-endpoint-service-name"
)

// Operation names, matching the AWS API exactly.
const (
	opCreateCluster = "CreateCluster"
	opGetCluster    = "GetCluster"
	opListClusters  = "ListClusters"
	opUpdateCluster = "UpdateCluster"
	opDeleteCluster = "DeleteCluster"

	opGetClusterPolicy    = "GetClusterPolicy"
	opPutClusterPolicy    = "PutClusterPolicy"
	opDeleteClusterPolicy = "DeleteClusterPolicy"

	opGetVpcEndpointServiceName = "GetVpcEndpointServiceName"

	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"

	opCreateStream = "CreateStream"
	opGetStream    = "GetStream"
	opDeleteStream = "DeleteStream"
	opListStreams  = "ListStreams"
)

// parseDSQLPath parses an HTTP method + path into an operation name and a
// resource identifier: a cluster identifier, a "clusterId/streamId"
// composite (see streamKey), or a resource ARN for the /tags/ family.
func parseDSQLPath(method, path string) (string, string) {
	switch {
	case path == pathClusterRoot || path == pathClusterRoot+"/":
		return parseClusterRoot(method)
	case strings.HasPrefix(path, pathClusterPrefix):
		return parseClusterResource(method, path[len(pathClusterPrefix):])
	case strings.HasPrefix(path, pathClustersPrefix):
		return parseVpcEndpointServiceName(method, path[len(pathClustersPrefix):])
	case strings.HasPrefix(path, pathTagsPrefix):
		return parseTagsResource(method, path[len(pathTagsPrefix):])
	case strings.HasPrefix(path, pathStreamPrefix):
		return parseStreamResource(method, path[len(pathStreamPrefix):])
	}

	return "", ""
}

func parseClusterRoot(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListClusters, ""
	case http.MethodPost:
		return opCreateCluster, ""
	}

	return "", ""
}

// parseClusterResource routes /cluster/{id} and /cluster/{id}/policy paths.
func parseClusterResource(method, remainder string) (string, string) {
	if id, ok := strings.CutSuffix(remainder, policySuffix); ok {
		switch method {
		case http.MethodGet:
			return opGetClusterPolicy, id
		case http.MethodPost:
			return opPutClusterPolicy, id
		case http.MethodDelete:
			return opDeleteClusterPolicy, id
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opGetCluster, remainder
	case http.MethodPost:
		return opUpdateCluster, remainder
	case http.MethodDelete:
		return opDeleteCluster, remainder
	}

	return "", ""
}

func parseVpcEndpointServiceName(method, remainder string) (string, string) {
	id, ok := strings.CutSuffix(remainder, vpcEndpointServiceNameSuffix)
	if !ok || method != http.MethodGet {
		return "", ""
	}

	return opGetVpcEndpointServiceName, id
}

func parseTagsResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	switch method {
	case http.MethodGet:
		return opListTagsForResource, decoded
	case http.MethodPost:
		return opTagResource, decoded
	case http.MethodDelete:
		return opUntagResource, decoded
	}

	return "", ""
}

// parseStreamResource routes /stream/{clusterId} (list/create) and
// /stream/{clusterId}/{streamId} (get/delete) paths.
func parseStreamResource(method, remainder string) (string, string) {
	clusterID, streamID, hasStream := strings.Cut(remainder, "/")

	if !hasStream {
		switch method {
		case http.MethodGet:
			return opListStreams, clusterID
		case http.MethodPost:
			return opCreateStream, clusterID
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opGetStream, streamKey(clusterID, streamID)
	case http.MethodDelete:
		return opDeleteStream, streamKey(clusterID, streamID)
	}

	return "", ""
}
