package kafka

import (
	"net/http"
	"net/url"
	"strings"
)

// parseKafkaPath parses an HTTP method + path into an operation name and resource ARN.
func parseKafkaPath(method, path string) (string, string) {
	if op, resource := parseClusterAndConfigPath(method, path); op != "" {
		return op, resource
	}

	return parseExtendedOpsPath(method, path)
}

// parseClusterAndConfigPath handles cluster (v1/v2), configuration, and tag paths.
func parseClusterAndConfigPath(method, path string) (string, string) {
	switch {
	case path == "/v1/clusters" || path == "/v1/clusters/":
		return parseClusterRootV1(method)
	case strings.HasPrefix(path, clustersV1Prefix):
		return parseClusterResourceV1(method, path[len(clustersV1Prefix):])
	case path == "/api/v2/clusters" || path == "/api/v2/clusters/":
		return parseClusterRootV2(method)
	case strings.HasPrefix(path, clustersV2Prefix):
		return parseClusterResourceV2(method, path[len(clustersV2Prefix):])
	case path == "/v1/configurations" || path == "/v1/configurations/":
		return parseConfigurationRoot(method)
	case strings.HasPrefix(path, configurationsPrefix):
		return parseConfigurationResource(method, path[len(configurationsPrefix):])
	case strings.HasPrefix(path, tagsPrefix):
		return parseTagsResource(method, path[len(tagsPrefix):])
	}

	return "", ""
}

// parseExtendedOpsPath handles operations, replicator, and VPC connection paths.
func parseExtendedOpsPath(method, path string) (string, string) {
	switch {
	case strings.HasPrefix(path, operationsPrefix):
		return parseOperationResource(method, path[len(operationsPrefix):])
	case strings.HasPrefix(path, operationsV2Prefix):
		return parseOperationV2Resource(method, path[len(operationsV2Prefix):])
	case path == replicatorsRoot || path == replicatorsRoot+"/":
		return parseReplicatorsRoot(method)
	case strings.HasPrefix(path, replicatorsPrefix):
		return parseReplicatorResource(method, path[len(replicatorsPrefix):])
	case path == vpcConnectionRoot || path == vpcConnectionRoot+"/":
		return parseVpcConnectionRoot(method)
	case path == vpcConnectionsListPath || path == vpcConnectionsListPath+"/":
		if method == http.MethodGet {
			return opListVpcConnections, ""
		}
	case strings.HasPrefix(path, vpcConnectionPrefix):
		return parseVpcConnectionResource(method, path[len(vpcConnectionPrefix):])
	}

	return parseMiscTopLevelPath(method, path)
}

// parseMiscTopLevelPath handles the remaining top-level GET-only paths:
// ListKafkaVersions and GetCompatibleKafkaVersions (the latter carries its
// clusterArn as a query parameter, not a path segment).
func parseMiscTopLevelPath(method, path string) (string, string) {
	switch path {
	case kafkaVersionsRoot, kafkaVersionsRoot + "/":
		if method == http.MethodGet {
			return opListKafkaVersions, ""
		}
	case compatibleKafkaVersionsPath, compatibleKafkaVersionsPath + "/":
		if method == http.MethodGet {
			return opGetCompatibleKafkaVersions, ""
		}
	}

	return "", ""
}

func parseClusterRootV1(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListClusters, ""
	case http.MethodPost:
		return opCreateCluster, ""
	}

	return "", ""
}

// parseClusterResourceV1 routes V1 cluster resource paths.
func parseClusterResourceV1(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if op, id := parseClusterResourceV1Topics(method, decoded); op != "" {
		return op, id
	}

	if op, id := parseClusterResourceV1Config(method, decoded); op != "" {
		return op, id
	}

	switch method {
	case http.MethodGet:
		return opDescribeCluster, decoded
	case http.MethodDelete:
		return opDeleteCluster, decoded
	}

	return "", ""
}

// parseClusterResourceV1Topics handles topic and scram secret sub-paths.
func parseClusterResourceV1Topics(method, decoded string) (string, string) {
	// /topics/{topicName}/partitions: DescribeTopicPartitions. Must be checked
	// before the generic /topics/{topicName} case below.
	if strings.HasSuffix(decoded, topicPartitionsSuffix) {
		trimmed := decoded[:len(decoded)-len(topicPartitionsSuffix)]

		if idx := strings.Index(trimmed, topicsSuffix+"/"); idx != -1 {
			arnStr := trimmed[:idx]
			topicName := trimmed[idx+len(topicsSuffix)+1:]

			if method == http.MethodGet {
				return opDescribeTopicPartitions, arnStr + topicKeySeparator + topicName
			}

			return "", ""
		}
	}

	// /topics/{topicName}: must be checked before /topics suffix.
	if idx := strings.Index(decoded, topicsSuffix+"/"); idx != -1 {
		arnStr := decoded[:idx]
		topicName := decoded[idx+len(topicsSuffix)+1:]

		switch method {
		case http.MethodDelete:
			return opDeleteTopic, arnStr + topicKeySeparator + topicName
		case http.MethodGet:
			return opDescribeTopic, arnStr + topicKeySeparator + topicName
		case http.MethodPut:
			return opUpdateTopic, arnStr + topicKeySeparator + topicName
		}

		return "", ""
	}

	// /topics (no trailing topic name): CreateTopic (POST) or ListTopics (GET).
	if strings.HasSuffix(decoded, topicsSuffix) {
		arnStr := decoded[:len(decoded)-len(topicsSuffix)]

		switch method {
		case http.MethodPost:
			return opCreateTopic, arnStr
		case http.MethodGet:
			return opListTopics, arnStr
		}

		return "", ""
	}

	// /scram-secrets: BatchAssociateScramSecret (POST), BatchDisassociateScramSecret (PATCH), ListScramSecrets (GET).
	if strings.HasSuffix(decoded, scramSecretsSuffix) {
		arnStr := decoded[:len(decoded)-len(scramSecretsSuffix)]

		switch method {
		case http.MethodPost:
			return opBatchAssociateScramSecret, arnStr
		case http.MethodPatch:
			return opBatchDisassociateScramSecret, arnStr
		case http.MethodGet:
			return opListScramSecrets, arnStr
		}

		return "", ""
	}

	return "", ""
}

// parseClusterResourceV1Config handles policy, broker, VPC, and operations sub-paths.
func parseClusterResourceV1Config(method, decoded string) (string, string) {
	// /policy: DeleteClusterPolicy (DELETE), GetClusterPolicy (GET), PutClusterPolicy (PUT).
	if strings.HasSuffix(decoded, policySuffix) {
		arnStr := decoded[:len(decoded)-len(policySuffix)]

		switch method {
		case http.MethodDelete:
			return opDeleteClusterPolicy, arnStr
		case http.MethodGet:
			return opGetClusterPolicy, arnStr
		case http.MethodPut:
			return opPutClusterPolicy, arnStr
		}

		return "", ""
	}

	// /bootstrap-brokers: GetBootstrapBrokers.
	if strings.HasSuffix(decoded, bootstrapBrokersSuffix) {
		arnStr := decoded[:len(decoded)-len(bootstrapBrokersSuffix)]

		if method == http.MethodGet {
			return opGetBootstrapBrokers, arnStr
		}

		return "", ""
	}

	if op, id := parseClusterResourceV1BrokerUpdates(method, decoded); op != "" {
		return op, id
	}

	if op, id := parseClusterResourceV1ClusterUpdates(method, decoded); op != "" {
		return op, id
	}

	if op, id := parseClusterResourceV1Misc(method, decoded); op != "" {
		return op, id
	}

	return "", ""
}

// clusterUpdateOpFor returns op with the ARN stripped of suffix when method
// matches wantMethod, otherwise it reports no match. Used for the single-method
// V1 cluster update sub-paths (all PUT except UpdateSecurity, which is PATCH).
func clusterUpdateOpFor(method, wantMethod, op, decoded, suffix string) (string, string) {
	if method != wantMethod {
		return "", ""
	}

	return op, decoded[:len(decoded)-len(suffix)]
}

// parseClusterResourceV1BrokerUpdates handles the /nodes/{count,storage,type}
// broker update sub-paths (PUT). Must be checked before the generic /nodes
// (ListNodes) and /storage (UpdateStorage) suffix checks, since e.g.
// ".../nodes/storage" also ends with "/storage".
func parseClusterResourceV1BrokerUpdates(method, decoded string) (string, string) {
	if strings.HasSuffix(decoded, brokerCountUpdateSuffix) {
		return clusterUpdateOpFor(
			method,
			http.MethodPut,
			opUpdateBrokerCount,
			decoded,
			brokerCountUpdateSuffix,
		)
	}

	if strings.HasSuffix(decoded, brokerStorageUpdateSuffix) {
		return clusterUpdateOpFor(
			method,
			http.MethodPut,
			opUpdateBrokerStorage,
			decoded,
			brokerStorageUpdateSuffix,
		)
	}

	if strings.HasSuffix(decoded, brokerTypeUpdateSuffix) {
		return clusterUpdateOpFor(
			method,
			http.MethodPut,
			opUpdateBrokerType,
			decoded,
			brokerTypeUpdateSuffix,
		)
	}

	return "", ""
}

// parseClusterResourceV1ClusterUpdates handles the cluster-level V1 update
// sub-paths: configuration, kafka version, connectivity, monitoring,
// rebalancing, security (PATCH), and storage.
func parseClusterResourceV1ClusterUpdates(method, decoded string) (string, string) {
	if strings.HasSuffix(decoded, clusterConfigUpdateSuffix) {
		return clusterUpdateOpFor(
			method,
			http.MethodPut,
			opUpdateClusterConfiguration,
			decoded,
			clusterConfigUpdateSuffix,
		)
	}

	if strings.HasSuffix(decoded, clusterKafkaVersionSuffix) {
		return clusterUpdateOpFor(
			method,
			http.MethodPut,
			opUpdateClusterKafkaVersion,
			decoded,
			clusterKafkaVersionSuffix,
		)
	}

	if strings.HasSuffix(decoded, connectivityUpdateSuffix) {
		return clusterUpdateOpFor(
			method,
			http.MethodPut,
			opUpdateConnectivity,
			decoded,
			connectivityUpdateSuffix,
		)
	}

	if strings.HasSuffix(decoded, monitoringUpdateSuffix) {
		return clusterUpdateOpFor(
			method,
			http.MethodPut,
			opUpdateMonitoring,
			decoded,
			monitoringUpdateSuffix,
		)
	}

	if strings.HasSuffix(decoded, rebalancingUpdateSuffix) {
		return clusterUpdateOpFor(
			method,
			http.MethodPut,
			opUpdateRebalancing,
			decoded,
			rebalancingUpdateSuffix,
		)
	}

	if strings.HasSuffix(decoded, securityUpdateSuffix) {
		return clusterUpdateOpFor(
			method,
			http.MethodPatch,
			opUpdateSecurity,
			decoded,
			securityUpdateSuffix,
		)
	}

	if strings.HasSuffix(decoded, storageUpdateSuffix) {
		return clusterUpdateOpFor(
			method,
			http.MethodPut,
			opUpdateStorage,
			decoded,
			storageUpdateSuffix,
		)
	}

	return "", ""
}

// parseClusterResourceV1Misc handles nodes, reboot, VPC, and operations paths.
func parseClusterResourceV1Misc(method, decoded string) (string, string) {
	// /nodes: ListNodes.
	if strings.HasSuffix(decoded, nodesSuffix) {
		arnStr := decoded[:len(decoded)-len(nodesSuffix)]

		if method == http.MethodGet {
			return opListNodes, arnStr
		}

		return "", ""
	}

	// /reboot-broker: RebootBroker.
	if strings.HasSuffix(decoded, rebootBrokerSuffix) {
		arnStr := decoded[:len(decoded)-len(rebootBrokerSuffix)]

		if method == http.MethodPut {
			return opRebootBroker, arnStr
		}

		return "", ""
	}

	// /client-vpc-connections: ListClientVpcConnections.
	if strings.HasSuffix(decoded, clientVpcConnectionSuffix) {
		arnStr := decoded[:len(decoded)-len(clientVpcConnectionSuffix)]

		if method == http.MethodGet {
			return opListClientVpcConnections, arnStr
		}

		return "", ""
	}

	// /client-vpc-connection (singular): RejectClientVpcConnection. The target VPC
	// connection ARN travels in the request body (vpcConnectionArn), not the path --
	// the resource here is the owning cluster ARN. Must be checked after the plural
	// /client-vpc-connections (ListClientVpcConnections) suffix above, since HasSuffix
	// on the shorter singular suffix would otherwise never be reached first anyway
	// (the strings differ in their final character, so order doesn't matter here).
	if strings.HasSuffix(decoded, clientVpcConnectionSingular) {
		arnStr := decoded[:len(decoded)-len(clientVpcConnectionSingular)]

		if method == http.MethodPut {
			return opRejectClientVpcConnection, arnStr
		}

		return "", ""
	}

	// /operations: ListClusterOperations (GET).
	if strings.HasSuffix(decoded, "/operations") {
		arnStr := decoded[:len(decoded)-len("/operations")]

		if method == http.MethodGet {
			return opListClusterOperations, arnStr
		}

		return "", ""
	}

	return "", ""
}

func parseClusterRootV2(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListClustersV2, ""
	case http.MethodPost:
		return opCreateClusterV2, ""
	}

	return "", ""
}

// parseClusterResourceV2 handles /api/v2/clusters/{ClusterArn} sub-paths. Unlike
// the V1 tree, the real MSK API only exposes ListClusterOperationsV2 and
// DescribeClusterV2 under the /api/v2/clusters prefix -- every Update* operation
// (UpdateBrokerCount, UpdateSecurity, etc.) actually lives under /v1/clusters/
// (see parseClusterResourceV1BrokerUpdates / parseClusterResourceV1ClusterUpdates),
// even though ClusterV2 is the "modern" describe/list surface.
func parseClusterResourceV2(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	// /operations: ListClusterOperationsV2.
	if strings.HasSuffix(decoded, "/operations") {
		arnStr := decoded[:len(decoded)-len("/operations")]

		if method == http.MethodGet {
			return opListClusterOperationsV2, arnStr
		}

		return "", ""
	}

	if method == http.MethodGet {
		return opDescribeClusterV2, decoded
	}

	return "", ""
}

func parseConfigurationRoot(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListConfigurations, ""
	case http.MethodPost:
		return opCreateConfiguration, ""
	}

	return "", ""
}

func parseConfigurationResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	// /revisions/{revision}: DescribeConfigurationRevision (GET) or ListConfigurationRevisions (GET on root).
	if idx := strings.Index(decoded, revisionsSuffix+"/"); idx != -1 {
		configArn := decoded[:idx]
		revision := decoded[idx+len(revisionsSuffix)+1:]

		if method == http.MethodGet {
			return opDescribeConfigurationRevision, configArn + topicKeySeparator + revision
		}

		return "", ""
	}

	if strings.HasSuffix(decoded, revisionsSuffix) {
		configArn := decoded[:len(decoded)-len(revisionsSuffix)]

		if method == http.MethodGet {
			return opListConfigurationRevisions, configArn
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opDescribeConfiguration, decoded
	case http.MethodDelete:
		return opDeleteConfiguration, decoded
	case http.MethodPut:
		return opUpdateConfiguration, decoded
	}

	return "", ""
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

func parseOperationResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if method == http.MethodGet {
		return opDescribeClusterOperation, decoded
	}

	return "", ""
}

func parseOperationV2Resource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if method == http.MethodGet {
		return opDescribeClusterOperationV2, decoded
	}

	return "", ""
}

func parseReplicatorsRoot(method string) (string, string) {
	switch method {
	case http.MethodPost:
		return opCreateReplicator, ""
	case http.MethodGet:
		return opListReplicators, ""
	}

	return "", ""
}

func parseReplicatorResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	switch method {
	case http.MethodDelete:
		return opDeleteReplicator, decoded
	case http.MethodGet:
		return opDescribeReplicator, decoded
	case http.MethodPut:
		// UpdateReplicationInfo lives at .../replicators/{ReplicatorArn}/replication-info,
		// not bare .../replicators/{ReplicatorArn}; strip the suffix to recover the ARN.
		if strings.HasSuffix(decoded, replicationInfoSuffix) {
			return opUpdateReplicationInfo, decoded[:len(decoded)-len(replicationInfoSuffix)]
		}
	}

	return "", ""
}

// parseVpcConnectionRoot handles the singular /v1/vpc-connection root, which
// only accepts CreateVpcConnection. ListVpcConnections lives at the distinct
// plural /v1/vpc-connections path (see vpcConnectionsListPath).
func parseVpcConnectionRoot(method string) (string, string) {
	if method == http.MethodPost {
		return opCreateVpcConnection, ""
	}

	return "", ""
}

func parseVpcConnectionResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	switch method {
	case http.MethodDelete:
		return opDeleteVpcConnection, decoded
	case http.MethodGet:
		return opDescribeVpcConnection, decoded
	}

	return "", ""
}
