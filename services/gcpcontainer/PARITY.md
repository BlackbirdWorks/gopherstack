---
service: gcpcontainer
sdk_module: cloud.google.com/go/container@v1.56.0
last_audit_commit: 86ab2e0a7
last_audit_date: 2026-10-29
overall: B
ops:
  ClusterManager.CreateCluster: {wire: ok, errors: partial, state: ok, persist: ok, note: "Zonal and regional, Standard and Autopilot. Validates name (40 chars, lowercase letters/digits/hyphens), location, labels, release channel and version, node pools (machine type, disk, image type, taints, labels, autoscaling, max pods), ipAllocationPolicy (pod /8-/21 and services /16-/27 ranges, '/N' size requests auto-allocated, secondary range names resolved against gcpcompute), private cluster master /28, authorized networks and maintenance policy. Network and subnetwork are checked against gcpcompute when wired ('default' always exists). Returns a RUNNING CREATE_CLUSTER operation."}
  ClusterManager.GetCluster: {wire: ok, errors: ok, state: ok, persist: n/a, note: "status, node counts, instanceGroupUrls and etag are derived at read time from the operations on the injectable clock."}
  ClusterManager.ListClusters: {wire: ok, errors: ok, state: ok, persist: n/a, note: "'-' lists every location."}
  ClusterManager.UpdateCluster: {wire: ok, errors: partial, state: ok, persist: ok, note: "Desired* fields map onto the same-named Cluster or NetworkConfig field; master/node version, node pool autoscaling and image type, locations, cluster autoscaling, logging/monitoring service, addons and authorized networks have dedicated handling. Fields that cannot be honoured are 400, never ignored."}
  ClusterManager.DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "Allowed while provisioning (the create operation is aborted); STOPPING until the DELETE_CLUSTER operation ends."}
  ClusterManager.SetLabels: {wire: ok, errors: partial, state: ok, persist: ok, note: "labelFingerprint required and checked."}
  ClusterManager.SetMasterAuth: {wire: ok, errors: partial, state: ok, persist: ok, note: "SET_PASSWORD, GENERATE_PASSWORD, SET_USERNAME store basic-auth credentials on masterAuth; they are not enforced and do not reach a docker-engine API server."}
  ClusterManager.SetLegacyAbac: {wire: ok, errors: ok, state: ok, persist: ok, note: "Rejected for Autopilot."}
  ClusterManager.SetNetworkPolicy: {wire: ok, errors: partial, state: ok, persist: ok, note: "CALICO only; the network policy addon must be enabled first. Stored, not enforced."}
  ClusterManager.SetMaintenancePolicy: {wire: ok, errors: partial, state: ok, persist: ok, note: "resourceVersion is required and checked; daily, recurring and exclusion windows are validated. The 48h-per-32-days availability rule is not enforced."}
  ClusterManager.StartIPRotation: {wire: ok, errors: partial, state: ok, persist: ok, note: "Endpoint changes and, with rotateCredentials, a new CA is issued. Rejected with the docker engine."}
  ClusterManager.CompleteIPRotation: {wire: ok, errors: partial, state: ok, persist: ok, note: "FAILED_PRECONDITION when no rotation is in progress."}
  ClusterManager.SetLoggingService: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ClusterManager.SetMonitoringService: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ClusterManager.SetAddonsConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "Each addon section in the request replaces the stored one."}
  ClusterManager.SetLocations: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ClusterManager.UpdateMaster: {wire: ok, errors: ok, state: ok, persist: ok, note: "Upgrade only."}
  ClusterManager.CreateNodePool: {wire: ok, errors: partial, state: ok, persist: ok, note: "Not allowed on Autopilot. Pool operations on different pools run concurrently; cluster-wide operations conflict with everything."}
  ClusterManager.GetNodePool: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ClusterManager.ListNodePools: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ClusterManager.UpdateNodePool: {wire: ok, errors: partial, state: ok, persist: ok, note: "Version, image type, machine type, disk, locations, tags, taints, labels, resource labels, upgrade settings and any request field with a same-named NodeConfig field; others are 400. Etag checked."}
  ClusterManager.DeleteNodePool: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ClusterManager.SetNodePoolSize: {wire: ok, errors: ok, state: ok, persist: ok, note: "Rejected while autoscaling is enabled."}
  ClusterManager.SetNodePoolAutoscaling: {wire: ok, errors: ok, state: ok, persist: ok, note: "Per-zone and total limits are exclusive; the size is clamped into the limits."}
  ClusterManager.SetNodePoolManagement: {wire: ok, errors: ok, state: ok, persist: ok, note: "Auto-upgrade cannot be disabled on a release channel."}
  ClusterManager.RollbackNodePoolUpgrade: {wire: ok, errors: ok, state: ok, persist: ok, note: "Restores the version before the last upgrade."}
  ClusterManager.GetOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "container.v1 Operation resource (not google.longrunning); RUNNING until the injectable clock passes its end, or until the k3s API server answers with the docker engine."}
  ClusterManager.ListOperations: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ClusterManager.GetServerConfig: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Documented representative catalog: four release channels with default, valid and upgrade-target versions, valid master and node versions, image types."}
  ClusterManager.CancelOperation: {wire: gap, errors: gap, state: gap, persist: n/a, note: "Unimplemented (UNIMPLEMENTED)."}
  ClusterManager.GetJSONWebKeys: {wire: gap, errors: gap, state: gap, persist: n/a, note: "Unimplemented (UNIMPLEMENTED)."}
  ClusterManager.ListUsableSubnetworks: {wire: gap, errors: gap, state: gap, persist: n/a, note: "Unimplemented (UNIMPLEMENTED)."}
  ClusterManager.CheckAutopilotCompatibility: {wire: gap, errors: gap, state: gap, persist: n/a, note: "Unimplemented (UNIMPLEMENTED)."}
  ClusterManager.FetchClusterUpgradeInfo: {wire: gap, errors: gap, state: gap, persist: n/a, note: "Unimplemented (UNIMPLEMENTED); also FetchNodePoolUpgradeInfo, CompleteNodePoolUpgrade and CompleteControlPlaneUpgrade."}
families:
  operations_clock: {status: ok, note: "Statuses of clusters, node pools and operations are derived from timestamps on an injectable clock (InMemoryBackend.SetClock, SetTiming): cluster PROVISIONING, RECONCILING, STOPPING, RUNNING."}
  docker_engine: {status: ok, note: "GKE_ENGINE=docker (or --engines) starts one single-node k3s container per cluster through the shared pkgs/k3sengine (also used by EKS). Endpoint is host:port, masterAuth.clusterCaCertificate the serving CA, bearer token GKE_CLUSTER_TOKEN (default documented token). Node pools stay metadata."}
  routing_isolation: {status: ok, note: "Own gRPC and REST listener on --gcp-container-port / GCP_CONTAINER_PORT (default 8144), reserved in the shared PortAlloc pool, never on the shared AWS router."}
  persistence: {status: ok, note: "Clusters, node pools (with sizes and previous versions), rotation state and operations are in backendSnapshot; docker-engine clusters are started again on restore."}
  auth: {status: partial, note: "No request is authenticated or authorised."}
items_still_open:
  - "The control plane is metadata: without GKE_ENGINE=docker the endpoint is a documentation-range address with a real but unused CA, and node pools never run nodes (the docker engine runs one k3s node regardless of pool sizes). Instance groups named in instanceGroupUrls are not created in gcpcompute."
  - "Unimplemented RPCs: CancelOperation, GetJSONWebKeys, ListUsableSubnetworks, CheckAutopilotCompatibility, FetchClusterUpgradeInfo, FetchNodePoolUpgradeInfo, CompleteNodePoolUpgrade, CompleteControlPlaneUpgrade."
  - "Version catalog and region/zone lists are a fixed representative set; Autopilot being regional-only, error message texts and the PreconditionFailure/BadRequest/ResourceInfo detail payloads were chosen without a production reference (only the cluster name rules were checked against Google docs)."
  - "createSubnetwork is not honoured (no subnet is created in gcpcompute), pod/service ranges of other clusters are only avoided for auto-allocation, and quotas are not modelled."
  - "No dashboard page, and the emulator does not enforce IAM or return permission errors."
---
