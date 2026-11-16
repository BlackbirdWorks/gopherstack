package ocicontainerengine

const clusterAutoscalerCfgs = `
cluster-autoscaler.ContainerResources | O | null | CPU and memory requests and limits for autoscaler containers.
annotations | O |  | Annotations added to the Cluster Autoscaler deployment.
authType | R | instance | Authentication type: instance principal or workload identity.
balanceSimilarNodeGroups | O | false | Detects similar node groups and balances nodes between them.
balancingIgnoreLabel | O |  | Node label ignored when comparing node group similarity.
balancingLabel | O |  | Node label used exclusively to compare node group similarity.
cordonNodeBeforeTerminating | O | false | Cordons nodes before terminating them during scale-down.
coresTotal | O | 0:320000 | Minimum and maximum CPU cores in the cluster.
daemonsetEvictionForEmptyNodes | O | false | Gracefully terminates DaemonSet pods on empty nodes.
daemonsetEvictionForOccupiedNodes | O | true | Gracefully terminates DaemonSet pods on non-empty nodes.
debuggingSnapshotEnabled | O | false | Enables the debugging snapshot feature.
emitPerNodegroupMetrics | O | false | Emits metrics per node group.
enforceNodeGroupMinSize | O | false | Scales a node group up to its configured minimum size if needed.
estimator | O | binpacking | Resource estimator type used during scale-up.
expander | O | random | Node group expander type used during scale-up.
expendablePodsPriorityCutoff | O | -10 | Pods below this priority are expendable.
ignoreDaemonsetsUtilization | O | false | Ignores DaemonSet pods in scale-down utilization calculations.
ignoreMirrorPodsUtilization | O | false | Ignores mirror pods in scale-down utilization calculations.
leaderElect | O | true | Enables leader election before running the main loop.
leaderElectLeaseDuration | O | 15s | Wait time before acquiring an unrenewed leader slot.
leaderElectRenewDeadline | O | 10s | Interval for the active instance to renew leadership.
leaderElectResourceLock | O | leases | Resource type used for leader election locking.
leaderElectRetryPeriod | O | 2s | Wait between leadership acquisition and renewal attempts.
maxAutoprovisionedNodeGroupCount | O | 15 | Maximum number of auto-provisioned node groups.
maxEmptyBulkDelete | O | 10 | Maximum number of empty nodes deleted simultaneously.
maxFailingTime | O | 15m | Maximum time since last successful run before restart.
maxGracefulTerminationSec | O | 600 | Maximum seconds to wait for pod termination during scale-down.
maxInactivity | O | 10m | Maximum time since last activity before restart.
maxNodeProvisionTime | O | 15m | Maximum time to wait for node provisioning.
maxNodesTotal | O | 0 | Maximum nodes across all node pools.
maxTotalUnreadyPercentage | O | 45 | Maximum percentage of unready nodes before halting operations.
memoryTotal | O | 0:6400000 | Minimum and maximum gigabytes of memory in the cluster.
minReplicaCount | O | 0 | Minimum replicas required to allow pod deletion during scale-down.
nodes | O |  | Min:max:node-pool OCID list managed by the autoscaler.
nodeGroupAutoDiscovery | O | none | Compartment and tag criteria for discovering node pools.
okTotalUnreadyCount | O | 3 | Allowed unready nodes regardless of percentage threshold.
recordDuplicatedEvents | O | false | Prints duplicated events within a 5-minute window.
scaleDownCandidatesPoolMinCount | R | 50 | Minimum additional non-empty scale-down candidates.
scaleDownCandidatesPoolRatio | R | 0.1 | Ratio of nodes considered as additional scale-down candidates.
scaleDownDelayAfterAdd | R | 10m | Delay after scale-up before scale-down evaluation resumes.
scaleDownDelayAfterDelete | R | 10s | Delay after node deletion before scale-down evaluation resumes.
scaleDownDelayAfterFailure | R | 3m | Delay after scale-down failure before evaluation resumes.
scaleDownEnabled | O | true | Determines whether the autoscaler scales the cluster down.
scaleDownNonEmptyCandidatesCount | R | 30 | Maximum non-empty nodes considered per iteration for drain.
scaleDownUnneededTime | R | 10m | Time a node must be unneeded before scale-down eligibility.
scaleDownUnreadyTime | R | 20m | Time an unready node must be unneeded before scale-down eligibility.
scaleDownUtilizationThreshold | R | 0.5 | Utilization level below which a node becomes a scale-down candidate.
scanInterval | O | 10s | How often the cluster is re-evaluated for scaling.
skipNodesWithCustomControllerPods | O | true | Never deletes nodes with pods owned by custom controllers.
skipNodesWithLocalStorage | O | true | Never deletes nodes with pods using local storage.
skipNodesWithSystemPods | O | true | Never deletes nodes with kube-system pods, except DaemonSet or mirror pods.
statusConfigMapName | O | cluster-autoscaler-status | Name of the status ConfigMap the autoscaler writes.
stderrthreshold | O | info | Log severity threshold above which logs go to stderr.
unremovableNodeRecheckTimeout | R | 5m | Timeout before rechecking a node that could not be removed.
v | O | 0 | Logging verbosity level.
writeStatusConfigmap | O | true | Determines whether status information is written to a ConfigMap.`

const nodeFeatureDiscoveryCfgs = `
master.tolerations | O | null | Custom tolerations for NFD master pods.
master.affinity | O | null | Node affinity rules for NFD master pods.
master.config | O | null | Additional master config merged into nfd-master.conf.
master.resources | O | null | Resource requests and limits for the NFD master container.
master.replicaCount | O | null | Replica count for the NFD master deployment.
master.extraArgs | O | null | Additional command-line arguments for the NFD master container.
master.annotations | O | null | Annotations merged into the NFD master pod template.
priorityClassName | O | null | PriorityClassName for NFD master and gc deployments.
worker.config | O | null | Additional worker config merged into nfd-worker.conf.
worker.resources | O | null | Resource requests and limits for the NFD worker container.
worker.tolerations | O | null | Additional tolerations appended to NFD worker pods.
worker.nodeSelector | O | null | Node selector labels merged into the NFD worker DaemonSet.
worker.priorityClassName | O | null | PriorityClassName for the NFD worker DaemonSet.
worker.extraArgs | O | null | Additional command-line arguments for the NFD worker container.
worker.annotations | O | null | Annotations merged into the NFD worker pod template.
master.customConfigData | O |  | Custom content for the node-feature-discovery-master-custom-conf ConfigMap.
worker.customConfigData | O |  | Custom content for the node-feature-discovery-worker-custom-conf ConfigMap.`

const nvidiaGpuOperatorCfgs = `
skipNodeFeatureDiscoveryDependencyCheck | O | false | Skip the Node Feature Discovery dependency check.
disableNvidiaGpuPlugin | O | false | Disable the existing NvidiaDevicePlugin add-on and resources.
cdi.enabled | O | false | Enable the Container Device Interface.
cdi.default | O | false | Use CDI as the default device injection option.
daemonsets.labels | O | null | Labels for all GPU Operator DaemonSets.
daemonsets.annotations | O | null | Annotations for all GPU Operator DaemonSets.
daemonsets.tolerations | O | null | Tolerations for all GPU Operator DaemonSets.
daemonsets.rollingUpdate.maxUnavailable | O | 1 | Maximum unavailable pods during a DaemonSet rolling update.
dcgm.enabled | O | false | Enable the DCGM Hostengine as a separate pod.
dcgm.args | O | null | Arguments for the DCGM Hostengine.
dcgm.env | O | null | Environment variables for the DCGM Hostengine.
dcgm.containerResources | O | null | Resource requests and limits for the DCGM Hostengine pod.
dcgmExporter.enabled | O | true | Enable the DCGM Exporter DaemonSet for metrics.
dcgmExporter.env | O | null | Environment variables for the DCGM Exporter.
dcgmExporter.resources | O | null | Resource requests and limits for the DCGM Exporter pod.
dcgmExporter.service.internalTrafficPolicy | O | Cluster | InternalTrafficPolicy for the DCGM Exporter service.
dcgmExporter.serviceMonitor.enabled | O | false | Enable a ServiceMonitor for the DCGM Exporter.
dcgmExporter.serviceMonitor.interval | O | 15s | Scrape interval for the DCGM Exporter.
dcgmExporter.serviceMonitor.honorLabels | O | false | Prefer target metric labels on collisions.
dcgmExporter.serviceMonitor.additionalLabels | O | null | Additional labels for the ServiceMonitor.
dcgmExporter.serviceMonitor.relabelings | O | null | Relabeling rules for the ServiceMonitor.
dcgmExporter.config.name | O | null | ConfigMap name for custom exporter metrics.
migManager.enabled | O | false | Enable deployment of the MIG Manager.
migManager.resources | O | null | Resource requests and limits for MIG Manager pods.
migManager.env | O | null | Environment variables for the MIG Manager.
migManager.config.name | O | null | ConfigMap name with the mig-parted configuration.
migManager.gpuClientsConfig.name | O | null | ConfigMap name with the gpu-clients configuration.
devicePlugin.enabled | O | true | Enable the NVIDIA Kubernetes Device Plugin.
devicePlugin.args | O | null | Arguments for the Device Plugin.
devicePlugin.env | O | null | Environment variables for the Device Plugin.
devicePlugin.resources | O | null | Resource requests and limits for Device Plugin pods.
devicePlugin.config.name | O | null | ConfigMap name providing the Device Plugin configuration.
devicePlugin.mps.root | O | /run/nvidia/mps | MPS root path on the host.
toolkit.enabled | O | true | Enable deployment of the NVIDIA Container Toolkit.
toolkit.env | O | null | Environment variables for the Container Toolkit.
toolkit.resources | O | null | Resource requests and limits for Toolkit pods.
toolkit.installDir | O | /usr/local/nvidia | Container Toolkit install directory on the host.
mig.strategy | O | single | MIG strategy: none, single, or mixed.
operator.logging.level | O | info | Zap logging level: debug, info, error, or an integer.
operator.resources | O | null | Resource requests and limits for the GPU Operator pod.
hostPaths.rootFS | O | / | Host root filesystem path; must be chroot-able.
hostPaths.driverInstallDir | O | /run/nvidia/driver | Root directory for NVIDIA driver files on the host.
driver.rdma.enabled | O | false | Enable RDMA support for the NVIDIA driver.
driver.rdma.useHostMofed | O | false | Use the host-provided Mellanox OFED instead of the container's.`

const nvidiaNetworkOperatorCfgs = `
operator.nodeSelectors | O | null | Node selectors for operator pods.
operator.tolerations | O | null | Tolerations for operator pods.
nicClusterPolicy.tolerations | O | null | Tolerations for NicClusterPolicy DaemonSets.
nicClusterPolicy.deploymentTolerations | O | null | Tolerations for NicClusterPolicy Deployments.
operator.affinity | O | null | Affinity rules for operator pods.
operator.resources | O | null | CPU and memory limits for the operator container.
operator.cniBinDirectory | O | /opt/cni/bin | Node directory for CNI binaries.
operator.cniNetworkDirectory | O | /etc/cni/net.d | Node directory for CNI configuration files.
operator.admissionControllers.enabled | O | false | Enables operator admission controllers.
sriovNetworkOperator.enabled | O | false | Deploys the SR-IOV Network Operator.
sriov-network-operator.operator.resourcePrefix | O | nvidia.com | Prefix for SR-IOV extended resource names.
sriov-network-operator.operator.admissionControllers.enabled | O | false | Enables SR-IOV admission controllers.
sriov-network-operator.operator.sriovOperatorConfig.configDaemonNodeSelectors | O | null | SR-IOV daemon node selectors.
vfCreationMode | O | custom | VF creation mode: sriovNetworkOperator or custom.
customizeVfCreationConfigMap | O | false | Skips creating the vf-shape-config ConfigMap.
skipNodeFeatureDiscoveryDependencyCheck | O | false | Skips the Node Feature Discovery dependency check.`
