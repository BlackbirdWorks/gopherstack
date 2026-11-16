package ocicontainerengine

import "strings"

// addonCfg is one configuration argument of an add-on, from the per-add-on pages linked at
// docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contengconfiguringclusteraddons-configurationarguments.htm.
type addonCfg struct {
	key, def, desc string
	required       bool
}

const (
	allCommon = "affinity,nodeSelectors,numOfReplicas,rollingUpdate,tolerations,topologySpreadConstraints"
	noDefault = "null"
	cfgFields = 4
)

func commonCfgs(used string) []addonCfg {
	all := []addonCfg{
		{key: "affinity", desc: "A group of affinity scheduling rules."},
		{
			key:  "nodeSelectors",
			desc: "Key/value pairs selecting the worker nodes add-on pods run on; must match node labels.",
		},
		{key: "numOfReplicas", def: "1", required: true, desc: "The number of replicas of the add-on deployment."},
		{key: "rollingUpdate", desc: "Controls the rolling update behavior through maxSurge and maxUnavailable."},
		{key: "tolerations", desc: "Tolerations that allow add-on pods to run on worker nodes with matching taints."},
		{key: "topologySpreadConstraints", desc: "How matching pods are spread among the given topology."},
	}

	var out []addonCfg

	for _, c := range all {
		if strings.Contains(","+used+",", ","+c.key+",") {
			out = append(out, c)
		}
	}

	return out
}

// parseCfgs reads "key | R or O | default | description" lines.
func parseCfgs(table string) []addonCfg {
	var out []addonCfg

	for line := range strings.SplitSeq(strings.TrimSpace(table), "\n") {
		f := strings.SplitN(line, " | ", cfgFields)
		if len(f) != cfgFields {
			continue
		}

		c := addonCfg{key: f[0], required: f[1] == "R", def: f[2], desc: f[3]}
		if c.def == noDefault {
			c.def = ""
		}

		out = append(out, c)
	}

	return out
}

type addonCfgSet struct {
	common   string
	specific string
}

func addonCfgSets() map[string]addonCfgSet {
	return map[string]addonCfgSet{
		"kubeproxy": {allCommon, `
customizeKubeProxyConfigMap | R | false | false: Oracle manages kube-proxy; true: you customize it with a configMap.
kube-proxy.ContainerResources | O | null | Resource requests and limits for the add-on containers.`},
		"coredns": {allCommon, `
coreDnsContainerResources | O | null | Resource requests and limits for the add-on containers.
coreDnsEnhancedMetrics | O | true | false generates default metrics; true generates more granular metrics.
customizeCoreDNSConfigMap | R | false | false: Oracle manages CoreDNS; true: you customize it with a configMap.
minReplica | R | 1 | Minimum number of CoreDNS deployment replicas.
nodesPerReplica | R | 1 | Number of CoreDNS replicas per cluster node.`},
		"ocivcnnativepodnetworkingcniplugin": {allCommon, `
install-cni-ips.ContainerResources | O | null | Resource requests and limits for the install-cni-ips containers.`},
		"flannel": {allCommon, `
kube-flannel.ContainerResources | O | null | Resource requests and limits for the add-on containers.`},
		"observabilityagent": {allCommon, `
otelMetricsPort | O | 9995 | Port on the worker node used to expose metrics.
otelHealthCheckPort | O | 9996 | Port used by Kubernetes for the otel collector health check.
discoveryHealthCheckPort | O | 9997 | Port used by Kubernetes for the otel config agent health check.`},
		"nodeproblemdetector": {allCommon, `
enableKubernetesExporter | O | false | Runs node problem detector with the Kubernetes exporter enabled.`},
		"amdgpuoperator": {allCommon, `
skipNodeFeatureDiscoveryDependencyCheck | O | false | Skips the Node Feature Discovery dependency check.
selector | O | null | Selects Kubernetes nodes managed by AMD GPU Operator.
devicePlugin | O | null | Configures AMD GPU device plugin and node labeller.
metricsExporter | O | null | Configures AMD GPU metrics collection and exposure.
testRunner | O | null | Configures AMD GPU health validation and diagnostic testing on selected nodes.
configManager | O | null | Configures AMD GPU partitioning profiles through the Device Config Manager.
draDriver | O | null | Configures Kubernetes Dynamic Resource Allocation for AMD GPUs.
controllerManager | O | null | Configures the AMD GPU Operator controller deployment.`},
		"certificatemanager": {allCommon, `
cert-manager-cainjector.ContainerResources | O | null | CPU and memory requests and limits for cainjector containers.
cert-manager-controller.ContainerResources | O | null | CPU and memory requests and limits for controller containers.
cert-manager-webhook.ContainerResources | O | null | CPU and memory requests and limits for webhook containers.`},
		"clusterautoscaler": {allCommon, clusterAutoscalerCfgs},
		"csidriversmb": {"", `
controller.tolerations | O | null | Custom tolerations for the CSI Driver SMB controller.
controller.affinity | O | null | Node affinity rules for scheduling the controller pod.`},
		"oracledatabaseoperatorforkubernetes": {allCommon, `
manager.ContainerResources | O | null | Sets add-on container resource requests and limits.
skipAddonDependenciesCheck | O | null | Skips checking for other required add-ons.`},
		"gpuhealthmonitoring": {allCommon, ``},
		"istio": {allCommon, `
customizeConfigMap | R | false | Retain custom Istio configuration across Oracle updates.
discovery.ContainerResources | O | null | CPU and memory requests and limits for the istiod discovery container.
discovery.EnvVariables | O | null | Environment variables for the istiod discovery container.
enableIngressGateway | R | false | Enables the classic Istio ingress gateway.
istio-ingressgateway.Annotations | O |  | Annotations for the ingress gateway, such as load balancer settings.
istio-ingressgateway.HorizontalPodAutoscalerMinReplicas | O | null | Minimum ingress gateway autoscaler replicas.
istio-ingressgateway.HorizontalPodAutoscalerMaxReplicas | O | null | Maximum ingress gateway autoscaler replicas.
istio-ingressgateway.PodDisruptionBudgetMinAvailable | O | null | Minimum gateway pods available during disruption.
istiod.HorizontalPodAutoscalerMinReplicas | O | null | Minimum istiod controller replicas.
istiod.HorizontalPodAutoscalerMaxReplicas | O | null | Maximum istiod controller replicas.
istiod.PodDisruptionBudgetMinAvailable | O | null | Minimum istiod pods available during disruption.
profile | R | oke-default | Istio installation profile.`},
		"kubernetesdashboard": {allCommon, `
kubernetes-dashboard.ContainerResources | O | null | Resource requests and limits for dashboard containers.`},
		"kubernetesmetricsserver": {allCommon, `
metrics-server.ContainerResources | O | null | Sets Metrics Server container resource requests and limits.
skipAddonDependenciesCheck | O | null | Skips checking required add-on dependencies like cert-manager.`},
		"ocinativeingresscontroller": {allCommon, `
authSecretName | O |  | Secret for user authentication when authType is user.
authType | O | instance | Authentication type: instance, user, or workloadIdentity.
certDeletionGracePeriodInDays | O | 0 | Days before deleting unused Certificates resources.
compartmentId | R |  | Compartment OCID for load balancer and certificates.
controllerClass | O | oci.oraclecloud.com/native-ingress-controller | IngressClass controller name this add-on manages.
emitEvents | O | false | Emit Kubernetes events for Ingress errors.
leaseLockName | O | oci-native-ingress-controller | Lease name for leader election.
leaseLockNamespace | O | native-ingress-controller-system | Namespace of the leader election lease.
loadBalancerSubnetId | R |  | OCID of the load balancer subnet.
logVerbosity | O | 4 | Logging verbosity level number.
metricsBackend | O | prometheus | Name of the metrics backend.
metricsPort | O | 2223 | Metrics port number.
oci-native-ingress-controller.ContainerResources | O | null | CPU and memory requests and limits for containers.
useLbCompartmentForCertificates | O | false | Use the load balancer compartment for certificate resources.`},
		"nodefeaturediscovery": {"affinity,nodeSelectors,numOfReplicas,tolerations", nodeFeatureDiscoveryCfgs},
		"nvidiagpuoperator":    {"rollingUpdate,tolerations", nvidiaGpuOperatorCfgs},
		"nvidiagpuplugin": {allCommon, `
deviceIdStrategy | O | uuid | Strategy for passing device IDs to the runtime.
deviceListStrategy | O | envvar | Strategy for passing the device list; comma-separated multiple values.
driverRoot | O | / | Root path for the NVIDIA driver installation.
failOnInitError | O | true | Fail the plugin on initialization error; false blocks indefinitely.
migStrategy | O | none | Strategy for exposing MIG devices: none, single, mixed.
nvidia-gpu-device-plugin.ContainerResources | O | null | CPU and memory requests and limits for containers.
passDeviceSpecs | O | false | Pass device paths and permissions to containers.
useConfigFile | O | false | Configure the plugin from the nvidia-device-plugin-config ConfigMap.`},
		"nvidianetworkoperator": {"affinity,nodeSelectors,tolerations", nvidiaNetworkOperatorCfgs},
		"tiller": {allCommon, `
tiller.ContainerResources | O | null | Resource requests and limits for add-on containers.`},
		"weblogickubernetesoperator": {allCommon, `
weblogic-operator.ContainerResources | O | null | CPU and memory requests and limits for operator containers.
weblogic-operator-webhook.ContainerResources | O | null | CPU and memory limits for the webhook container.`},
	}
}

// addonConfigs returns the documented configuration arguments of a catalogue add-on.
func addonConfigs(name string) []addonCfg {
	set, ok := addonCfgSets()[addonKey(name)]
	if !ok {
		return nil
	}

	return append(commonCfgs(set.common), parseCfgs(set.specific)...)
}
