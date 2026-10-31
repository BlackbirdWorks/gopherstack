package app

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
)

const (
	envDNSAddr         = "DNS_ADDR"
	envDNSUpstream     = "TRANSPARENT_DNS_UPSTREAM"
	envPersistMode     = "GOPHERSTACK_PERSIST_MODE"
	envPersistFlush    = "GOPHERSTACK_PERSIST_FLUSH_INTERVAL"
	defaultDNSPort     = "53"
	lsStrategyRequest  = "ON_REQUEST"
	lsStrategySched    = "SCHEDULED"
	lsStrategyStartup  = "ON_STARTUP"
	lsStrategyShutdown = "ON_SHUTDOWN"
	lsStrategyManual   = "MANUAL"
	envPersistLoad     = "GOPHERSTACK_PERSIST_LOAD"
	faultStatusBadReq  = 400
	faultCodeThrottled = "ProvisionedThroughputExceededException"
)

// localstackUnsupportedKeys are documented LocalStack options with a behaviour
// Gopherstack does not provide; they are warned about once at startup.
func localstackUnsupportedKeys() []string {
	return slices.Concat(localstackUnsupportedCore(), localstackUnsupportedRuntime())
}

func localstackUnsupportedCore() []string {
	return []string{
		"USE_SSL",
		"LOCALSTACK_HOST",
		"PARITY_AWS_ACCESS_KEY_ID",
		"APIGW_WEBSOCKETS_LEGACY_PORT",
		"APPSYNC_JS_LIBS_VERSION",
		"BEDROCK_PREWARM",
		"DEFAULT_BEDROCK_MODEL",
		"BEDROCK_PULL_MODELS",
		"CFN_PER_RESOURCE_TIMEOUT",
		"CFN_VERBOSE_ERRORS",
		"CFN_STRING_REPLACEMENT_DENY_LIST",
		"CLOUDFRONT_LAMBDA_EDGE",
		"CODEBUILD_REMOVE_CONTAINERS",
		"CODEBUILD_ENABLE_CUSTOM_IMAGES",
		"CODEPIPELINE_GH_TOKEN",
		"DMS_SERVERLESS_DEPROVISIONING_DELAY",
		"DMS_SERVERLESS_STATUS_CHANGE_WAITING_TIME",
		"DOCDB_PROXY_CONTAINER",
		"DYNAMODB_SHARE_DB",
		"DYNAMODB_DELAY_TRANSIENT_STATUSES",
		"DYNAMODB_CORS",
		"DYNAMODB_REMOVE_EXPIRED_ITEMS",
		"ECS_REMOVE_CONTAINERS",
		"ECS_DISABLE_AWS_ENDPOINT_URL",
		"ECS_TASK_EXECUTOR",
		"EC2_DOCKER_INIT",
		"EC2_DOWNLOAD_DEFAULT_IMAGES",
		"EC2_EBS_MAX_VOLUME_SIZE",
		"EC2_MOUNT_BLOCK_DEVICES",
		"EC2_REMOVE_CONTAINERS",
		"EC2_VM_MANAGER",
		"EKS_LOADBALANCER_PORT",
		"MANAGED_K8S_PROVIDER",
		"EKS_PERSIST_CLUSTER_CONTENTS",
		"REDIS_CONTAINER_MODE",
		"GLUE_JOB_EXECUTOR",
		"DOCKER_GLOBAL_IMAGE_PREFIX",
	}
}

func localstackUnsupportedRuntime() []string {
	return []string{
		"BUCKET_MARKER_LOCAL",
		"LAMBDA_DISABLE_AWS_ENDPOINT_URL",
		"LAMBDA_DISABLE_JAVA_SDK_V2_CERTIFICATE_VALIDATION",
		"LAMBDA_DOCKER_DNS",
		"LAMBDA_DOCKER_NETWORK",
		"LAMBDA_DOWNLOAD_AWS_LAYERS",
		"LAMBDA_EVENT_SOURCE_MAPPING_SQS_POLLER_COUNT",
		"LAMBDA_IGNORE_ARCHITECTURE",
		"LAMBDA_PREBUILD_IMAGES",
		"LAMBDA_RUNTIME_ENVIRONMENT_TIMEOUT",
		"LAMBDA_RUNTIME_EXECUTOR",
		"LAMBDA_RUNTIME_IMAGE_MAPPING",
		"LAMBDA_SYNCHRONOUS_CREATE",
		"LAMBDA_TRUNCATE_STDOUT",
		"LAMBDA_WEB_DISABLE_AWS_ENDPOINT_URL",
		"LAMBDA_WEB_IGNORE_ARCHITECTURE",
		"LAMBDA_WEB_RUNTIME_IMAGE_MAPPING",
		"MWAA_PIP_TRUSTED_HOSTS",
		"MWAA_S3_POLL_INTERVAL",
		"NEPTUNE_DB_TYPE",
		"NEPTUNE_ENABLE_TRANSACTION",
		"NEPTUNE_GREMLIN_DEBUG",
		"NEPTUNE_USE_SSL",
		"TF_COMPAT_MODE",
		"RDS_CLUSTER_ENDPOINT_HOST_ONLY",
		"RDS_PG_CUSTOM_VERSIONS",
		"RDS_MYSQL_DOCKER",
		"MYSQL_IMAGE",
		"MSSQL_IMAGE",
		"MSSQL_ACCEPT_EULA",
		"RDS_PG_MAX_CONNECTIONS",
		"S3FILES_EXPORT_DELAY_SECONDS",
		"SNS_SES_SENDER_ADDRESS",
		"SQS_CLOUDWATCH_METRICS_REPORT_INTERVAL",
		"DISABLE_CORS_HEADERS",
		"ENABLE_CONFIG_UPDATES",
		"SMTP_EMAIL",
		"DISABLE_COMPATIBILITY_RULES",
		"POD_LOAD_CLI_TIMEOUT",
		"POD_ENCRYPTION",
		"ENABLE_POD_RESOURCES",
		"EXTENSION_AUTO_INSTALL",
		"CUSTOM_SSL_CERT_PATH",
		"SSL_NO_VERIFY",
		"DNS_LOCAL_NAME_PATTERNS",
		"LOG_LICENSE_ISSUES",
		"ECR_ENDPOINT_STRATEGY",
	}
}

// localstackBenignKeys describe LocalStack infrastructure (JVM, Docker, Kubernetes,
// legacy providers) with no Gopherstack counterpart; they are only logged at debug.
func localstackBenignKeys() []string {
	return []string{
		"EAGER_SERVICE_LOADING", "HOSTNAME_EXTERNAL", "DISABLE_EVENTS", "SKIP_SSL_CERT_DOWNLOAD",
		"ALLOW_NONSTANDARD_REGIONS", "DOCKER_FLAGS", "IMAGE_NAME", "MAIN_CONTAINER_NAME",
		"LOCALSTACK_VOLUME_DIR", "DOCKER_SOCK", "DOCKER_BRIDGE_IP", "LEGACY_DOCKER_CLIENT", "DOCKER_CMD",
		"FORCE_NONINTERACTIVE", "DOCKER_HOST", "OVERRIDE_IN_DOCKER", "K8S_IMAGE_PULL_POLICY",
		"DYNAMODB_HEAP_SIZE", "DYNAMODB_IN_MEMORY", "DYNAMODB_LOCAL_PORT",
		"DYNAMODB_OPTIMIZE_DB_BEFORE_STARTUP", "DYNAMODB_SKIP_LEGACY_MIGRATION",
		"KINESIS_MOCK_PROVIDER_ENGINE", "KINESIS_MOCK_MAXIMUM_HEAP_SIZE", "KINESIS_MOCK_INITIAL_HEAP_SIZE",
		"IGNORE_ES_DOWNLOAD_ERRORS", "IGNORE_OS_DOWNLOAD_ERRORS", "SKIP_INFRA_DOWNLOADS",
		"K3S_FLAGS", "K3S_IMAGE_TAG", "K3S_IMAGE_REPOSITORY", "K3D_START_LB_INGRESS", "K3D_CLUSTER_TOKEN",
		"K3D_VERSION", "DEVELOP", "DEVELOP_PORT", "WAIT_FOR_DEBUGGER",
		"LOCALSTACK_AUTH_TOKEN", "LOCALSTACK_API_KEY", "ACTIVATE_PRO",
		"LEGACY_EDGE_PROXY", "USE_LIGHT_IMAGE", "MULTI_ACCOUNTS",
		"PORT_WEB_UI", "EDGE_PORT", "EDGE_PORT_HTTP", "EDGE_BIND_HOST", "LOCALSTACK_HOSTNAME",
		"SQS_PROVIDER", "KINESIS_PROVIDER", "KMS_PROVIDER", "LAMBDA_EXECUTOR", "PERSIST_ALL",
	}
}

// localstackUnsupportedPrefixes/Suffixes cover families of container-flag options.
func localstackBenignAffixes() ([]string, []string) {
	return []string{"PROVIDER_OVERRIDE_", "LOCALSTACK_K8S_", "LAMBDA_K8S_", "EKS_K3"},
		[]string{"_DOCKER_FLAGS"}
}

// planOptions translates LocalStack options that need more than a rename.
func (p *envPlan) planOptions() {
	p.planDNS()
	p.planPersistence()
	p.planPortRange()

	p.planSQS()
	p.planLimits()
	p.planS3Signatures()

	if ms, err := strconv.Atoi(p.val("LAMBDA_KEEPALIVE_MS")); err == nil && ms >= 0 {
		p.put("LAMBDA_IDLE_TIMEOUT", (time.Duration(ms) * time.Millisecond).String())
	}

	if h := p.val("HOSTNAME_FROM_LAMBDA"); h != "" {
		p.put("LAMBDA_DOCKER_HOST", h)
	}

	if v := p.val("OUTBOUND_HTTP_PROXY"); v != "" {
		p.put("HTTP_PROXY", v)
	}

	if v := p.val("OUTBOUND_HTTPS_PROXY"); v != "" {
		p.put("HTTPS_PROXY", v)
	}

	if v := p.val("REQUESTS_CA_BUNDLE"); v != "" {
		p.put("SSL_CERT_FILE", v)
	}
}

// planSQS inverts LocalStack's positive SQS toggles onto the negative native switches.
func (p *envPlan) planSQS() {
	for _, m := range []struct{ ls, native string }{
		{"SQS_DELAY_PURGE_RETRY", "GOPHERSTACK_SQS_DISABLE_PURGE_COOLDOWN"},
		{"SQS_DELAY_RECENTLY_DELETED", "GOPHERSTACK_SQS_DISABLE_RECENTLY_DELETED_COOLDOWN"},
		{"SQS_ENABLE_MESSAGE_RETENTION_PERIOD", "GOPHERSTACK_SQS_DISABLE_MESSAGE_RETENTION"},
	} {
		if b, err := strconv.ParseBool(p.val(m.ls)); err == nil && !b {
			p.put(m.native, "true")
		}
	}
}

// planS3Signatures inverts LocalStack's skip toggle (default skip) onto the native opt-in.
func (p *envPlan) planS3Signatures() {
	if b, err := strconv.ParseBool(p.val("S3_SKIP_SIGNATURE_VALIDATION")); err == nil && !b {
		p.put("GOPHERSTACK_S3_VALIDATE_SIGNATURES", "true")
	}
}

func (p *envPlan) planDNS() {
	switch a := p.val("DNS_ADDRESS"); a {
	case "", "0", "0.0.0.0":
		// LocalStack uses 0 to disable the DNS server; 0.0.0.0 is its port-53 default.
	default:
		p.put(envDNSAddr, withDefaultPort(a, defaultDNSPort))
	}

	if v := p.val("DNS_NAME_PATTERNS_TO_RESOLVE_UPSTREAM"); v != "" {
		p.put("TRANSPARENT_DNS_SKIP_PATTERNS", v)
	}

	if b, err := strconv.ParseBool(p.val("DISABLE_TRANSPARENT_ENDPOINT_INJECTION")); err == nil && b {
		p.put("TRANSPARENT_DISABLE_ENDPOINT_INJECTION", "true")
	}

	if s := p.val("DNS_SERVER"); s != "" {
		p.put(envDNSUpstream, withDefaultPort(s, defaultDNSPort))
	}
}

func withDefaultPort(addr, port string) string {
	if strings.HasPrefix(addr, ":") || (strings.Contains(addr, ":") && !strings.Contains(addr, "::")) {
		return addr
	}

	return strings.TrimSuffix(addr, ":") + ":" + port
}

func (p *envPlan) planPersistence() {
	switch strings.ToUpper(p.val("SNAPSHOT_SAVE_STRATEGY")) {
	case "":
	case lsStrategyRequest:
		p.put(envPersistMode, "snapshot")
	case lsStrategySched:
		p.put(envPersistMode, "hybrid")
	case lsStrategyShutdown:
		p.put(envPersistMode, "shutdown")
	case lsStrategyManual:
		p.put(envPersistMode, "manual")
	default:
		p.unsupported = append(p.unsupported, "SNAPSHOT_SAVE_STRATEGY")
	}

	switch strings.ToUpper(p.val("SNAPSHOT_LOAD_STRATEGY")) {
	case "", lsStrategyStartup:
	case lsStrategyRequest:
		p.put(envPersistLoad, "request")
	case lsStrategyManual:
		p.put(envPersistLoad, "manual")
	default:
		p.unsupported = append(p.unsupported, "SNAPSHOT_LOAD_STRATEGY")
	}

	if secs, err := strconv.Atoi(p.val("SNAPSHOT_FLUSH_INTERVAL")); err == nil && secs > 0 {
		p.put(envPersistFlush, (time.Duration(secs) * time.Second).String())
	}
}

func (p *envPlan) planPortRange() {
	if v, err := strconv.Atoi(p.val("EXTERNAL_SERVICE_PORTS_START")); err == nil && v > 0 {
		p.put("PORT_RANGE_START", strconv.Itoa(v))
	}

	// LocalStack's end is inclusive; Gopherstack's is exclusive.
	if v, err := strconv.Atoi(p.val("EXTERNAL_SERVICE_PORTS_END")); err == nil && v > 0 {
		p.put("PORT_RANGE_END", strconv.Itoa(v+1))
	}
}

// localstackFaultRules turns KINESIS_ERROR_PROBABILITY and DYNAMODB_ERROR_PROBABILITY
// into probabilistic throughput-exceeded fault rules; invalid or zero values yield none.
func localstackFaultRules(getenv func(string) string) []chaos.FaultRule {
	var rules []chaos.FaultRule

	for _, o := range []struct{ env, svc string }{
		{"KINESIS_ERROR_PROBABILITY", "kinesis"},
		{"DYNAMODB_ERROR_PROBABILITY", "dynamodb"},
	} {
		prob, err := strconv.ParseFloat(strings.TrimSpace(getenv(o.env)), 64)
		if err != nil || prob <= 0 || prob > 1 {
			continue
		}

		rules = append(rules, chaos.FaultRule{
			Service:     o.svc,
			Probability: &prob,
			Error:       &chaos.FaultError{Code: faultCodeThrottled, StatusCode: faultStatusBadReq},
		})
	}

	return rules
}

// planLimits maps LocalStack's Lambda and Kinesis quota options onto the native settings.
func (p *envPlan) planLimits() {
	for ls, native := range map[string]string{
		"LAMBDA_LIMITS_CONCURRENT_EXECUTIONS":          "LAMBDA_ACCOUNT_CONCURRENT_EXECUTIONS",
		"LAMBDA_LIMITS_MINIMUM_UNRESERVED_CONCURRENCY": "LAMBDA_ACCOUNT_MIN_UNRESERVED_CONCURRENCY",
		"LAMBDA_LIMITS_TOTAL_CODE_SIZE":                "LAMBDA_ACCOUNT_TOTAL_CODE_SIZE",
		"LAMBDA_LIMITS_CODE_SIZE_ZIPPED":               "LAMBDA_CODE_SIZE_ZIPPED",
		"LAMBDA_LIMITS_CODE_SIZE_UNZIPPED":             "LAMBDA_CODE_SIZE_UNZIPPED",
		"LAMBDA_LIMITS_CREATE_FUNCTION_REQUEST_SIZE":   "LAMBDA_CREATE_FUNCTION_REQUEST_SIZE",
		"LAMBDA_LIMITS_MAX_FUNCTION_ENVVAR_SIZE_BYTES": "LAMBDA_MAX_ENVIRONMENT_BYTES",
	} {
		if n, err := strconv.ParseInt(p.val(ls), 10, 64); err == nil && n > 0 {
			p.put(native, strconv.FormatInt(n, 10))
		}
	}

	if p.truthy("LAMBDA_RUNTIME_VALIDATION") {
		p.put("LAMBDA_STRICT_RUNTIME_VALIDATION", "true")
	}

	for ls, native := range map[string]string{
		"KINESIS_SHARD_LIMIT":                  "KINESIS_ACCOUNT_SHARD_LIMIT",
		"KINESIS_ON_DEMAND_STREAM_COUNT_LIMIT": "KINESIS_ACCOUNT_ON_DEMAND_STREAM_LIMIT",
	} {
		v := p.val(ls)
		if strings.EqualFold(v, "infinity") || strings.EqualFold(v, "unlimited") {
			p.put(native, "-1")
		} else if n, err := strconv.Atoi(v); err == nil && n > 0 {
			p.put(native, strconv.Itoa(n))
		}
	}

	if ms, err := strconv.Atoi(p.val("KINESIS_LATENCY")); err == nil && ms >= 0 {
		p.put("KINESIS_RESPONSE_LATENCY", (time.Duration(ms) * time.Millisecond).String())
	}
}
