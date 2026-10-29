package app

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	enginesStub   = "stub"
	enginesAuto   = "auto"
	enginesDocker = "docker"

	engineProbeTimeout = 5 * time.Second
	envContainerRT     = "CONTAINER_RUNTIME"
	envECSRuntime      = "GOPHERSTACK_ECS_RUNTIME"
	envInfluxEngine    = "TIMESTREAM_INFLUXDB_ENGINE"
)

// EngineSettings holds the global engine switch and the ECS task runtime.
type EngineSettings struct {
	Engines    string `name:"engines"     env:"GOPHERSTACK_ENGINES"    default:"stub" enum:"stub,auto,docker" help:"Engine default: stub, auto (docker/podman if reachable, else stub), or docker. Per-service flags override."` //nolint:lll,golines // config struct tags are intentionally verbose
	ECSRuntime string `name:"ecs-runtime" env:"GOPHERSTACK_ECS_RUNTIME" default:""     help:"ECS task runtime: none or docker. Unset follows --engines."`                                                                        //nolint:lll,golines // config struct tags are intentionally verbose
}

type engineProbe func(ctx context.Context) (container.RuntimeName, error)

type engineDeps struct {
	probe  engineProbe
	getenv func(string) string
	setenv func(string, string)
	args   []string
}

func osEngineDeps(args []string) engineDeps {
	return engineDeps{
		probe:  defaultEngineProbe,
		getenv: os.Getenv,
		setenv: func(k, v string) { _ = os.Setenv(k, v) },
		args:   args,
	}
}

type engineEntry struct {
	field    func(*CLI) *string
	flag     string
	env      string
	fallback string
}

func engineEntries() []engineEntry {
	return []engineEntry{
		{func(c *CLI) *string { return &c.ElastiCacheEngine }, "elasticache-engine", "ELASTICACHE_ENGINE", "embedded"},
		{func(c *CLI) *string { return &c.MemoryDBEngine }, "memorydb-engine", "MEMORYDB_ENGINE", "embedded"},
		{func(c *CLI) *string { return &c.KafkaEngine }, "kafka-engine", "KAFKA_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.FlinkEngine }, "flink-engine", "FLINK_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.MWAAEngine }, "mwaa-engine", "MWAA_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.MQEngine }, "mq-engine", "MQ_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.EKSEngine }, "eks-engine", "EKS_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.GKEEngine }, "gke-engine", "GKE_ENGINE", enginesStub},
		{
			func(c *CLI) *string { return &c.OCIContainerEngine.Engine },
			"oci-oke-engine", "OKE_ENGINE", enginesStub,
		},
		{func(c *CLI) *string { return &c.OCIMySQL.Engine }, "oci-mysql-engine", "OCI_MYSQL_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.RDSEngine }, "rds-engine", "RDS_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.DocDBEngine }, "docdb-engine", "DOCDB_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.RedshiftEngine }, "redshift-engine", "REDSHIFT_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.NeptuneEngine }, "neptune-engine", "NEPTUNE_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.OpenSearchEngine }, "opensearch-engine", "OPENSEARCH_ENGINE", enginesStub},
		{
			func(c *CLI) *string { return &c.ElasticsearchEngine },
			"elasticsearch-engine", "ELASTICSEARCH_ENGINE", enginesStub,
		},
		{func(c *CLI) *string { return &c.SageMakerEngine }, "sagemaker-engine", "SAGEMAKER_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.CodeBuild.Engine }, "codebuild-engine", "CODEBUILD_ENGINE", enginesStub},
		{func(c *CLI) *string { return &c.Glue.Engine }, "glue-engine", "GLUE_ENGINE", enginesStub},
	}
}

func defaultEngineProbe(ctx context.Context) (container.RuntimeName, error) {
	ctx, cancel := context.WithTimeout(ctx, engineProbeTimeout)
	defer cancel()

	return container.Probe(ctx, container.Config{})
}

func flagGiven(args []string, flag string) bool {
	for _, a := range args {
		if a == "--"+flag || strings.HasPrefix(a, "--"+flag+"=") {
			return true
		}
	}

	return false
}

// resolveEngines computes the effective engine mode per service from the global switch.
// Explicit per-service flags or env vars always win. Results land in c.engineModes and
// the env vars read by services that have no config interface; persisted fields are untouched.
func (c *CLI) resolveEngines(ctx context.Context, log *slog.Logger, d engineDeps) {
	if v := c.EngineSwitch.ECSRuntime; v != "" {
		d.setenv(envECSRuntime, v)
	}

	mode := c.EngineSwitch.Engines
	if mode != enginesAuto && mode != enginesDocker {
		return
	}

	target := enginesDocker

	if mode == enginesAuto {
		rt, err := d.probe(ctx)
		if err != nil {
			log.WarnContext(ctx, "engines=auto: no container runtime reachable; using stub engines", "error", err)

			return
		}

		log.InfoContext(ctx, "engines=auto: container runtime reachable; using docker engines", "runtime", string(rt))

		if d.getenv(envContainerRT) == "" && rt != container.RuntimeDocker {
			d.setenv(envContainerRT, string(rt))
		}
	}

	c.engineModes = make(map[string]string, len(engineEntries()))

	for _, e := range engineEntries() {
		if d.getenv(e.env) != "" || flagGiven(d.args, e.flag) || *e.field(c) != e.fallback {
			continue
		}

		c.engineModes[e.flag] = target
	}

	for _, key := range []string{envInfluxEngine, envECSRuntime} {
		if d.getenv(key) == "" {
			d.setenv(key, target)
		}
	}
}

// engineMode returns the resolved mode for flag, or cur when no override applies.
func (c *CLI) engineMode(flag, cur string) string {
	if v, ok := c.engineModes[flag]; ok {
		return v
	}

	return cur
}

func (c *CLI) applyRuntimeConfig(ctx context.Context, log *slog.Logger) {
	c.GetGlobalConfig().Update(
		c.AccountID, c.Region, c.LatencyMs,
		c.JanitorTimeout, c.EnforceIAM, c.AutoPurgeTTL,
	)
	c.resolveEngines(ctx, log, osEngineDeps(os.Args[1:]))
}
