package gcp_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const googleProviderVersion = "8.5.0"

func providerBlock() string {
	return fmt.Sprintf(`terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = %q
    }
  }
}

provider "google" {
  project      = "gopherstack-test"
  region       = "us-central1"
  access_token = "fake"

  storage_custom_endpoint         = %q
  big_query_custom_endpoint       = %q
  pubsub_custom_endpoint          = %q
  secret_manager_custom_endpoint  = %q
  kms_custom_endpoint             = %q
  cloud_tasks_custom_endpoint     = %q
  cloud_scheduler_custom_endpoint = %q
  sql_custom_endpoint             = %q
  logging_custom_endpoint         = %q
  redis_custom_endpoint           = %q
  monitoring_custom_endpoint      = %q
  spanner_custom_endpoint         = %q

  artifact_registry_custom_endpoint = %q
  cloud_build_custom_endpoint       = %q
  compute_custom_endpoint           = %q

  resource_manager_custom_endpoint          = %q
  resource_manager_v3_custom_endpoint       = %q
  cloud_resource_manager_custom_endpoint    = %q
  service_usage_custom_endpoint             = %q
  billing_custom_endpoint                   = %q
  cloud_billing_custom_endpoint             = %q
}
`, googleProviderVersion, storageEndpoint, bigQueryEndpoint, pubSubEndpoint, secretsEndpoint, kmsEndpoint,
		tasksEndpoint, schedulerEndpoint, sqlEndpoint, loggingEndpoint, redisEndpoint, monitoringEndpoint,
		spannerEndpoint, artifactEndpoint, cloudBuildEndpoint, computeEndpoint,
		rmV1Endpoint, rmV3Endpoint, rmV1Endpoint, rmV1Endpoint, rmV1Endpoint, rmV1Endpoint)
}

type tofuRunner struct {
	t   *testing.T
	dir string
	env []string
}

func newRunner(t *testing.T, hcl string) *tofuRunner {
	t.Helper()

	if sharedContainer == nil {
		t.Skip("gcp terraform suite was skipped in TestMain")
	}

	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(providerBlock()+hcl), 0o600); err != nil {
		t.Fatalf("writing main.tf: %v", err)
	}

	if err := os.MkdirAll(providerCacheDir, 0o750); err != nil {
		t.Logf("could not create provider cache dir: %v", err)
	}

	env := append(
		os.Environ(),
		"TF_IN_AUTOMATION=1",
		"TF_PLUGIN_CACHE_DIR="+providerCacheDir,
		"TF_PLUGIN_CACHE_MAY_BREAK_DEPENDENCY_LOCK_FILE=true",
	)

	return &tofuRunner{t: t, dir: dir, env: env}
}

func (r *tofuRunner) run(args ...string) (int, error) {
	r.t.Helper()

	cmd := exec.Command(tofuBinaryPath, args...)
	cmd.Dir = r.dir
	cmd.Env = r.env

	out, err := cmd.CombinedOutput()
	r.t.Logf("tofu %v:\n%s", args, out)

	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		return exitErr.ExitCode(), err
	}

	return 0, err
}

// output returns a raw root-module output value.
func (r *tofuRunner) output(name string) string {
	r.t.Helper()

	cmd := exec.Command(tofuBinaryPath, "output", "-raw", name)
	cmd.Dir = r.dir
	cmd.Env = r.env

	out, err := cmd.Output()
	if err != nil {
		r.t.Fatalf("tofu output %s: %v", name, err)
	}

	return string(out)
}

// lifecycle runs init, apply, a no-drift re-plan and (via cleanup) destroy.
func (r *tofuRunner) lifecycle() {
	r.t.Helper()

	r.lifecycleThen(nil)
}

// lifecycleThen is lifecycle with inspect run against the applied, drift-free state before destroy.
func (r *tofuRunner) lifecycleThen(inspect func()) {
	r.t.Helper()

	if _, err := r.run("init", "-no-color"); err != nil {
		r.t.Skip("tofu init failed (no registry access for hashicorp/google?)")
	}

	r.t.Cleanup(func() {
		if _, err := r.run("destroy", "-auto-approve", "-no-color"); err != nil {
			r.t.Errorf("tofu destroy failed: %v", err)
		}
	})

	if _, err := r.run("apply", "-auto-approve", "-no-color"); err != nil {
		r.t.Fatalf("tofu apply failed: %v", err)
	}

	code, err := r.run("plan", "-detailed-exitcode", "-no-color")
	if code == 2 {
		r.t.Fatalf("plan after apply is not empty (drift)")
	}

	if err != nil {
		r.t.Fatalf("tofu plan failed: %v", err)
	}

	if inspect != nil {
		inspect()
	}
}
