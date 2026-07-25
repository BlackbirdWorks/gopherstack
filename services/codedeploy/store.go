// Package codedeploy provides an in-memory implementation of the AWS CodeDeploy service.
package codedeploy

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	statusSucceeded       = "Succeeded"
	statusStopped         = "Stopped"
	statusFailed          = "Failed"
	computePlatformServer = "Server"
	computePlatformLambda = "Lambda"
	computePlatformECS    = "ECS"
)

// Deployment target/instance type discriminators shared between the backend
// computation (deployment_instances.go) and its wire conversion
// (handler_deployment_instances.go).
const (
	targetTypeInstance = "instanceTarget"
	targetTypeECS      = "ecsTarget"
	targetTypeLambda   = "lambdaTarget"
)

// InMemoryBackend is the in-memory store for CodeDeploy resources.
//
// deployments, deploymentConfigs, and applicationRevisions carry a real,
// wire-visible identity field and no live *tags.Tags field, so each
// registers directly on b.registry as a "clean" *store.Table.
// applicationRevisions is keyed by a composite appName+canonical-revision-JSON
// string (see applicationRevisionKey in store_setup.go), with
// applicationRevisionsByApp replacing a per-application scan for
// ListApplicationRevisions.
//
// applications, deploymentGroups, and onPremisesInstances each carry a live
// *tags.Tags field marked json:"-", so each is a "dirty" table (store.New
// only, NOT store.Register-ed onto b.registry -- see store_setup.go)
// round-tripped through a DTO wrapper in persistence.go. deploymentGroups
// was previously nested by application; it flattens to one *store.Table
// keyed by the composite "appName/dgName" string (see dgKey in
// store_setup.go), with deploymentGroupsByApp replacing the old
// map[string]map[string]*DeploymentGroup nesting for per-application scans.
//
// githubTokens is deliberately NOT converted: it is an identity-less set
// (map[string]struct{}), so there is no *T value for store.Table to key on.
// It remains a plain map, unchanged by this refactor.
type InMemoryBackend struct {
	registry                  *store.Registry
	applications              *store.Table[Application]
	deploymentGroups          *store.Table[DeploymentGroup]
	deploymentGroupsByApp     *store.Index[DeploymentGroup]
	deployments               *store.Table[Deployment]
	onPremisesInstances       *store.Table[OnPremisesInstance]
	deploymentConfigs         *store.Table[DeploymentConfig]
	applicationRevisions      *store.Table[ApplicationRevision]
	applicationRevisionsByApp *store.Index[ApplicationRevision]
	githubTokens              map[string]struct{}
	mu                        *lockmetrics.RWMutex
	accountID                 string
	region                    string
}

// NewInMemoryBackend creates a new in-memory CodeDeploy backend with pre-seeded default configs.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:     store.NewRegistry(),
		githubTokens: make(map[string]struct{}),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("codedeploy"),
	}

	registerAllTables(b)
	b.seedDefaultConfigs()

	return b
}

// Reset clears all state, returning the backend to a fresh empty state (with default configs re-seeded).
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, app := range b.applications.All() {
		if app.Tags != nil {
			app.Tags.Close()
		}
	}

	for _, dg := range b.deploymentGroups.All() {
		if dg.Tags != nil {
			dg.Tags.Close()
		}
	}

	for _, inst := range b.onPremisesInstances.All() {
		if inst.Tags != nil {
			inst.Tags.Close()
		}
	}

	b.registry.ResetAll()
	b.applications.Reset()
	b.deploymentGroups.Reset()
	b.onPremisesInstances.Reset()
	b.githubTokens = make(map[string]struct{})

	b.seedDefaultConfigs()
}

// ensureTags returns the given tags if non-nil, or creates a new tags.Tags with the given key.
func ensureTags(existing *tags.Tags, key string) *tags.Tags {
	if existing != nil {
		return existing
	}

	return tags.New(key)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// validateComputePlatform returns an error if the given platform is not a valid CodeDeploy compute platform.
func validateComputePlatform(platform string) error {
	if _, ok := validComputePlatforms()[platform]; !ok {
		return fmt.Errorf("%w: invalid computePlatform %q, must be Server, Lambda, or ECS",
			ErrInvalidComputePlatform, platform)
	}

	return nil
}

// validComputePlatforms lists the accepted CodeDeploy compute platforms.
func validComputePlatforms() map[string]struct{} {
	return map[string]struct{}{
		computePlatformServer: {},
		computePlatformLambda: {},
		computePlatformECS:    {},
	}
}
