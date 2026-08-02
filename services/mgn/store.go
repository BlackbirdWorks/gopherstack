package mgn

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

// InMemoryBackend is the in-memory store for AWS Application Migration
// Service (MGN).
//
// A single coarse lock guards every collection below: operations routinely
// cross resource boundaries (AssociateSourceServers reads+writes an
// Application and every named SourceServer; StartTest/StartCutover create a
// Job while reading N SourceServers; TagResource resolves an ARN into
// whichever of 12 taggable resource kinds it names), so the invariant
// boundary is the whole backend -- see .claude/memories/pkgs-catalog.md's
// locking rule.
type InMemoryBackend struct {
	sourceServers               *store.Table[SourceServer]
	launchConfigs               *store.Table[LaunchConfiguration]
	replicationConfigs          *store.Table[ReplicationConfiguration]
	launchTemplates             *store.Table[LaunchConfigurationTemplate]
	replicationTemplates        *store.Table[ReplicationConfigurationTemplate]
	jobs                        *store.Table[Job]
	jobLogs                     *store.Table[JobLog]
	jobLogsByJob                *store.Index[JobLog]
	applications                *store.Table[Application]
	waves                       *store.Table[Wave]
	connectors                  *store.Table[Connector]
	vcenterClients              *store.Table[VcenterClient]
	exportTasks                 *store.Table[ExportTask]
	importTasks                 *store.Table[ImportTask]
	importFileEnrichments       *store.Table[ImportFileEnrichment]
	sourceServerActions         *store.Table[SourceServerActionDocument]
	sourceServerActionsByServer *store.Index[SourceServerActionDocument]
	templateActions             *store.Table[TemplateActionDocument]
	templateActionsByTemplate   *store.Index[TemplateActionDocument]
	nmDefinitions               *store.Table[NetworkMigrationDefinition]
	nmExecutions                *store.Table[NetworkMigrationExecution]
	nmExecutionsByDef           *store.Index[NetworkMigrationExecution]
	nmJobs                      *store.Table[NetworkMigrationJob]
	nmJobsByExecution           *store.Index[NetworkMigrationJob]
	registry                    *store.Registry

	mu                 *lockmetrics.RWMutex
	work               *worker.Group
	accountID          string
	region             string
	serviceInitialized bool
}

// NewInMemoryBackend creates a new in-memory AWS Application Migration
// Service backend.
func NewInMemoryBackend(ctx context.Context, accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("mgn"),
		work:      worker.NewGroup(ctx, "mgn"),
	}
	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Close stops all scheduled state-transition timers so none outlives the
// backend. Safe to call multiple times.
func (b *InMemoryBackend) Close() { b.work.Stop() }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	closeAllTags(b)
	b.registry.ResetAll()
	b.serviceInitialized = false
}

// closeAllTags releases every tags.Tags instance's Prometheus registration
// before the underlying tables are cleared. Callers must hold b.mu.
func closeAllTags(b *InMemoryBackend) {
	for _, s := range b.sourceServers.All() {
		closeTags(s.Tags)
	}

	for _, t := range b.launchTemplates.All() {
		closeTags(t.Tags)
	}

	for _, t := range b.replicationTemplates.All() {
		closeTags(t.Tags)
	}

	for _, j := range b.jobs.All() {
		closeTags(j.Tags)
	}

	for _, a := range b.applications.All() {
		closeTags(a.Tags)
	}

	for _, w := range b.waves.All() {
		closeTags(w.Tags)
	}

	for _, c := range b.connectors.All() {
		closeTags(c.Tags)
	}

	for _, v := range b.vcenterClients.All() {
		closeTags(v.Tags)
	}

	for _, e := range b.exportTasks.All() {
		closeTags(e.Tags)
	}

	for _, i := range b.importTasks.All() {
		closeTags(i.Tags)
	}

	for _, d := range b.nmDefinitions.All() {
		closeTags(d.Tags)
	}

	for _, e := range b.nmExecutions.All() {
		closeTags(e.Tags)
	}
}

func closeTags(t interface{ Close() }) {
	if t != nil {
		t.Close()
	}
}

// ---- ARN builders ----
//
// UNCONFIRMED resource-path segments: Terraform's AWS provider has ZERO MGN
// resources to corroborate against (PARITY.md's "gaps" section -- confirmed
// via GitHub API directory listing, internal/service/mgn/ contains only
// generated client boilerplate), and AWS's own Service Authorization
// Reference page returned only a JS shell to automated fetching. Only the
// ARN service segment ("mgn") has indirect corroboration, via botocore's
// service-2.json endpointPrefix/serviceId/signingName all being literally
// "mgn". Every resource-path segment below is this package's best-effort
// guess from AWS naming convention (kebab-case singular resource kind,
// matching resiliencehub/outposts' own convention), NOT a confirmed value.

func (b *InMemoryBackend) sourceServerARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "source-server/"+id)
}

func (b *InMemoryBackend) jobARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "job/"+id)
}

func (b *InMemoryBackend) applicationARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "application/"+id)
}

func (b *InMemoryBackend) waveARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "wave/"+id)
}

func (b *InMemoryBackend) connectorARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "connector/"+id)
}

func (b *InMemoryBackend) vcenterClientARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "vcenter-client/"+id)
}

func (b *InMemoryBackend) launchTemplateARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "launch-configuration-template/"+id)
}

func (b *InMemoryBackend) replicationTemplateARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "replication-configuration-template/"+id)
}

func (b *InMemoryBackend) exportTaskARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "export-task/"+id)
}

func (b *InMemoryBackend) importTaskARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "import-task/"+id)
}

func (b *InMemoryBackend) nmDefinitionARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "network-migration-definition/"+id)
}

// nmExecutionARN builds the (undocumented anywhere in the SDK -- Network
// Migration executions carry no Arn field on their own wire type at all,
// confirmed by direct read of types.NetworkMigrationExecution) synthetic
// ARN this backend uses internally to key the tagging trio's ARN-based tag
// store for executions, since NetworkMigrationExecution.Tags does exist on
// the wire (family L's doc comment) despite no Arn field to address it by.
func (b *InMemoryBackend) nmExecutionARN(id string) string {
	return arn.Build("mgn", b.region, b.accountID, "network-migration-execution/"+id)
}

// resourceIDFromARN extracts the resource ID from an MGN ARN of the form
// ".../{marker}{id}", returning ok=false if arnStr does not contain marker.
func resourceIDFromARN(arnStr, marker string) (string, bool) {
	idx := strings.LastIndex(arnStr, marker)
	if idx < 0 {
		return "", false
	}

	id := arnStr[idx+len(marker):]
	if id == "" {
		return "", false
	}

	return id, true
}

// ---- ID generation ----
//
// No ID format is confirmed anywhere in this SDK module (no doc-comment
// examples the way directconnect's "dxcon-ffabc123" ones were) -- every
// generator below is a defensible, UNCONFIRMED hex-suffix convention
// matching services/resiliencehub's identical randomHexID() treatment.

const randomIDBytes = 8

func randomHexID() string {
	buf := make([]byte, randomIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}

	return hex.EncodeToString(buf)
}

func newSourceServerID() string        { return "s-" + randomHexID() }
func newJobID() string                 { return "mgnjob-" + randomHexID() }
func newApplicationID() string         { return "app-" + randomHexID() }
func newWaveID() string                { return "wave-" + randomHexID() }
func newConnectorID() string           { return "connector-" + randomHexID() }
func newVcenterClientID() string       { return "vcenter-" + randomHexID() }
func newLaunchTemplateID() string      { return "lt-" + randomHexID() }
func newReplicationTemplateID() string { return "rt-" + randomHexID() }
func newExportID() string              { return "export-" + randomHexID() }
func newImportID() string              { return "import-" + randomHexID() }
func newNMDefinitionID() string        { return "nmd-" + randomHexID() }
func newNMJobID() string               { return "nmjob-" + randomHexID() }

// nowRFC3339 formats the current UTC time as the RFC3339 string convention
// this package uses for every SDK member typed as *string but semantically
// a timestamp (see models.go's doc comment).
func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

// nowUTC returns the current time in UTC -- a tiny named helper so call
// sites read as intent ("record this timestamp now") rather than a bare
// time.Now().UTC() repeated at every call site, matching
// services/directconnect's identical helper.
func nowUTC() time.Time { return time.Now().UTC() }
