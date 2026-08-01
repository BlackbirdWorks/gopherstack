package resiliencehub

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

// asyncTransitionDelay is the simulated delay before an async resource
// (assessment, resolution, import, metrics export, grouping task) reaches
// its terminal state, mirroring services/outposts'
// orderTransitionDelay/capacityTaskTransitionDelay and services/grafana's
// workspaceTransitionDelay.
const asyncTransitionDelay = 100 * time.Millisecond

// InMemoryBackend is the in-memory store for AWS Resilience Hub.
//
// A single coarse lock guards every collection below: operations routinely
// cross resource boundaries (StartAppAssessment reads an App+its bound
// ResiliencyPolicy and writes an AppAssessment; PublishAppVersion reads and
// deep-copies a draft AppVersion embedded on App; TagResource resolves an ARN
// into whichever of App/ResiliencyPolicy/AppAssessment it names), so the
// invariant boundary is the whole backend -- see
// .claude/memories/pkgs-catalog.md's locking rule.
type InMemoryBackend struct {
	apps              *store.Table[App]
	policies          *store.Table[ResiliencyPolicy]
	assessments       *store.Table[AppAssessment]
	assessmentsByApp  *store.Index[AppAssessment]
	templates         *store.Table[RecommendationTemplate]
	templatesByAssess *store.Index[RecommendationTemplate]
	metricsExports    *store.Table[MetricsExport]
	groupingTasks     *store.Table[GroupingTask]
	groupingByApp     *store.Index[GroupingTask]
	registry          *store.Registry

	mu        *lockmetrics.RWMutex
	work      *worker.Group
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory AWS Resilience Hub backend.
func NewInMemoryBackend(ctx context.Context, accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("resiliencehub"),
		work:      worker.NewGroup(ctx, "resiliencehub"),
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

	for _, a := range b.apps.All() {
		if a.Tags != nil {
			a.Tags.Close()
		}
	}

	for _, p := range b.policies.All() {
		if p.Tags != nil {
			p.Tags.Close()
		}
	}

	for _, a := range b.assessments.All() {
		if a.Tags != nil {
			a.Tags.Close()
		}
	}

	for _, t := range b.templates.All() {
		if t.Tags != nil {
			t.Tags.Close()
		}
	}

	b.registry.ResetAll()
}

// AppARN builds the ARN for an App ID. Confirmed shape (read verbatim from
// the SDK's own doc comments, repeated on every AppArn field across
// types/types.go): "arn:{partition}:resiliencehub:{region}:{account}:app/{app-id}".
func (b *InMemoryBackend) AppARN(id string) string {
	return arn.Build("resiliencehub", b.region, b.accountID, "app/"+id)
}

// PolicyARN builds the ARN for a ResiliencyPolicy ID. Confirmed shape:
// "arn:{partition}:resiliencehub:{region}:{account}:resiliency-policy/{policy-id}".
func (b *InMemoryBackend) PolicyARN(id string) string {
	return arn.Build("resiliencehub", b.region, b.accountID, "resiliency-policy/"+id)
}

// AssessmentARN builds the ARN for an AppAssessment ID. DEVIATION FROM THE
// LITERAL SDK DOC COMMENT, DOCUMENTED: every AssessmentArn doc comment in
// this SDK module literally reads
// "arn:{partition}:resiliencehub:{region}:{account}:app-assessment/{app-id}"
// (PARITY.md read this verbatim and flagged it), which would make every
// assessment of the same App share one ARN -- that cannot be correct, since
// DescribeAppAssessment/DeleteAppAssessment must address one SPECIFIC
// assessment among potentially many for the same App (ListAppAssessments
// returns multiple). This is almost certainly a copy-paste doc-generation
// artifact in the upstream SDK (the same boilerplate sentence appears on
// App/AppAssessment/RecommendationTemplate ARN fields alike). This backend
// therefore mints a fresh, unique ID per assessment under the
// "app-assessment/" resource-type prefix instead of reusing the app-id
// verbatim -- see PARITY.md's judgment-call note.
func (b *InMemoryBackend) AssessmentARN(id string) string {
	return arn.Build("resiliencehub", b.region, b.accountID, "app-assessment/"+id)
}

// RecommendationTemplateARN builds the ARN for a RecommendationTemplate ID.
// UNCONFIRMED shape -- no doc comment in this SDK module describes
// RecommendationTemplateArn's resource-path segment (grepped, zero hits);
// "recommendation-template/{id}" is a defensible analogy to the confirmed
// app/resiliency-policy shapes, not a verified fact. See PARITY.md.
func (b *InMemoryBackend) RecommendationTemplateARN(id string) string {
	return arn.Build("resiliencehub", b.region, b.accountID, "recommendation-template/"+id)
}

// resourceIDFromARN extracts the resource ID from a Resilience Hub ARN of
// the form "arn:{partition}:resiliencehub:{region}:{account}:{kind}/{id}",
// returning ok=false if arnStr does not contain marker.
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

const randomIDBytes = 8

// randomHexID returns a random 16-character lowercase hex string.
func randomHexID() string {
	buf := make([]byte, randomIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}

	return hex.EncodeToString(buf)
}

// newAppID generates an App ID. UNCONFIRMED format (AppArn's {app-id}
// segment has no pattern trait in the Go types) -- a plausible UUID-shaped
// hex string, matching services/outposts' identical treatment of every
// unconfirmed ID format it generates.
func newAppID() string { return randomHexID() }

// newPolicyID generates a ResiliencyPolicy ID. UNCONFIRMED format.
func newPolicyID() string { return randomHexID() }

// newAssessmentID generates an AppAssessment ID. UNCONFIRMED format -- see
// AssessmentARN's doc comment for why this is a fresh ID, not the app-id.
func newAssessmentID() string { return randomHexID() }

// newRecommendationTemplateID generates a RecommendationTemplate ID.
// UNCONFIRMED format.
func newRecommendationTemplateID() string { return randomHexID() }

// newMetricsExportID generates a MetricsExport ID. UNCONFIRMED format.
func newMetricsExportID() string { return "export-" + randomHexID() }

// newGroupingID generates a resource-grouping-recommendation task ID.
// UNCONFIRMED format.
func newGroupingID() string { return "grouping-" + randomHexID() }

// newAppComponentID generates an AppComponent ID, used when
// CreateAppVersionAppComponentInput.Id is omitted. UNCONFIRMED format.
func newAppComponentID() string { return "appcomponent-" + randomHexID() }

// newResolutionID generates a ResolveAppVersionResources resolution ID.
// UNCONFIRMED format.
func newResolutionID() string { return "resolution-" + randomHexID() }

// containsStr reports whether ss contains s.
func containsStr(ss []string, s string) bool {
	return slices.Contains(ss, s)
}
