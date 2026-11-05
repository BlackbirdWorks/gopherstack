package lambdaweb

import (
	"crypto/rand"
	"math/big"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName = "lambda"

	functionActivationDelay = 500 * time.Millisecond
	revisionActivationDelay = 500 * time.Millisecond
	endpointActivationDelay = time.Second
	endpointUpdateDelay     = 500 * time.Millisecond
	deletionDelay           = 500 * time.Millisecond

	maxEndpointsPerFunction = 10
	maxRevisionsPerFunction = 50
	maxTotalArmVCpus        = 2000
	maxTotalRateLimit       = 100000
	maxEndpointRateLimit    = 10000
	maxNameLength           = 64
	maxTagsPerResource      = 50
	maxTagKeyLength         = 128
	maxTagValueLength       = 256
	maxPolicyBytes          = 20480

	revisionIDHexLen = 8
	requestIDLen     = 26
	domainIDLen      = 16
	globalDomainLen  = 11
	shardLen         = 3
	arnParts         = 6
	parentOnlySegs   = 2
	childSegs        = 4
)

var nameRe = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9_-]{0,62}[a-zA-Z0-9])?$`)

// InMemoryBackend is the in-memory Lambda Web Functions backend.
type InMemoryBackend struct {
	functions *store.Table[Function]
	revisions *store.Table[Revision]
	endpoints *store.Table[Endpoint]
	registry  *store.Registry
	mu        *lockmetrics.RWMutex

	clock          func() time.Time
	codes          CodeStore
	logs           LogSink
	creds          CredentialIssuer
	metrics        cwmetric.Emitter
	keys           KeyChecker
	roleTrust      RoleTrust
	principals     awsmeta.PrincipalResolver
	endpointURL    string
	run            *runtimeState
	accountID      string
	sigV4Secret    string
	lifecycleDelay time.Duration
}

// NewInMemoryBackend creates a backend for accountID.
func NewInMemoryBackend(accountID string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New("lambdaweb"),
		run:       newRuntimeState(),
		accountID: accountID,
	}

	registerAllTables(b)
	b.run.tick = b.SweepEnvironments

	return b
}

// Reset clears all backend state and stops every execution environment.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.registry.ResetAll()
	run := b.run
	b.mu.Unlock()

	run.stopAll()
}

func functionKey(region, name string) string { return region + "/" + name }

func revisionKey(region, fn, id string) string { return region + "/" + fn + "/" + id }

func endpointKey(region, fn, name string) string { return region + "/" + fn + "/e/" + name }

func functionARN(region, accountID, name string) string {
	return arn.Build(serviceName, region, accountID, resFunction+"/"+name)
}

func revisionARN(region, accountID, fn, id string) string {
	return arn.Build(serviceName, region, accountID, resFunction+"/"+fn+"/"+resRevision+"/"+id)
}

func endpointARN(region, accountID, fn, name string) string {
	return arn.Build(serviceName, region, accountID, resFunction+"/"+fn+"/"+resEndpoint+"/"+name)
}

const (
	idAlphabetConsonants = "bcdfghjklmnpqrstvwxz"
	idAlphabetShard      = "bcdfghjklmnpqrstvwxz23456789"
	idAlphabetRequest    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
)

func randomString(alphabet string, n int) string {
	out := make([]byte, n)
	limit := big.NewInt(int64(len(alphabet)))

	for i := range out {
		v, err := rand.Int(rand.Reader, limit)
		if err != nil {
			out[i] = alphabet[i%len(alphabet)]

			continue
		}

		out[i] = alphabet[v.Int64()]
	}

	return string(out)
}

func newRevisionID() string { return awsid.NewN("rev-", revisionIDHexLen) }

func newRequestID() string { return randomString(idAlphabetRequest, requestIDLen) }

func regionalDomain(region string) string {
	return randomString(idAlphabetConsonants, domainIDLen) + "." + randomString(idAlphabetShard, shardLen) +
		".lambda-web." + region + ".on.aws"
}

func globalDomain() string {
	return randomString(idAlphabetConsonants, globalDomainLen) + ".lambda-web.global.on.aws"
}

// parsedARN is a web-function ARN split into its parts.
type parsedARN struct {
	region   string
	account  string
	function string
	kind     string
	child    string
}

// parseWebARN splits arn:<partition>:lambda:<region>:<account>:web-function/<name>[/revision|endpoint/<x>].
func parseWebARN(s string) (parsedARN, bool) {
	parts := strings.SplitN(s, ":", arnParts)
	if len(parts) != arnParts || parts[0] != "arn" || parts[2] != serviceName {
		return parsedARN{}, false
	}

	segs := strings.Split(parts[5], "/")
	if len(segs) < 2 || segs[0] != resFunction || !nameRe.MatchString(segs[1]) {
		return parsedARN{}, false
	}

	out := parsedARN{region: parts[3], account: parts[4], function: segs[1]}

	switch len(segs) {
	case parentOnlySegs:
		return out, true
	case childSegs:
		if segs[2] == resRevision || segs[2] == resEndpoint {
			out.kind, out.child = segs[2], segs[3]

			return out, true
		}
	}

	return parsedARN{}, false
}

// functionRef resolves a function name or ARN to (region, name).
func (b *InMemoryBackend) functionRef(ref, region string) (string, string, error) {
	if strings.HasPrefix(ref, "arn:") {
		p, ok := parseWebARN(ref)
		if !ok || p.kind != "" || p.account != b.accountID {
			return "", "", validationError("Invalid web function ARN: %s", ref)
		}

		return p.region, p.function, nil
	}

	if !nameRe.MatchString(ref) {
		return "", "", validationError(
			"Invalid web function name %q: use up to %d letters, numbers, hyphens or underscores, "+
				"not beginning or ending with a hyphen or underscore", ref, maxNameLength)
	}

	return region, ref, nil
}

// childRef resolves an endpoint name/ARN or revision id/ARN under function fn.
func (b *InMemoryBackend) childRef(ref, kind, region, fn string) (string, error) {
	if !strings.HasPrefix(ref, "arn:") {
		return ref, nil
	}

	p, ok := parseWebARN(ref)
	if !ok || p.kind != kind || p.account != b.accountID || p.region != region || p.function != fn {
		return "", validationError("Invalid %s ARN: %s", kind, ref)
	}

	return p.child, nil
}

func (b *InMemoryBackend) functionLocked(region, name string) (*Function, error) {
	f, ok := b.functions.Get(functionKey(region, name))
	if !ok {
		return nil, notFoundError(resFunction, name)
	}

	return f, nil
}

func (b *InMemoryBackend) liveFunctionLocked(region, name string) (*Function, error) {
	f, err := b.functionLocked(region, name)
	if err != nil {
		return nil, err
	}

	if f.State == stateDeleting {
		return nil, notFoundError(resFunction, name)
	}

	return f, nil
}

func (b *InMemoryBackend) revisionsOfLocked(region, fn string) []*Revision {
	var out []*Revision

	for _, r := range b.revisions.All() {
		if r.Region == region && r.FunctionName == fn {
			out = append(out, r)
		}
	}

	slices.SortFunc(out, func(a, c *Revision) int {
		if d := c.CreatedAt.Compare(a.CreatedAt); d != 0 {
			return d
		}

		return strings.Compare(c.ID, a.ID)
	})

	return out
}

func (b *InMemoryBackend) endpointsOfLocked(region, fn string) []*Endpoint {
	var out []*Endpoint

	for _, e := range b.endpoints.All() {
		if e.Region == region && e.FunctionName == fn {
			out = append(out, e)
		}
	}

	slices.SortFunc(out, func(a, c *Endpoint) int { return strings.Compare(a.Name, c.Name) })

	return out
}

// effectiveWeightsLocked is the routing table of e: LatestRevision endpoints follow the newest Active
// revision (or the newest revision while none is Active yet).
func (b *InMemoryBackend) effectiveWeightsLocked(e *Endpoint) []RevisionWeight {
	if e.AutoDeploymentMode != modeLatestRevision {
		return slices.Clone(e.Weights)
	}

	revs := b.revisionsOfLocked(e.Region, e.FunctionName)

	for _, r := range revs {
		if r.State == stateActive {
			return []RevisionWeight{{RevisionID: r.ID, Weight: percentTotal}}
		}
	}

	if len(revs) > 0 {
		return []RevisionWeight{{RevisionID: revs[0].ID, Weight: percentTotal}}
	}

	return nil
}

const percentTotal = 100

// advanceLocked applies every lifecycle transition whose deadline has passed.
func (b *InMemoryBackend) advanceLocked() {
	now := b.now()

	b.advanceEndpointsLocked(now)
	b.advanceRevisionsLocked(now)

	for _, f := range b.functions.All() {
		if f.PendingUntil.IsZero() || now.Before(f.PendingUntil) {
			continue
		}

		if f.State == stateDeleting {
			b.removeFunctionLocked(f)

			continue
		}

		if f.State == statePending {
			f.State, f.StateReason, f.PendingUntil = stateActive, "", time.Time{}
		}
	}
}

func (b *InMemoryBackend) advanceEndpointsLocked(now time.Time) {
	for _, e := range b.endpoints.All() {
		if !e.PendingUntil.IsZero() && !now.Before(e.PendingUntil) {
			switch e.State {
			case stateDeleting:
				b.endpoints.Delete(endpointKey(e.Region, e.FunctionName, e.Name))

				continue
			case statePending:
				e.State, e.StateReason, e.PendingUntil = stateActive, "", time.Time{}
			}
		}

		if e.UpdateStatus == updateInProgress && !now.Before(e.UpdatePendingUntil) {
			e.UpdateStatus, e.UpdatePendingUntil = updateSuccessful, time.Time{}
		}
	}
}

func (b *InMemoryBackend) advanceRevisionsLocked(now time.Time) {
	activated := map[string]string{}

	for _, r := range b.revisions.All() {
		if r.State == statePending && !r.PendingUntil.IsZero() && !now.Before(r.PendingUntil) {
			r.State, r.StateReason, r.PendingUntil = stateActive, "", time.Time{}
			activated[r.Region+"/"+r.FunctionName] = r.Region
		}
	}

	for key := range activated {
		region, fn, _ := strings.Cut(key, "/")
		b.cleanupRevisionsLocked(region, fn)
	}
}

// cleanupRevisionsLocked deletes the oldest unused revisions beyond the per-function cleanup target.
func (b *InMemoryBackend) cleanupRevisionsLocked(region, fn string) {
	revs := b.revisionsOfLocked(region, fn)
	if len(revs) <= maxRevisionsPerFunction {
		return
	}

	inUse := b.revisionsInUseLocked(region, fn)

	for i := len(revs) - 1; i >= 0 && len(revs) > maxRevisionsPerFunction; i-- {
		r := revs[i]
		if inUse[r.ID] || i == 0 {
			continue
		}

		b.revisions.Delete(revisionKey(r.Region, r.FunctionName, r.ID))
		b.run.stopRevision(r.Region, r.FunctionName, r.ID)

		revs = slices.Delete(revs, i, i+1)
	}
}

func (b *InMemoryBackend) revisionsInUseLocked(region, fn string) map[string]bool {
	used := map[string]bool{}

	for _, e := range b.endpointsOfLocked(region, fn) {
		for _, w := range b.effectiveWeightsLocked(e) {
			used[w.RevisionID] = true
		}
	}

	return used
}

func (b *InMemoryBackend) removeFunctionLocked(f *Function) {
	for _, e := range b.endpointsOfLocked(f.Region, f.Name) {
		b.endpoints.Delete(endpointKey(e.Region, e.FunctionName, e.Name))
	}

	for _, r := range b.revisionsOfLocked(f.Region, f.Name) {
		b.revisions.Delete(revisionKey(r.Region, r.FunctionName, r.ID))
	}

	b.run.stopFunction(f.Region, f.Name)
	b.functions.Delete(functionKey(f.Region, f.Name))
}
