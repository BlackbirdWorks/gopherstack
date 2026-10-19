package amp

import (
	"cmp"
	"context"
	"maps"
	"slices"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	defaultEvalInterval = 60
	defaultShingleSize  = 8
	defaultSampleSize   = 256
	minEvalInterval     = 1
	maxShingle          = 1024
	maxSample           = 1 << 20
	missingMark         = "markAsAnomaly"
	missingSkip         = "skip"
)

// DetectorInput holds Create/PutAnomalyDetector parameters.
type DetectorInput struct {
	Tags              map[string]string
	Labels            map[string]string
	IgnoreAbove       *IgnoreNear
	IgnoreBelow       *IgnoreNear
	EvalInterval      *int32
	ShingleSize       *int32
	SampleSize        *int32
	WorkspaceID       string
	ID                string
	Alias             string
	ClientToken       string
	Query             string
	MissingDataAction string
}

func validateDetector(in DetectorInput) error {
	if err := validateDetectorQuery(in.Query); err != nil {
		return err
	}

	if in.EvalInterval != nil && (*in.EvalInterval < minEvalInterval || *in.EvalInterval > maxIntervalSecs) {
		return invalid("evaluationIntervalInSeconds must be between %d and %d", minEvalInterval, maxIntervalSecs)
	}

	if in.ShingleSize != nil && (*in.ShingleSize < 1 || *in.ShingleSize > maxShingle) {
		return invalid("shingleSize must be between 1 and %d", maxShingle)
	}

	if in.SampleSize != nil && (*in.SampleSize < 1 || *in.SampleSize > maxSample) {
		return invalid("sampleSize must be between 1 and %d", maxSample)
	}

	for _, n := range []*IgnoreNear{in.IgnoreAbove, in.IgnoreBelow} {
		if n != nil && (n.Amount == nil) == (n.Ratio == nil) {
			return invalid("ignoreNearExpected must set exactly one of amount or ratio")
		}
	}

	return nil
}

func (b *InMemoryBackend) wireDetector(d *AnomalyDetector, full bool) wDetector {
	out := wDetector{
		Alias: d.Alias, AnomalyDetectorID: d.ID, Arn: d.ARN, Tags: cloneTags(d.Tags),
		Status: b.wireStatus(d.Pending), CreatedAt: epoch(d.CreatedAt), ModifiedAt: epoch(d.ModifiedAt),
	}
	if !full {
		return out
	}

	interval := d.EvaluationIntervalInSeconds
	out.EvaluationIntervalInSeconds = &interval
	out.Labels = maps.Clone(d.Labels)
	out.Configuration = &wDetectorConfig{RandomCutForest: wRandomCutForest{
		Query: d.Query, ShingleSize: d.ShingleSize, SampleSize: d.SampleSize,
		IgnoreNearExpectedFromAbove: wireIgnore(d.IgnoreAbove), IgnoreNearExpectedFromBelow: wireIgnore(d.IgnoreBelow),
	}}

	if d.MissingDataAction != "" {
		out.MissingDataAction = map[string]bool{d.MissingDataAction: true}
	}

	return out
}

func wireIgnore(n *IgnoreNear) *wIgnoreNear {
	if n == nil {
		return nil
	}

	return &wIgnoreNear{Amount: n.Amount, Ratio: n.Ratio}
}

func (b *InMemoryBackend) detectorLocked(sc scope, wsID, id string) (*AnomalyDetector, error) {
	if _, err := b.workspaceLocked(sc, wsID); err != nil {
		return nil, err
	}

	d, ok := b.detectors.Get(detectorKey(wsID, id))
	if !ok || b.gone(d.Pending) {
		return nil, notFound(resDetector, id, "Anomaly detector %s does not exist", id)
	}

	return d, nil
}

func applyDetector(d *AnomalyDetector, in DetectorInput) {
	d.Alias, d.Query, d.Labels = in.Alias, in.Query, maps.Clone(in.Labels)
	d.IgnoreAbove, d.IgnoreBelow, d.MissingDataAction = in.IgnoreAbove, in.IgnoreBelow, in.MissingDataAction
	d.EvaluationIntervalInSeconds = cmpOr(in.EvalInterval, defaultEvalInterval)
	d.ShingleSize = cmpOr(in.ShingleSize, defaultShingleSize)
	d.SampleSize = cmpOr(in.SampleSize, defaultSampleSize)
}

func cmpOr(p *int32, def int32) int32 {
	if p == nil {
		return def
	}

	return *p
}

// CreateAnomalyDetector creates a detector in CREATING state.
func (b *InMemoryBackend) CreateAnomalyDetector(ctx context.Context, in DetectorInput) (wDetector, error) {
	for _, err := range []error{validateDetector(in), validateTags(in.Tags), validateAlias(in.Alias)} {
		if err != nil {
			return wDetector{}, err
		}
	}

	if in.Alias == "" {
		return wDetector{}, invalid("alias is required")
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CreateAnomalyDetector")
	defer b.mu.Unlock()

	b.purgeLocked()

	if _, err := b.activeWorkspaceLocked(sc, in.WorkspaceID); err != nil {
		return wDetector{}, err
	}

	d, err := idempotency.Create(b.idem, "CreateAnomalyDetector", in.ClientToken, idempotency.Fingerprint(in),
		detectorKeyOf,
		func(key string) (*AnomalyDetector, error) {
			d, ok := b.detectors.Get(key)
			if !ok {
				return nil, notFound(resDetector, key, "gone")
			}

			return d, nil
		},
		func() (*AnomalyDetector, error) {
			id := "ad-" + uuid.NewString()
			now := b.now()
			d := &AnomalyDetector{
				WorkspaceID: in.WorkspaceID, ID: id, CreatedAt: now, ModifiedAt: now, Tags: cloneTags(in.Tags),
				ARN: sc.arn("anomalydetector/" + in.WorkspaceID + "/" + id), Pending: b.pend(statusCreating),
			}
			applyDetector(d, in)
			b.detectors.Put(d)

			return d, nil
		})
	if err != nil {
		return wDetector{}, mapIdem(err)
	}

	return b.wireDetector(d, false), nil
}

// PutAnomalyDetector replaces a detector's configuration.
func (b *InMemoryBackend) PutAnomalyDetector(ctx context.Context, in DetectorInput) (wDetector, error) {
	if err := validateDetector(in); err != nil {
		return wDetector{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("PutAnomalyDetector")
	defer b.mu.Unlock()

	b.purgeLocked()

	if _, err := b.activeWorkspaceLocked(sc, in.WorkspaceID); err != nil {
		return wDetector{}, err
	}

	d, err := b.detectorLocked(sc, in.WorkspaceID, in.ID)
	if err != nil {
		return wDetector{}, err
	}

	if ierr := b.idle(resDetector, in.ID, d.Pending); ierr != nil {
		return wDetector{}, ierr
	}

	in.Alias = cmpString(in.Alias, d.Alias)
	applyDetector(d, in)
	d.ModifiedAt = b.now()
	d.Pending = b.pend(statusUpdating)

	return b.wireDetector(d, false), nil
}

func cmpString(v, def string) string {
	if v == "" {
		return def
	}

	return v
}

// DescribeAnomalyDetector returns a detector with its configuration.
func (b *InMemoryBackend) DescribeAnomalyDetector(ctx context.Context, wsID, id string) (wDetector, error) {
	b.mu.RLock("DescribeAnomalyDetector")
	defer b.mu.RUnlock()

	d, err := b.detectorLocked(b.scopeOf(ctx), wsID, id)
	if err != nil {
		return wDetector{}, err
	}

	return b.wireDetector(d, true), nil
}

// ListAnomalyDetectors lists detectors, optionally by exact alias.
func (b *InMemoryBackend) ListAnomalyDetectors(
	ctx context.Context, wsID, alias, token string, maxResults int,
) (page.Page[wDetector], error) {
	b.mu.RLock("ListAnomalyDetectors")
	defer b.mu.RUnlock()

	if _, err := b.workspaceLocked(b.scopeOf(ctx), wsID); err != nil {
		return page.Page[wDetector]{}, err
	}

	var items []*AnomalyDetector

	b.detectors.Range(func(d *AnomalyDetector) bool {
		if d.WorkspaceID == wsID && !b.gone(d.Pending) && (alias == "" || d.Alias == alias) {
			items = append(items, d)
		}

		return true
	})

	slices.SortFunc(items, func(x, y *AnomalyDetector) int { return cmp.Compare(x.ID, y.ID) })

	pg, err := restroute.Paginate(items, token, maxResults, listDefault, listMax)
	if err != nil {
		return page.Page[wDetector]{}, err
	}

	out := page.Page[wDetector]{Next: pg.Next, Data: make([]wDetector, 0, len(pg.Data))}
	for _, d := range pg.Data {
		out.Data = append(out.Data, b.wireDetector(d, false))
	}

	return out, nil
}

// DeleteAnomalyDetector starts detector deletion.
func (b *InMemoryBackend) DeleteAnomalyDetector(ctx context.Context, wsID, id string) error {
	sc := b.scopeOf(ctx)

	b.mu.Lock("DeleteAnomalyDetector")
	defer b.mu.Unlock()

	b.purgeLocked()

	if _, err := b.activeWorkspaceLocked(sc, wsID); err != nil {
		return err
	}

	d, err := b.detectorLocked(sc, wsID, id)
	if err != nil {
		return err
	}

	if ierr := b.idle(resDetector, id, d.Pending); ierr != nil {
		return ierr
	}

	d.Pending = b.pend(statusDeleting)

	return nil
}
