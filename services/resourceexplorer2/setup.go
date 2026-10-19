package resourceexplorer2

import (
	"cmp"
	"context"
	"slices"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const defaultSetupView = "default-view"

// CreateSetup creates an index and default view in each Region, optionally making one an aggregator.
func (b *InMemoryBackend) CreateSetup(
	ctx context.Context,
	regions []string,
	viewName string,
	aggregators []string,
) (string, error) {
	if len(regions) == 0 {
		return "", invalid("RegionList must contain at least one Region")
	}

	for _, r := range append(slices.Clone(regions), aggregators...) {
		if err := validateRegion(r); err != nil {
			return "", err
		}
	}

	if len(aggregators) > 1 {
		return "", invalid("only one aggregator Region is allowed")
	}

	if len(aggregators) == 1 && !slices.Contains(regions, aggregators[0]) {
		return "", invalid("AggregatorRegions must be listed in RegionList")
	}

	if viewName == "" {
		viewName = defaultSetupView
	}

	if !viewNameRE.MatchString(viewName) {
		return "", invalid("ViewName must be 1-%d letters, digits or hyphens", maxViewName)
	}

	account := b.callerAccount(ctx)
	task := &SetupTask{ID: uuid.NewString(), AccountID: account, Kind: taskCreate}

	for _, r := range uniqueSorted(regions) {
		task.Regions = append(task.Regions, b.setupRegion(ctx, r, viewName, slices.Contains(aggregators, r)))
	}

	b.mu.Lock("CreateSetup")
	defer b.mu.Unlock()

	b.tasks.Put(task)

	return task.ID, nil
}

func uniqueSorted(in []string) []string {
	out := slices.Clone(in)
	slices.Sort(out)

	return slices.Compact(out)
}

func errDetail(err error) *ErrorDetail {
	if apiErr, ok := asAPIError(err); ok {
		return &ErrorDetail{Code: apiErr.Code, Message: apiErr.Message}
	}

	return &ErrorDetail{Code: "InternalServerException", Message: err.Error()}
}

func (b *InMemoryBackend) setupRegion(ctx context.Context, region, viewName string, aggregator bool) TaskRegion {
	rctx := awsmeta.WithRegion(ctx, region)
	out := TaskRegion{Region: region, IndexStatus: statusSucceeded, ViewStatus: statusSucceeded}

	typ := indexTypeLocal
	if aggregator {
		typ = indexTypeAggregator
	}

	idx, err := b.createIndexOfType(rctx, "", typ, nil)

	switch {
	case err == nil:
		out.IndexARN = idx.Arn
	case isConflict(err):
		out.IndexStatus = statusSkipped
		out.IndexARN = b.existingIndexARN(rctx)
	default:
		out.IndexStatus, out.IndexErr = statusFailed, errDetail(err)
		out.ViewStatus = statusSkipped

		return out
	}

	view, err := b.CreateView(rctx, &CreateViewInput{ViewName: viewName})

	switch {
	case err == nil:
		out.ViewARN = view.ViewArn
		if _, aerr := b.AssociateDefaultView(rctx, view.ViewArn); aerr != nil {
			out.ViewErr = errDetail(aerr)
		}
	case isConflict(err):
		out.ViewStatus = statusSkipped
	default:
		out.ViewStatus, out.ViewErr = statusFailed, errDetail(err)
	}

	return out
}

func (b *InMemoryBackend) existingIndexARN(ctx context.Context) string {
	d, err := b.GetIndex(ctx)
	if err != nil {
		return ""
	}

	return d.Arn
}

// DeleteSetup deletes the index of the listed Regions, or of every Region.
func (b *InMemoryBackend) DeleteSetup(ctx context.Context, regions []string, all bool) (string, error) {
	if !all && len(regions) == 0 {
		return "", invalid("RegionList is required unless DeleteInAllRegions is set")
	}

	for _, r := range regions {
		if err := validateRegion(r); err != nil {
			return "", err
		}
	}

	account := b.callerAccount(ctx)

	if all {
		b.mu.RLock("DeleteSetup")
		for _, i := range b.liveIndexes(account) {
			regions = append(regions, i.Region)
		}
		b.mu.RUnlock()
	}

	task := &SetupTask{ID: uuid.NewString(), AccountID: account, Kind: taskDelete}

	for _, r := range uniqueSorted(regions) {
		rctx := awsmeta.WithRegion(ctx, r)
		tr := TaskRegion{Region: r, IndexStatus: statusSucceeded, ViewStatus: statusSucceeded}

		d, err := b.GetIndex(rctx)

		switch {
		case err != nil:
			tr.IndexStatus, tr.ViewStatus = statusSkipped, statusSkipped
		default:
			tr.IndexARN = d.Arn
			if _, err = b.DeleteIndex(rctx, d.Arn); err != nil {
				tr.IndexStatus, tr.IndexErr = statusFailed, errDetail(err)
			}
		}

		task.Regions = append(task.Regions, tr)
	}

	b.mu.Lock("DeleteSetup")
	defer b.mu.Unlock()

	b.tasks.Put(task)

	return task.ID, nil
}

// IndexStatusWire is the IndexStatus shape.
type IndexStatusWire struct {
	ErrorDetails *ErrorDetail  `json:"ErrorDetails,omitempty"`
	Index        *IndexSummary `json:"Index,omitempty"`
	Status       string        `json:"Status"`
}

// ViewStatusWire is the ViewStatus shape.
type ViewStatusWire struct {
	ErrorDetails *ErrorDetail `json:"ErrorDetails,omitempty"`
	View         *ViewWire    `json:"View,omitempty"`
	Status       string       `json:"Status"`
}

// RegionStatusWire is the RegionStatus shape.
type RegionStatusWire struct {
	Index  *IndexStatusWire `json:"Index,omitempty"`
	View   *ViewStatusWire  `json:"View,omitempty"`
	Region string           `json:"Region"`
}

// GetSetup reports the live status of a setup task per Region.
func (b *InMemoryBackend) GetSetup(
	ctx context.Context, taskID, token string, maxResults int,
) (page.Page[RegionStatusWire], error) {
	b.mu.RLock("GetSetup")
	defer b.mu.RUnlock()

	t, ok := b.tasks.Get(taskID)
	if !ok || t.AccountID != b.callerAccount(ctx) {
		return page.Page[RegionStatusWire]{}, notFound("task %s not found", taskID)
	}

	rows := make([]RegionStatusWire, 0, len(t.Regions))
	for i := range t.Regions {
		rows = append(rows, b.regionStatus(t.Kind, &t.Regions[i]))
	}

	slices.SortFunc(rows, func(x, y RegionStatusWire) int { return cmp.Compare(x.Region, y.Region) })

	return paginate(rows, token, maxResults, maxPage)
}

func (b *InMemoryBackend) regionStatus(kind string, tr *TaskRegion) RegionStatusWire {
	idx := &IndexStatusWire{Status: tr.IndexStatus, ErrorDetails: tr.IndexErr}

	if i, ok := b.indexes.Get(tr.IndexARN); ok && tr.IndexARN != "" {
		st := b.stateOf(i)
		idx.Index = &IndexSummary{Arn: i.ARN, Region: i.Region, Type: i.Type}

		if tr.IndexStatus == statusSucceeded {
			switch {
			case kind == taskCreate && st != stateActive:
				idx.Status = statusInProgress
			case kind == taskDelete && st != stateDeleted:
				idx.Status = statusInProgress
			}
		}
	}

	out := RegionStatusWire{Region: tr.Region, Index: idx}

	if kind == taskCreate {
		vs := &ViewStatusWire{Status: tr.ViewStatus, ErrorDetails: tr.ViewErr}

		if v, ok := b.views.Get(tr.ViewARN); ok && tr.ViewARN != "" {
			w := toWire(v)
			vs.View = &w
		}

		out.View = vs
	}

	return out
}
