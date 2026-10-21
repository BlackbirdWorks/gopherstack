package gcplogging

import (
	"slices"
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func cloneView(v *loggingpb.LogView) *loggingpb.LogView {
	out, _ := proto.Clone(v).(*loggingpb.LogView)

	return out
}

func validateView(v *loggingpb.LogView) error {
	if len(v.GetDescription()) > maxDescriptionLen {
		return errInvalid("View description is too long.")
	}

	_, err := compileFilter(v.GetFilter())

	return err
}

func (b *InMemoryBackend) activeBucketLocked(name string) error {
	bd, ok := b.buckets[name]
	if !ok {
		return errNotFound("Bucket %s does not exist.", name)
	}

	if !bd.deleteRequested.IsZero() {
		return errPrecondition("Bucket %s is pending deletion.", name)
	}

	return nil
}

// CreateView adds a view to a bucket.
func (b *InMemoryBackend) CreateView(req *loggingpb.CreateViewRequest) (*loggingpb.LogView, error) {
	if err := checkBucketName(req.GetParent()); err != nil {
		return nil, err
	}

	if !bucketIDRe.MatchString(req.GetViewId()) {
		return nil, errInvalid("Invalid view id [%s].", req.GetViewId())
	}

	in := req.GetView()
	if in == nil {
		in = &loggingpb.LogView{}
	}

	if err := validateView(in); err != nil {
		return nil, err
	}

	b.seed(parentOf(req.GetParent()))
	b.mu.Lock("CreateView")
	defer b.mu.Unlock()

	if err := b.activeBucketLocked(req.GetParent()); err != nil {
		return nil, err
	}

	name := req.GetParent() + "/views/" + req.GetViewId()
	if _, ok := b.views[name]; ok {
		return nil, errExists("View %s already exists.", name)
	}

	now := timestamppb.New(b.now())
	v := cloneView(in)
	v.Name, v.CreateTime, v.UpdateTime = name, now, now
	b.views[name] = v

	return cloneView(v), nil
}

// GetView returns one view.
func (b *InMemoryBackend) GetView(name string) (*loggingpb.LogView, error) {
	m := viewRe.FindStringSubmatch(name)
	if m == nil {
		return nil, errInvalid("Invalid view name [%s].", name)
	}

	b.seed(parentOf(name))
	b.mu.RLock("GetView")
	defer b.mu.RUnlock()

	v, ok := b.views[name]
	if !ok {
		return nil, errNotFound("View %s does not exist.", name)
	}

	return cloneView(v), nil
}

// ListViews returns a bucket's views ordered by name.
func (b *InMemoryBackend) ListViews(req *loggingpb.ListViewsRequest) ([]*loggingpb.LogView, string, error) {
	if err := checkBucketName(req.GetParent()); err != nil {
		return nil, "", err
	}

	if err := pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	b.seed(parentOf(req.GetParent()))
	b.mu.RLock("ListViews")

	var all []*loggingpb.LogView

	for name, v := range b.views {
		if strings.HasPrefix(name, req.GetParent()+"/views/") {
			all = append(all, cloneView(v))
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c *loggingpb.LogView) int { return strings.Compare(a.GetName(), c.GetName()) })
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

// UpdateView applies the masked fields to a view.
func (b *InMemoryBackend) UpdateView(req *loggingpb.UpdateViewRequest) (*loggingpb.LogView, error) {
	name := req.GetName()
	if !viewRe.MatchString(name) {
		return nil, errInvalid("Invalid view name [%s].", name)
	}

	if req.GetView() == nil {
		return nil, errInvalid("view is required.")
	}

	b.seed(parentOf(name))
	b.mu.Lock("UpdateView")
	defer b.mu.Unlock()

	cur, ok := b.views[name]
	if !ok {
		return nil, errNotFound("View %s does not exist.", name)
	}

	if strings.HasSuffix(name, "/views/"+allLogsViewID) {
		return nil, errInvalid("The _AllLogs view cannot be updated.")
	}

	next := cloneView(cur)
	paths := req.GetUpdateMask().GetPaths()

	if len(paths) == 0 {
		paths = []string{pathDescription, pathFilter}
	}

	for _, p := range paths {
		switch p {
		case pathDescription:
			next.Description = req.GetView().GetDescription()
		case pathFilter:
			next.Filter = req.GetView().GetFilter()
		default:
			return nil, errInvalid("Invalid update_mask path [%s]: supported paths are description, filter.", p)
		}
	}

	if err := validateView(next); err != nil {
		return nil, err
	}

	next.UpdateTime = timestamppb.New(b.now())
	b.views[name] = next

	return cloneView(next), nil
}

// DeleteView removes a view.
func (b *InMemoryBackend) DeleteView(name string) error {
	if !viewRe.MatchString(name) {
		return errInvalid("Invalid view name [%s].", name)
	}

	b.seed(parentOf(name))
	b.mu.Lock("DeleteView")
	defer b.mu.Unlock()

	if _, ok := b.views[name]; !ok {
		return errNotFound("View %s does not exist.", name)
	}

	if strings.HasSuffix(name, "/views/"+allLogsViewID) {
		return errInvalid("The _AllLogs view cannot be deleted.")
	}

	delete(b.views, name)

	return nil
}
