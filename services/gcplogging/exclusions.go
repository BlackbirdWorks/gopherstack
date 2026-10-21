package gcplogging

import (
	"slices"
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func cloneExclusion(e *loggingpb.LogExclusion) *loggingpb.LogExclusion {
	out, _ := proto.Clone(e).(*loggingpb.LogExclusion)

	return out
}

func validateExclusion(e *loggingpb.LogExclusion) error {
	if e == nil {
		return errInvalid("exclusion is required.")
	}

	if e.GetFilter() == "" {
		return errInvalid("Exclusion filter must be set.")
	}

	if len(e.GetDescription()) > maxDescriptionLen {
		return errInvalid("Exclusion description is too long.")
	}

	_, err := compileFilter(e.GetFilter())

	return err
}

// CreateExclusion adds a log exclusion under the parent.
func (b *InMemoryBackend) CreateExclusion(req *loggingpb.CreateExclusionRequest) (*loggingpb.LogExclusion, error) {
	parent := req.GetParent()
	if err := checkParent(parent); err != nil {
		return nil, err
	}

	in := req.GetExclusion()
	if err := validateExclusion(in); err != nil {
		return nil, err
	}

	if err := checkResourceID("exclusion", in.GetName()); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateExclusion")
	defer b.mu.Unlock()

	key := parent + "/exclusions/" + in.GetName()
	if _, ok := b.exclusions[key]; ok {
		return nil, errExists("Exclusion %s already exists.", in.GetName())
	}

	now := timestamppb.New(b.now())
	ex := cloneExclusion(in)
	ex.CreateTime, ex.UpdateTime = now, now
	b.exclusions[key] = ex

	return cloneExclusion(ex), nil
}

// GetExclusion returns one exclusion.
func (b *InMemoryBackend) GetExclusion(name string) (*loggingpb.LogExclusion, error) {
	if _, _, err := childName(name, "exclusions"); err != nil {
		return nil, err
	}

	b.mu.RLock("GetExclusion")
	defer b.mu.RUnlock()

	ex, ok := b.exclusions[name]
	if !ok {
		return nil, errNotFound("Exclusion %s does not exist.", name)
	}

	return cloneExclusion(ex), nil
}

// ListExclusions returns the parent's exclusions ordered by name.
func (b *InMemoryBackend) ListExclusions(
	req *loggingpb.ListExclusionsRequest,
) ([]*loggingpb.LogExclusion, string, error) {
	parent := req.GetParent()
	if err := checkParent(parent); err != nil {
		return nil, "", err
	}

	if err := page.ValidateToken(req.GetPageToken()); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	b.mu.RLock("ListExclusions")

	var all []*loggingpb.LogExclusion

	for key, ex := range b.exclusions {
		if strings.HasPrefix(key, parent+"/exclusions/") {
			all = append(all, cloneExclusion(ex))
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c *loggingpb.LogExclusion) int { return strings.Compare(a.GetName(), c.GetName()) })
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

// UpdateExclusion applies the masked fields of the request exclusion.
func (b *InMemoryBackend) UpdateExclusion(req *loggingpb.UpdateExclusionRequest) (*loggingpb.LogExclusion, error) {
	if _, _, err := childName(req.GetName(), "exclusions"); err != nil {
		return nil, err
	}

	if len(req.GetUpdateMask().GetPaths()) == 0 {
		return nil, errInvalid("update_mask must list the fields to change.")
	}

	src := req.GetExclusion()
	if src == nil {
		return nil, errInvalid("exclusion is required.")
	}

	b.mu.Lock("UpdateExclusion")
	defer b.mu.Unlock()

	cur, ok := b.exclusions[req.GetName()]
	if !ok {
		return nil, errNotFound("Exclusion %s does not exist.", req.GetName())
	}

	next := cloneExclusion(cur)

	for _, p := range req.GetUpdateMask().GetPaths() {
		switch p {
		case pathDescription:
			next.Description = src.GetDescription()
		case pathFilter:
			next.Filter = src.GetFilter()
		case pathDisabled:
			next.Disabled = src.GetDisabled()
		default:
			return nil, errInvalid(
				"Invalid update_mask path [%s]: supported paths are description, filter, disabled.",
				p,
			)
		}
	}

	if err := validateExclusion(next); err != nil {
		return nil, err
	}

	next.UpdateTime = timestamppb.New(b.now())
	b.exclusions[req.GetName()] = next

	return cloneExclusion(next), nil
}

// DeleteExclusion removes an exclusion.
func (b *InMemoryBackend) DeleteExclusion(name string) error {
	if _, _, err := childName(name, "exclusions"); err != nil {
		return err
	}

	b.mu.Lock("DeleteExclusion")
	defer b.mu.Unlock()

	if _, ok := b.exclusions[name]; !ok {
		return errNotFound("Exclusion %s does not exist.", name)
	}

	delete(b.exclusions, name)

	return nil
}
