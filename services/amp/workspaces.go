package amp

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	defaultRetentionDays = 150
)

// CreateWorkspaceInput holds CreateWorkspace parameters.
type CreateWorkspaceInput struct {
	Tags        map[string]string
	Alias       string
	ClientToken string
	KMSKeyARN   string
}

// maxWorkspacesPerRegion is the default AMP workspaces-per-Region service quota.
const maxWorkspacesPerRegion = 25

type endpointBaseKey struct{}

func (b *InMemoryBackend) endpoint(ctx context.Context, sc scope, id string) string {
	base, ok := ctx.Value(endpointBaseKey{}).(string)
	if !ok || base == "" {
		base = "https://aps-workspaces." + sc.region + ".amazonaws.com"
	}

	return base + "/workspaces/" + id + "/"
}

func (b *InMemoryBackend) wireWorkspace(ctx context.Context, w *Workspace) wWorkspace {
	return wWorkspace{
		Alias: w.Alias, Arn: w.ARN, KmsKeyArn: w.KMSKeyARN, WorkspaceID: w.ID, Tags: cloneTags(w.Tags),
		PrometheusEndpoint: b.endpoint(ctx, scope{region: w.Region}, w.ID),
		Status:             wCode{StatusCode: b.state(w.Pending)},
		CreatedAt:          epoch(w.CreatedAt),
	}
}

// workspaceLocked finds a live workspace owned by the caller.
func (b *InMemoryBackend) workspaceLocked(sc scope, id string) (*Workspace, error) {
	w, ok := b.workspaces.Get(id)
	if !ok || !sc.owns(w.AccountID, w.Region) || b.gone(w.Pending) {
		return nil, notFound(resWorkspace, id, "Workspace %s does not exist", id)
	}

	return w, nil
}

// activeWorkspaceLocked is workspaceLocked for mutations, which need an ACTIVE workspace.
func (b *InMemoryBackend) activeWorkspaceLocked(sc scope, id string) (*Workspace, error) {
	w, err := b.workspaceLocked(sc, id)
	if err != nil {
		return nil, err
	}

	if st := b.state(w.Pending); st != statusActive {
		return nil, conflict(resWorkspace, id, "Workspace %s is %s", id, st)
	}

	return w, nil
}

func validateKMS(key string) error {
	if key != "" && !kmsKeyRe.MatchString(key) {
		return invalid("kmsKeyArn must be a KMS key ARN")
	}

	return nil
}

// CreateWorkspace creates a workspace in CREATING state.
func (b *InMemoryBackend) CreateWorkspace(ctx context.Context, in CreateWorkspaceInput) (wWorkspace, error) {
	alias := strings.TrimSpace(in.Alias)

	for _, err := range []error{validateAlias(alias), validateTags(in.Tags), validateKMS(in.KMSKeyARN)} {
		if err != nil {
			return wWorkspace{}, err
		}
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CreateWorkspace")
	defer b.mu.Unlock()

	b.purgeLocked()

	w, err := idempotency.Create(b.idem, "CreateWorkspace", in.ClientToken, idempotency.Fingerprint(in),
		func(w *Workspace) string { return w.ID },
		func(id string) (*Workspace, error) { return b.workspaceLocked(sc, id) },
		func() (*Workspace, error) {
			if b.workspaceCountLocked(sc) >= maxWorkspacesPerRegion {
				return nil, quotaExceeded(resWorkspace,
					"Workspace quota of %d per Region exceeded", maxWorkspacesPerRegion)
			}

			id := "ws-" + uuid.NewString()
			w := &Workspace{
				ID: id, ARN: sc.arn("workspace/" + id), AccountID: sc.account, Region: sc.region,
				Alias: alias, KMSKeyARN: in.KMSKeyARN, Tags: cloneTags(in.Tags), CreatedAt: b.now(),
				Pending: b.pend(statusCreating),
				Config:  WorkspaceConfig{RetentionPeriodInDays: defaultRetentionDays},
			}
			b.workspaces.Put(w)

			return w, nil
		})
	if err != nil {
		return wWorkspace{}, mapIdem(err)
	}

	return b.wireWorkspace(ctx, w), nil
}

func (b *InMemoryBackend) workspaceCountLocked(sc scope) int {
	n := 0

	b.workspaces.Range(func(w *Workspace) bool {
		if sc.owns(w.AccountID, w.Region) && !b.gone(w.Pending) {
			n++
		}

		return true
	})

	return n
}

func mapIdem(err error) error {
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return conflict(resWorkspace, "", "ClientToken was already used with different parameters")
	}

	return err
}

// DescribeWorkspace returns a workspace.
func (b *InMemoryBackend) DescribeWorkspace(ctx context.Context, id string) (wWorkspace, error) {
	b.mu.RLock("DescribeWorkspace")
	defer b.mu.RUnlock()

	w, err := b.workspaceLocked(b.scopeOf(ctx), id)
	if err != nil {
		return wWorkspace{}, err
	}

	return b.wireWorkspace(ctx, w), nil
}

// ListWorkspaces lists workspaces, optionally by alias prefix.
func (b *InMemoryBackend) ListWorkspaces(
	ctx context.Context, alias, token string, maxResults int,
) (page.Page[wWorkspace], error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("ListWorkspaces")
	defer b.mu.RUnlock()

	var items []*Workspace

	b.workspaces.Range(func(w *Workspace) bool {
		if sc.owns(w.AccountID, w.Region) && !b.gone(w.Pending) && hasPrefixFold(w.Alias, strings.TrimSpace(alias)) {
			items = append(items, w)
		}

		return true
	})

	slices.SortFunc(items, func(x, y *Workspace) int { return cmp.Compare(x.ID, y.ID) })

	pg, err := restroute.Paginate(items, token, maxResults, listDefault, listMax)
	if err != nil {
		return page.Page[wWorkspace]{}, err
	}

	out := page.Page[wWorkspace]{Next: pg.Next, Data: make([]wWorkspace, 0, len(pg.Data))}
	for _, w := range pg.Data {
		out.Data = append(out.Data, b.wireWorkspace(ctx, w))
	}

	return out, nil
}

// UpdateWorkspaceAlias sets or clears a workspace alias.
func (b *InMemoryBackend) UpdateWorkspaceAlias(ctx context.Context, id, alias string) error {
	alias = strings.TrimSpace(alias)
	if err := validateAlias(alias); err != nil {
		return err
	}

	b.mu.Lock("UpdateWorkspaceAlias")
	defer b.mu.Unlock()

	b.purgeLocked()

	w, err := b.activeWorkspaceLocked(b.scopeOf(ctx), id)
	if err != nil {
		return err
	}

	w.Alias = alias

	return nil
}

// DeleteWorkspace starts workspace deletion; children and samples go with it.
func (b *InMemoryBackend) DeleteWorkspace(ctx context.Context, id string) error {
	b.mu.Lock("DeleteWorkspace")
	defer b.mu.Unlock()

	b.purgeLocked()

	w, err := b.workspaceLocked(b.scopeOf(ctx), id)
	if err != nil {
		return err
	}

	if w.Pending.Kind == statusDeleting {
		return nil
	}

	w.Pending = b.pend(statusDeleting)

	return nil
}
