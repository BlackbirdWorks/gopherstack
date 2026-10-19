package amp

import (
	"cmp"
	"context"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// NamespaceInput holds Create/PutRuleGroupsNamespace parameters.
type NamespaceInput struct {
	Tags        map[string]string
	WorkspaceID string
	Name        string
	ClientToken string
	Data        []byte
}

func (b *InMemoryBackend) wireNamespace(n *RuleGroupsNamespace, withData bool) wNamespace {
	w := wNamespace{
		Arn: n.ARN, Name: n.Name, Tags: cloneTags(n.Tags), Status: b.wireStatus(n.Doc.Pending),
		CreatedAt: epoch(n.Doc.CreatedAt), ModifiedAt: epoch(n.Doc.ModifiedAt),
	}
	if withData {
		w.Data = n.Doc.Data
	}

	return w
}

func (b *InMemoryBackend) namespaceLocked(sc scope, wsID, name string) (*RuleGroupsNamespace, error) {
	if _, err := b.workspaceLocked(sc, wsID); err != nil {
		return nil, err
	}

	n, ok := b.namespaces.Get(nsKey(wsID, name))
	if !ok || b.gone(n.Doc.Pending) {
		return nil, notFound(resNamespace, name, "Rule groups namespace %s does not exist", name)
	}

	return n, nil
}

func validateNamespaceName(name string) error {
	if name == "" || len(name) > maxNamespaceLen || !namespaceRe.MatchString(name) {
		return invalid("name must be 1-%d characters of letters, digits, '.', '-' or '_'", maxNamespaceLen)
	}

	return nil
}

// CreateRuleGroupsNamespace validates and stores a rules file.
func (b *InMemoryBackend) CreateRuleGroupsNamespace(ctx context.Context, in NamespaceInput) (wNamespace, error) {
	for _, err := range []error{
		validateNamespaceName(in.Name), validateTags(in.Tags), validateData("rule groups namespace", in.Data),
	} {
		if err != nil {
			return wNamespace{}, err
		}
	}

	if err := validateRules(in.Data); err != nil {
		return wNamespace{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CreateRuleGroupsNamespace")
	defer b.mu.Unlock()

	b.purgeLocked()

	if _, err := b.activeWorkspaceLocked(sc, in.WorkspaceID); err != nil {
		return wNamespace{}, err
	}

	n, err := idempotency.Create(b.idem, "CreateRuleGroupsNamespace", in.ClientToken, idempotency.Fingerprint(in),
		func(n *RuleGroupsNamespace) string { return nsKey(n.WorkspaceID, n.Name) },
		func(key string) (*RuleGroupsNamespace, error) {
			n, ok := b.namespaces.Get(key)
			if !ok {
				return nil, notFound(resNamespace, key, "gone")
			}

			return n, nil
		},
		func() (*RuleGroupsNamespace, error) {
			if _, exists := b.namespaces.Get(nsKey(in.WorkspaceID, in.Name)); exists {
				return nil, conflict(resNamespace, in.Name, "Rule groups namespace %s already exists", in.Name)
			}

			now := b.now()
			n := &RuleGroupsNamespace{
				WorkspaceID: in.WorkspaceID, Name: in.Name, Tags: cloneTags(in.Tags),
				ARN: sc.arn("rulegroupsnamespace/" + in.WorkspaceID + "/" + in.Name),
				Doc: Doc{Data: in.Data, CreatedAt: now, ModifiedAt: now, Pending: b.pend(statusCreating)},
			}
			b.namespaces.Put(n)

			return n, nil
		})
	if err != nil {
		return wNamespace{}, mapIdem(err)
	}

	return b.wireNamespace(n, false), nil
}

// PutRuleGroupsNamespace replaces a rules file.
func (b *InMemoryBackend) PutRuleGroupsNamespace(ctx context.Context, in NamespaceInput) (wNamespace, error) {
	if err := validateData("rule groups namespace", in.Data); err != nil {
		return wNamespace{}, err
	}

	if err := validateRules(in.Data); err != nil {
		return wNamespace{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("PutRuleGroupsNamespace")
	defer b.mu.Unlock()

	b.purgeLocked()

	if _, err := b.activeWorkspaceLocked(sc, in.WorkspaceID); err != nil {
		return wNamespace{}, err
	}

	n, err := b.namespaceLocked(sc, in.WorkspaceID, in.Name)
	if err != nil {
		return wNamespace{}, err
	}

	if st := b.state(n.Doc.Pending); st != statusActive {
		return wNamespace{}, conflict(resNamespace, in.Name, "Rule groups namespace %s is %s", in.Name, st)
	}

	n.Doc.Data = in.Data
	n.Doc.ModifiedAt = b.now()
	n.Doc.Pending = b.pend(statusUpdating)

	return b.wireNamespace(n, false), nil
}

// DescribeRuleGroupsNamespace returns a namespace with its data.
func (b *InMemoryBackend) DescribeRuleGroupsNamespace(ctx context.Context, wsID, name string) (wNamespace, error) {
	b.mu.RLock("DescribeRuleGroupsNamespace")
	defer b.mu.RUnlock()

	n, err := b.namespaceLocked(b.scopeOf(ctx), wsID, name)
	if err != nil {
		return wNamespace{}, err
	}

	return b.wireNamespace(n, true), nil
}

// ListRuleGroupsNamespaces lists namespaces, optionally by name prefix.
func (b *InMemoryBackend) ListRuleGroupsNamespaces(
	ctx context.Context, wsID, prefix, token string, maxResults int,
) (page.Page[wNamespace], error) {
	b.mu.RLock("ListRuleGroupsNamespaces")
	defer b.mu.RUnlock()

	if _, err := b.workspaceLocked(b.scopeOf(ctx), wsID); err != nil {
		return page.Page[wNamespace]{}, err
	}

	var items []*RuleGroupsNamespace

	b.namespaces.Range(func(n *RuleGroupsNamespace) bool {
		if n.WorkspaceID == wsID && !b.gone(n.Doc.Pending) && hasPrefixFold(n.Name, prefix) {
			items = append(items, n)
		}

		return true
	})

	slices.SortFunc(items, func(x, y *RuleGroupsNamespace) int { return cmp.Compare(x.Name, y.Name) })

	pg, err := restroute.Paginate(items, token, maxResults, listDefault, listMax)
	if err != nil {
		return page.Page[wNamespace]{}, err
	}

	out := page.Page[wNamespace]{Next: pg.Next, Data: make([]wNamespace, 0, len(pg.Data))}
	for _, n := range pg.Data {
		out.Data = append(out.Data, b.wireNamespace(n, false))
	}

	return out, nil
}

// DeleteRuleGroupsNamespace starts namespace deletion.
func (b *InMemoryBackend) DeleteRuleGroupsNamespace(ctx context.Context, wsID, name string) error {
	sc := b.scopeOf(ctx)

	b.mu.Lock("DeleteRuleGroupsNamespace")
	defer b.mu.Unlock()

	b.purgeLocked()

	if _, err := b.activeWorkspaceLocked(sc, wsID); err != nil {
		return err
	}

	n, err := b.namespaceLocked(sc, wsID, name)
	if err != nil {
		return err
	}

	n.Doc.Pending = b.pend(statusDeleting)

	return nil
}
