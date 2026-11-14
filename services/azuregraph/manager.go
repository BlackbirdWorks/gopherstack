package azuregraph

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (b *Backend) userByRef(id string) (*User, error) {
	u, ok := b.users.get(lc(id))
	if !ok {
		return nil, notFound(id)
	}

	return u, nil
}

// SetManager assigns managerID as the manager of the user; only users can be managers and chains cannot loop.
func (b *Backend) SetManager(id, managerID string) error {
	b.mu.Lock("SetManager")
	defer b.mu.Unlock()

	u, err := b.userByRef(id)
	if err != nil {
		return err
	}

	managerID = lc(managerID)
	if _, ok := b.lookup(managerID); !ok {
		return notFound(managerID)
	}

	m, ok := b.users.get(managerID)
	if !ok {
		return badRequest("Invalid object type for the manager relationship.")
	}

	for cur := m.ID; cur != ""; cur = b.managers[lc(cur)] {
		if strings.EqualFold(cur, u.ID) {
			return badRequest("A circular reference was detected in the manager chain.")
		}
	}

	b.managers[lc(u.ID)] = lc(m.ID)

	return nil
}

// RemoveManager clears the user's manager.
func (b *Backend) RemoveManager(id string) error {
	b.mu.Lock("RemoveManager")
	defer b.mu.Unlock()

	u, err := b.userByRef(id)
	if err != nil {
		return err
	}

	delete(b.managers, lc(u.ID))

	return nil
}

func managerNotFound() *Error { return notFound(propManager) }

// Manager returns the user's manager, or a 404 when none is assigned.
func (b *Backend) Manager(id string) (User, error) {
	b.mu.RLock("Manager")
	defer b.mu.RUnlock()

	return b.managerOf(id)
}

func (b *Backend) managerOf(id string) (User, error) {
	u, err := b.userByRef(id)
	if err != nil {
		return User{}, err
	}

	m, ok := b.users.get(b.managers[lc(u.ID)])
	if !ok {
		return User{}, managerNotFound()
	}

	return *m, nil
}

// DirectReports lists the users whose manager is the given user.
func (b *Backend) DirectReports(id string) ([]Object, error) {
	b.mu.RLock("DirectReports")
	defer b.mu.RUnlock()

	u, err := b.userByRef(id)
	if err != nil {
		return nil, err
	}

	var out []Object

	for _, r := range b.users.all() {
		if b.managers[lc(r.ID)] == lc(u.ID) {
			out = append(out, Object{Kind: kindUser, Entity: *r})
		}
	}

	return out, nil
}

func (b *Backend) dropManagerRefs(id string) {
	delete(b.managers, id)

	for k, v := range b.managers {
		if v == id {
			delete(b.managers, k)
		}
	}
}

func (h *Handler) serveManager(c *echo.Context, id string, tail []string) error {
	r, b := c.Request(), h.Backend

	switch {
	case len(tail) == 0 && r.Method == http.MethodGet:
		m, err := b.Manager(id)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.json(c, http.StatusOK, withContext(c, "directoryObjects/$entity", typed(kindUser, m)))
	case len(tail) == 1 && tail[0] == segRef:
		return h.managerRef(c, id)
	}

	return h.writeErr(c, segmentErr(strings.Join(tail, "/")))
}

func (h *Handler) managerRef(c *echo.Context, id string) error {
	r, b := c.Request(), h.Backend

	switch r.Method {
	case http.MethodGet:
		m, err := b.Manager(id)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.json(c, http.StatusOK, map[string]any{
			keyODataContext: odataBase(r) + "/$metadata#$ref",
			"@odata.id":     odataBase(r) + "/directoryObjects/" + m.ID,
		})
	case http.MethodPut:
		body, err := readJSON(c)
		if err != nil {
			return h.writeErr(c, err)
		}

		target, ok := odataRefTarget(body)
		if !ok {
			return h.writeErr(c, badRequest("A value is required for property '@odata.id'."))
		}

		return h.noContent(c, b.SetManager(id, target))
	case http.MethodDelete:
		return h.noContent(c, b.RemoveManager(id))
	}

	return h.methodNotAllowed(c)
}

// expandManager renders manager (optionally the chain via $levels) onto each expanded user.
func (h *Handler) expandManager(q *listQuery, e expandSpec, kind string, items []map[string]any) error {
	if kind != kindUser {
		return badRequest("Property '%s' on entity type 'microsoft.graph.%s' is not a valid expandable "+
			"navigation property.", e.name, kind)
	}

	if e.levels != 0 && (!q.eventual || !q.single) {
		return unsupported("The $levels option of $expand requires a single user and the " +
			"'ConsistencyLevel: eventual' header.")
	}

	depth := max(e.levels, 1)

	for _, it := range items {
		id, _ := it["id"].(string)
		h.Backend.chain(e, depth, id, it)
	}

	return nil
}

func (b *Backend) chain(e expandSpec, depth int, id string, into map[string]any) {
	b.mu.RLock("chain")
	defer b.mu.RUnlock()

	sub := &listQuery{selects: e.selects}
	cur := into

	for i := 0; e.levels == levelsMax || i < depth; i++ {
		m, err := b.managerOf(id)
		if err != nil {
			return
		}

		next := sub.project(typed(kindUser, m))
		cur[e.name] = next
		cur, id = next, m.ID
	}
}
