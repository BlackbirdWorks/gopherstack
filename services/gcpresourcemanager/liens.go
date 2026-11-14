package gcpresourcemanager

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	v1Liens         = "/v1/liens"
	kindLiens       = "liens"
	restrictDelete  = "resourcemanager.projects.delete"
	maxLienText     = 200
	maxLiensPerProj = 1000
)

type lienRec struct {
	Created      time.Time `json:"created"`
	Name         string    `json:"name"`
	Parent       string    `json:"parent"`
	Origin       string    `json:"origin,omitempty"`
	Reason       string    `json:"reason,omitempty"`
	Restrictions []string  `json:"restrictions"`
}

// v1Lien is the cloudresourcemanager v1 Lien resource.
type v1Lien struct {
	Name         string   `json:"name,omitempty"`
	Parent       string   `json:"parent,omitempty"`
	Origin       string   `json:"origin,omitempty"`
	Reason       string   `json:"reason,omitempty"`
	CreateTime   string   `json:"createTime,omitempty"`
	Restrictions []string `json:"restrictions,omitempty"`
}

func (l *lienRec) view() v1Lien {
	return v1Lien{
		Name: l.Name, Parent: l.Parent, Restrictions: slices.Clone(l.Restrictions), Origin: l.Origin,
		Reason: l.Reason, CreateTime: l.Created.UTC().Format(time.RFC3339Nano),
	}
}

func lienID(name string) string { return strings.TrimPrefix(name, kindLiens+"/") }

// CreateLien attaches a lien to an active project.
func (b *InMemoryBackend) CreateLien(in v1Lien) (v1Lien, error) {
	if len(in.Restrictions) == 0 {
		return v1Lien{}, errInvalid("restrictions must not be empty.")
	}

	for _, r := range in.Restrictions {
		if r != restrictDelete {
			return v1Lien{}, errInvalid("Restriction %q is not supported; liens support %s.", r, restrictDelete)
		}
	}

	if len(in.Origin) > maxLienText || len(in.Reason) > maxLienText {
		return v1Lien{}, errInvalid("origin and reason must be at most %d characters.", maxLienText)
	}

	if !strings.HasPrefix(in.Parent, kindProjects+"/") {
		return v1Lien{}, errInvalid("parent must be a project, for example projects/1234.")
	}

	b.mu.Lock("CreateLien")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.activeProjectLocked(in.Parent, "resourcemanager.projects.updateLiens")
	if err != nil {
		return v1Lien{}, err
	}

	parent := kindProjects + "/" + p.Number
	if len(b.projectLiensLocked(parent)) >= maxLiensPerProj {
		return v1Lien{}, errPrecondition("Project %s already has the maximum of %d liens.", p.ID, maxLiensPerProj)
	}

	rec := &lienRec{
		Name:   kindLiens + "/p" + p.Number + "-l" + uuid.NewString(),
		Parent: parent, Origin: in.Origin, Reason: in.Reason, Created: b.now(),
		Restrictions: slices.Clone(slices.Compact(slices.Sorted(slices.Values(in.Restrictions)))),
	}
	b.liens[lienID(rec.Name)] = rec

	return rec.view(), nil
}

func (b *InMemoryBackend) projectLiensLocked(parent string) []*lienRec {
	var out []*lienRec

	for _, l := range b.liens {
		if l.Parent == parent {
			out = append(out, l)
		}
	}

	slices.SortFunc(out, func(x, y *lienRec) int { return strings.Compare(x.Name, y.Name) })

	return out
}

// GetLien returns a lien by name.
func (b *InMemoryBackend) GetLien(name string) (v1Lien, error) {
	b.mu.Lock("GetLien")
	defer b.mu.Unlock()

	b.purgeLocked()

	l, ok := b.liens[lienID(name)]
	if !ok {
		return v1Lien{}, errDenied("resourcemanager.projects.get", name)
	}

	return l.view(), nil
}

// ListLiens lists the liens of one project.
func (b *InMemoryBackend) ListLiens(parent, token string, size int) (page.Page[v1Lien], error) {
	var none page.Page[v1Lien]

	if !strings.HasPrefix(parent, kindProjects+"/") {
		return none, errInvalid("parent must be a project, for example projects/1234.")
	}

	b.mu.Lock("ListLiens")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.projectLocked(parent, "resourcemanager.projects.get")
	if err != nil {
		return none, err
	}

	var views []v1Lien
	for _, l := range b.projectLiensLocked(kindProjects + "/" + p.Number) {
		views = append(views, l.view())
	}

	return page.New(views, token, size, v1Page), nil
}

// DeleteLien removes a lien.
func (b *InMemoryBackend) DeleteLien(name string) error {
	b.mu.Lock("DeleteLien")
	defer b.mu.Unlock()

	b.purgeLocked()

	id := lienID(name)
	if _, ok := b.liens[id]; !ok {
		return errDenied("resourcemanager.projects.updateLiens", name)
	}

	delete(b.liens, id)

	return nil
}

// lienBlocksDeleteLocked reports a lien that forbids deleting the project.
func (b *InMemoryBackend) lienBlocksDeleteLocked(number string) bool {
	return slices.ContainsFunc(b.projectLiensLocked(kindProjects+"/"+number), func(l *lienRec) bool {
		return slices.Contains(l.Restrictions, restrictDelete)
	})
}

func (b *InMemoryBackend) dropLiensOfLocked(parent string) {
	for id, l := range b.liens {
		if l.Parent == parent {
			delete(b.liens, id)
		}
	}
}

func (h v1Handler) liens(w http.ResponseWriter, r *http.Request) error {
	name := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, v1Liens), "/")

	switch {
	case name == "" && r.Method == http.MethodPost:
		return h.createLien(w, r)
	case name == "" && r.Method == http.MethodGet:
		return h.listLiens(w, r)
	case name != "" && r.Method == http.MethodGet:
		out, err := h.b.GetLien(name)
		if err != nil {
			return err
		}

		writeV1JSON(w, http.StatusOK, out)

		return nil
	case name != "" && r.Method == http.MethodDelete:
		if err := h.b.DeleteLien(name); err != nil {
			return err
		}

		writeV1Raw(w, http.StatusOK, []byte("{}"))

		return nil
	default:
		return methodNotAllowed(r)
	}
}

func (h v1Handler) createLien(w http.ResponseWriter, r *http.Request) error {
	var in v1Lien
	if err := readBody(r, &in); err != nil {
		return err
	}

	out, err := h.b.CreateLien(in)
	if err != nil {
		return err
	}

	writeV1JSON(w, http.StatusOK, out)

	return nil
}

func (h v1Handler) listLiens(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	size, _ := strconv.Atoi(q.Get("pageSize"))

	pg, err := h.b.ListLiens(q.Get("parent"), q.Get("pageToken"), size)
	if err != nil {
		return err
	}

	resp := map[string]any{}
	if len(pg.Data) > 0 {
		resp["liens"] = pg.Data
	}

	if pg.Next != "" {
		resp["nextPageToken"] = pg.Next
	}

	writeV1JSON(w, http.StatusOK, resp)

	return nil
}
