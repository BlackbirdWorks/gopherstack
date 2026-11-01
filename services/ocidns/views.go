package ocidns

import (
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// ViewWire is the wire form of a view.
type ViewWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	FreeformTags   map[string]string         `json:"freeformTags"`
	DefinedTags    map[string]map[string]any `json:"definedTags"`
	CompartmentID  string                    `json:"compartmentId"`
	DisplayName    string                    `json:"displayName"`
	ID             string                    `json:"id"`
	Self           string                    `json:"self"`
	LifecycleState string                    `json:"lifecycleState"`
	IsProtected    bool                      `json:"isProtected"`
}

func (b *InMemoryBackend) viewWire(v *View) ViewWire {
	return ViewWire{
		TimeCreated: v.Created, TimeUpdated: v.Updated, FreeformTags: v.free(), DefinedTags: v.defined(),
		CompartmentID: v.CompartmentID, DisplayName: v.DisplayName, ID: v.ID, Self: b.self("views", v.ID),
		LifecycleState: stateActive, IsProtected: v.Protected,
	}
}

func (b *InMemoryBackend) viewByID(id string) (*View, *APIError) {
	v, ok := b.views[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return v, nil
}

// CreateViewIn is the body of CreateView.
type CreateViewIn struct {
	Tags
	CompartmentID string `json:"compartmentId"`
	DisplayName   string `json:"displayName"`
}

func (b *InMemoryBackend) newView(compartment, name string, protected bool, tags Tags) *View {
	now := b.nowUTC()
	v := &View{
		ID: newOCID("dnsview"), CompartmentID: compartment, DisplayName: name, Created: now, Updated: now,
		Protected: protected, Seq: b.nextSeq(), Tags: tags,
	}
	v.touch(now)
	b.views[v.ID] = v

	return v
}

// CreateView creates a private view.
func (b *InMemoryBackend) CreateView(in CreateViewIn) (ViewWire, tag, error) {
	b.mu.Lock("CreateView")
	defer b.mu.Unlock()

	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return ViewWire{}, tag{}, err
	}

	name := in.DisplayName
	if name == "" {
		name = "view" + b.nowUTC().Format("20060102150405")
	}

	v := b.newView(in.CompartmentID, name, false, in.Tags)

	return b.viewWire(v), tagOf(v.ID, v.meta), nil
}

// GetView returns a view.
func (b *InMemoryBackend) GetView(id string) (ViewWire, tag, error) {
	b.mu.RLock("GetView")
	defer b.mu.RUnlock()

	v, err := b.viewByID(id)
	if err != nil {
		return ViewWire{}, tag{}, err
	}

	return b.viewWire(v), tagOf(v.ID, v.meta), nil
}

// ViewFilter selects views in a list.
type ViewFilter struct {
	Compart string
	Name    string
	ID      string
	State   string
	Scope   string
}

var viewSorters = sorter[*View]{ //nolint:gochecknoglobals // immutable comparator table
	keyDisplayName: byString(func(v *View) string { return v.DisplayName }, func(v *View) int64 { return v.Seq }),
	keyTimeCreated: byCreated(func(v *View) time.Time { return v.Created }, func(v *View) int64 { return v.Seq }),
}

// ListViews lists the views of a compartment.
func (b *InMemoryBackend) ListViews(f ViewFilter, o ListOpts) (listResult[ViewWire], error) {
	b.mu.RLock("ListViews")
	defer b.mu.RUnlock()

	if err := b.checkCompartment(f.Compart); err != nil {
		return listResult[ViewWire]{}, err
	}

	if f.Scope != "" && !strings.EqualFold(f.Scope, scopePrivate) {
		return listResult[ViewWire]{Items: []ViewWire{}}, nil
	}

	var all []*View

	for _, v := range b.views {
		if v.CompartmentID == f.Compart && (f.Name == "" || strs.Equal(v.DisplayName, f.Name)) &&
			(f.ID == "" || v.ID == f.ID) && (f.State == "" || strings.EqualFold(f.State, stateActive)) {
			all = append(all, v)
		}
	}

	pg, err := sortPage(all, o, keyTimeCreated, true, viewSorters)
	if err != nil {
		return listResult[ViewWire]{}, err
	}

	return finish(pg, len(all), b.viewWire), nil
}

// UpdateViewIn is the body of UpdateView.
type UpdateViewIn struct {
	Tags
	DisplayName *string `json:"displayName"`
}

// UpdateView changes the mutable attributes of a view.
func (b *InMemoryBackend) UpdateView(id string, c cond, in UpdateViewIn) (ViewWire, tag, error) {
	b.mu.Lock("UpdateView")
	defer b.mu.Unlock()

	v, err := b.viewByID(id)
	if err != nil {
		return ViewWire{}, tag{}, err
	}

	if err = c.check(v.ID, v.meta); err != nil {
		return ViewWire{}, tag{}, err
	}

	if v.Protected {
		return ViewWire{}, tag{}, errParam("a protected view cannot be modified")
	}

	if in.DisplayName != nil {
		v.DisplayName = *in.DisplayName
	}

	v.Tags.merge(in.Tags)
	now := b.nowUTC()
	v.Updated = now
	v.touch(now)

	return b.viewWire(v), tagOf(v.ID, v.meta), nil
}

// DeleteView deletes a view that no zone or resolver references.
func (b *InMemoryBackend) DeleteView(id string, c cond) error {
	b.mu.Lock("DeleteView")
	defer b.mu.Unlock()

	v, err := b.viewByID(id)
	if err != nil {
		return err
	}

	if err = c.check(v.ID, v.meta); err != nil {
		return err
	}

	if v.Protected {
		return errParam("a protected view cannot be deleted")
	}

	for _, z := range b.zones {
		if z.ViewID == id {
			return errConflict("The view is referenced by zone " + z.Name)
		}
	}

	for _, r := range b.resolvers {
		if r.DefaultViewID == id || containsStr(r.AttachedViews, id) {
			return errConflict("The view is referenced by resolver " + r.ID)
		}
	}

	delete(b.views, id)

	return nil
}

// ChangeViewCompartment moves a view.
func (b *InMemoryBackend) ChangeViewCompartment(id string, c cond, in ChangeCompartmentIn) error {
	b.mu.Lock("ChangeViewCompartment")
	defer b.mu.Unlock()

	v, err := b.viewByID(id)
	if err != nil {
		return err
	}

	if err = c.check(v.ID, v.meta); err != nil {
		return err
	}

	if v.Protected {
		return errParam("a protected view cannot be moved")
	}

	if err = b.checkCompartment(in.CompartmentID); err != nil {
		return err
	}

	v.CompartmentID = in.CompartmentID
	v.touch(b.nowUTC())

	return nil
}

func containsStr(list []string, s string) bool {
	return slices.Contains(list, s)
}
