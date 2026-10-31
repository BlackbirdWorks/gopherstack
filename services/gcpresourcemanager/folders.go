package gcpresourcemanager

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	permFolderGet  = "resourcemanager.folders.get"
	permFolderList = "resourcemanager.folders.list"
)

func (b *InMemoryBackend) folderLocked(name, perm string) (*folderRec, error) {
	id, ok := splitName(name, kindFolders)
	if !ok {
		return nil, errInvalid("Invalid folder name %q: use folders/ID.", name)
	}

	f, found := b.folders[id]
	if !found {
		return nil, errDenied(perm, name)
	}

	return f, nil
}

// restorableParentLocked checks that a restored resource's parent is still active.
func (b *InMemoryBackend) restorableParentLocked(parent string) error {
	if f, ok := b.folders[strings.TrimPrefix(parent, kindFolders+"/")]; strings.HasPrefix(parent, kindFolders+"/") {
		if !ok {
			return errPrecondition("The parent %s no longer exists.", parent)
		}

		if f.requested() {
			return errPrecondition("The parent %s is in the DELETE_REQUESTED state.", parent)
		}
	}

	return nil
}

// depthLocked is the number of folders on the path from the organization down to and including parent.
func (b *InMemoryBackend) depthLocked(parent string) int {
	depth := 0

	for strings.HasPrefix(parent, kindFolders+"/") && depth <= 2*maxFolderDepth {
		f, ok := b.folders[strings.TrimPrefix(parent, kindFolders+"/")]
		if !ok {
			break
		}

		depth++
		parent = f.Parent
	}

	return depth
}

// heightLocked is the height of the active folder subtree rooted at id, counting id itself.
func (b *InMemoryBackend) heightLocked(id string) int {
	height := 1

	for cid, c := range b.folders {
		if c.Parent == kindFolders+"/"+id && !c.requested() {
			height = max(height, 1+b.heightLocked(cid))
		}
	}

	return height
}

func (b *InMemoryBackend) activeChildrenLocked(parent string) ([]*folderRec, int) {
	var folders []*folderRec

	projects := 0

	for _, f := range b.folders {
		if f.Parent == parent && !f.requested() {
			folders = append(folders, f)
		}
	}

	for _, p := range b.projects {
		if p.Parent == parent && !p.requested() {
			projects++
		}
	}

	return folders, projects
}

// placeLocked checks that a folder named name with a subtree of the given height may sit under parent.
func (b *InMemoryBackend) placeLocked(parent, name, self string, height int) error {
	siblings, _ := b.activeChildrenLocked(parent)

	if len(siblings) >= maxFolderFanout &&
		!slices.ContainsFunc(siblings, func(f *folderRec) bool { return f.ID == self }) {
		return preconditionFailure("Folder fanout limit reached.", parent,
			"A parent can hold at most 300 folders.")
	}

	if slices.ContainsFunc(
		siblings,
		func(f *folderRec) bool { return f.ID != self && strs.Equal(f.DisplayName, name) },
	) {
		return preconditionFailure("Folder display name is not unique under its parent.", parent,
			"The display name "+name+" is already used by a sibling folder.")
	}

	if b.depthLocked(parent)+height > maxFolderDepth {
		return preconditionFailure("Folder height limit exceeded.", parent,
			"The active folder hierarchy cannot exceed a height of 10.")
	}

	return nil
}

// CreateFolder creates a folder under the organization or another folder.
func (b *InMemoryBackend) CreateFolder(in *resourcemanagerpb.CreateFolderRequest) (*longrunningpb.Operation, error) {
	fo := in.GetFolder()
	if fo == nil || fo.GetParent() == "" {
		return nil, errInvalid("folder.parent must be specified.")
	}

	if err := validateFolderName(fo.GetDisplayName()); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFolder")
	defer b.mu.Unlock()

	b.purgeLocked()

	if strings.HasPrefix(fo.GetParent(), kindProjects+"/") {
		return nil, errInvalid("A folder's parent must be an organization or a folder.")
	}

	if err := b.parentLocked(fo.GetParent(), "resourcemanager.folders.create"); err != nil {
		return nil, err
	}

	if err := b.placeLocked(fo.GetParent(), fo.GetDisplayName(), "", 1); err != nil {
		return nil, err
	}

	now := b.now()
	id := uniqueDigits(numberDigits, func(n string) bool {
		_, ok := b.folders[n]

		return ok
	})
	f := &folderRec{
		ID: id, Parent: fo.GetParent(), DisplayName: fo.GetDisplayName(),
		Created: now, Updated: now,
	}
	b.folders[id] = f

	return b.opLocked("cf", &resourcemanagerpb.CreateFolderMetadata{
		DisplayName: f.DisplayName, Parent: f.Parent,
	}, f.view())
}

// GetFolder returns a folder, including one that is DELETE_REQUESTED.
func (b *InMemoryBackend) GetFolder(name string) (*resourcemanagerpb.Folder, error) {
	b.mu.Lock("GetFolder")
	defer b.mu.Unlock()

	b.purgeLocked()

	f, err := b.folderLocked(name, permFolderGet)
	if err != nil {
		return nil, err
	}

	return f.view(), nil
}

func folderViews(fs []*folderRec) []*resourcemanagerpb.Folder {
	slices.SortFunc(fs, func(a, c *folderRec) int {
		return cmp.Or(cmp.Compare(a.DisplayName, c.DisplayName), cmp.Compare(a.ID, c.ID))
	})

	out := make([]*resourcemanagerpb.Folder, 0, len(fs))
	for _, f := range fs {
		out = append(out, f.view())
	}

	return out
}

// ListFolders lists the direct child folders of an organization or folder, ordered by display name.
func (b *InMemoryBackend) ListFolders(in *resourcemanagerpb.ListFoldersRequest) (
	*resourcemanagerpb.ListFoldersResponse, error,
) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if in.GetParent() == "" {
		return nil, errInvalid("parent must be specified.")
	}

	b.mu.Lock("ListFolders")
	b.purgeLocked()

	if err := b.parentLocked(in.GetParent(), permFolderList); err != nil {
		b.mu.Unlock()

		return nil, err
	}

	var hits []*folderRec

	for _, f := range b.folders {
		if f.Parent == in.GetParent() && (in.GetShowDeleted() || !f.requested()) {
			hits = append(hits, f)
		}
	}

	out := folderViews(hits)
	b.mu.Unlock()

	pg := page.New(out, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &resourcemanagerpb.ListFoldersResponse{Folders: pg.Data, NextPageToken: pg.Next}, nil
}

func (f *folderRec) fields(name string) ([]string, bool) {
	switch name {
	case fieldDisplayName:
		return []string{f.DisplayName}, true
	case "parent":
		return []string{f.Parent}, true
	case fieldState:
		if f.requested() {
			return []string{stateRequested}, true
		}

		return []string{stateActive}, true
	case "id":
		return []string{f.ID}, true
	case fieldLabels:
		return nil, true
	default:
		return nil, false
	}
}

// SearchFolders returns the folders matching the query; ACTIVE ones unless the query names a state.
func (b *InMemoryBackend) SearchFolders(in *resourcemanagerpb.SearchFoldersRequest) (
	*resourcemanagerpb.SearchFoldersResponse, error,
) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	q, err := parseQuery(in.GetQuery(), true)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("SearchFolders")
	b.purgeLocked()

	var hits []*folderRec

	for _, f := range b.folders {
		ok, mErr := q.match(f.fields)
		if mErr != nil {
			b.mu.Unlock()

			return nil, mErr
		}

		if ok && (q.hasState || !f.requested()) {
			hits = append(hits, f)
		}
	}

	out := folderViews(hits)
	b.mu.Unlock()

	pg := page.New(out, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &resourcemanagerpb.SearchFoldersResponse{Folders: pg.Data, NextPageToken: pg.Next}, nil
}

func (b *InMemoryBackend) activeFolderLocked(name, perm string) (*folderRec, error) {
	f, err := b.folderLocked(name, perm)
	if err != nil {
		return nil, err
	}

	if f.requested() {
		return nil, errPrecondition("Folder %s is in the DELETE_REQUESTED state.", name)
	}

	return f, nil
}

// UpdateFolder renames an ACTIVE folder.
func (b *InMemoryBackend) UpdateFolder(in *resourcemanagerpb.UpdateFolderRequest) (*longrunningpb.Operation, error) {
	fo := in.GetFolder()
	if fo == nil || fo.GetName() == "" {
		return nil, errInvalid("folder.name must be specified.")
	}

	if err := checkMask(in.GetUpdateMask(), "display_name"); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateFolder")
	defer b.mu.Unlock()

	b.purgeLocked()

	f, err := b.activeFolderLocked(fo.GetName(), "resourcemanager.folders.update")
	if err != nil {
		return nil, err
	}

	if et := fo.GetEtag(); et != "" && et != f.view().GetEtag() {
		return nil, errAborted(
			"The folder %s was modified since it was read; re-read it and retry (etag mismatch).",
			fo.GetName(),
		)
	}

	if maskHas(in.GetUpdateMask(), "display_name") && fo.GetDisplayName() != "" {
		if err = validateFolderName(fo.GetDisplayName()); err != nil {
			return nil, err
		}

		if err = b.placeLocked(f.Parent, fo.GetDisplayName(), f.ID, 1); err != nil {
			return nil, err
		}

		f.DisplayName = fo.GetDisplayName()
	}

	f.Updated = b.now()

	return b.opLocked("uf", &resourcemanagerpb.UpdateFolderMetadata{}, f.view())
}

func (b *InMemoryBackend) isDescendantLocked(parent, id string) bool {
	for range 2 * maxFolderDepth {
		if parent == kindFolders+"/"+id {
			return true
		}

		f, ok := b.folders[strings.TrimPrefix(parent, kindFolders+"/")]
		if !ok {
			return false
		}

		parent = f.Parent
	}

	return false
}

// MoveFolder re-parents an ACTIVE folder under the organization or another active folder.
func (b *InMemoryBackend) MoveFolder(in *resourcemanagerpb.MoveFolderRequest) (*longrunningpb.Operation, error) {
	dest := in.GetDestinationParent()
	if dest == "" || strings.HasPrefix(dest, kindProjects+"/") {
		return nil, errInvalid("destination_parent must be an organization or a folder.")
	}

	b.mu.Lock("MoveFolder")
	defer b.mu.Unlock()

	b.purgeLocked()

	f, err := b.activeFolderLocked(in.GetName(), "resourcemanager.folders.move")
	if err != nil {
		return nil, err
	}

	if err = b.parentLocked(dest, "resourcemanager.folders.move"); err != nil {
		return nil, err
	}

	if b.isDescendantLocked(dest, f.ID) {
		return nil, errPrecondition("A folder cannot be moved into itself or one of its descendants.")
	}

	if err = b.placeLocked(dest, f.DisplayName, f.ID, b.heightLocked(f.ID)); err != nil {
		return nil, err
	}

	source := f.Parent
	f.Parent, f.Updated = dest, b.now()

	return b.opLocked("mf", &resourcemanagerpb.MoveFolderMetadata{
		DisplayName: f.DisplayName, SourceParent: source, DestinationParent: dest,
	}, f.view())
}

// DeleteFolder marks an empty folder DELETE_REQUESTED; deleting a requested folder is a no-op.
func (b *InMemoryBackend) DeleteFolder(in *resourcemanagerpb.DeleteFolderRequest) (*longrunningpb.Operation, error) {
	b.mu.Lock("DeleteFolder")
	defer b.mu.Unlock()

	b.purgeLocked()

	f, err := b.folderLocked(in.GetName(), "resourcemanager.folders.delete")
	if err != nil {
		return nil, err
	}

	if !f.requested() {
		folders, projects := b.activeChildrenLocked(in.GetName())
		if len(folders) > 0 || projects > 0 {
			return nil, preconditionFailure("Folder is not empty.", in.GetName(),
				"A folder can be deleted only when it holds no ACTIVE folders or projects.")
		}

		now := b.now()
		f.Deleted, f.Updated = now, now
	}

	view := f.view()

	if b.retention <= 0 {
		delete(b.folders, f.ID)
		b.policies.Drop(in.GetName())
	}

	return b.opLocked("df", &resourcemanagerpb.DeleteFolderMetadata{}, view)
}

// UndeleteFolder restores a DELETE_REQUESTED folder; an ACTIVE folder is a no-op success.
func (b *InMemoryBackend) UndeleteFolder(in *resourcemanagerpb.UndeleteFolderRequest) (
	*longrunningpb.Operation, error,
) {
	b.mu.Lock("UndeleteFolder")
	defer b.mu.Unlock()

	b.purgeLocked()

	f, err := b.folderLocked(in.GetName(), "resourcemanager.folders.undelete")
	if err != nil {
		return nil, err
	}

	if f.requested() {
		if err = b.restorableParentLocked(f.Parent); err != nil {
			return nil, err
		}

		if err = b.placeLocked(f.Parent, f.DisplayName, f.ID, b.heightLocked(f.ID)); err != nil {
			return nil, err
		}

		f.Deleted, f.Updated = time.Time{}, b.now()
	}

	return b.opLocked("uf", &resourcemanagerpb.UndeleteFolderMetadata{}, f.view())
}
