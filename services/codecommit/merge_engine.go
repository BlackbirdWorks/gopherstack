package codecommit

import (
	"bytes"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"
)

const (
	detailFileLevel = "FILE_LEVEL"
	detailLineLevel = "LINE_LEVEL"

	strategyNone         = "NONE"
	strategyAcceptSource = "ACCEPT_SOURCE"
	strategyAcceptDest   = "ACCEPT_DESTINATION"
	strategyAutomerge    = "AUTOMERGE"

	changeAdded    = "A"
	changeModified = "M"
	changeDeleted  = "D"

	objectTypeFile      = "FILE"
	objectTypeDirectory = "DIRECTORY"

	pickSource = "src"
	pickDest   = "dst"
)

// MergeSettings carries the conflict handling inputs shared by the merge operations.
type MergeSettings struct {
	Resolution       *ConflictResolution
	DetailLevel      string
	Strategy         string
	KeepEmptyFolders bool
}

// ConflictResolution is the caller's explicit resolution of conflicting files.
type ConflictResolution struct {
	DeleteFiles     []string
	ReplaceContents []ReplaceContentEntry
	SetFileModes    []SetFileModeEntry
}

// ReplaceContentEntry replaces a conflicting file's content.
type ReplaceContentEntry struct {
	FilePath        string
	ReplacementType string
	FileMode        string
	Content         []byte
}

// SetFileModeEntry sets a conflicting file's mode.
type SetFileModeEntry struct {
	FilePath string
	FileMode string
}

// side is one version of a file; absent is a missing file.
type side struct {
	mode    string
	blobID  string
	content []byte
	absent  bool
}

func (s side) sameAs(o side) bool {
	if s.absent || o.absent {
		return s.absent == o.absent
	}

	return s.mode == o.mode && bytes.Equal(s.content, o.content)
}

func (s side) sameContent(o side) bool {
	if s.absent || o.absent {
		return s.absent == o.absent
	}

	return bytes.Equal(s.content, o.content)
}

// fileMerge is the merge outcome of one path.
type fileMerge struct {
	path     string
	result   side
	hunks    []MergeHunk
	meta     ConflictMetadata
	conflict bool
}

// mergeEvaluation is the full outcome of merging source into destination.
type mergeEvaluation struct {
	byPath     map[string]*fileMerge
	tree       map[string]TreeEntry
	newBlobs   map[string][]byte
	baseID     string
	unresolved []*fileMerge
}

func (b *InMemoryBackend) sideOf(repo string, tree map[string]TreeEntry, path string) side {
	e, ok := tree[path]
	if !ok {
		return side{absent: true}
	}

	content, _ := b.blobContentLocked(repo, e.BlobID)

	return side{content: content, mode: e.Mode, blobID: e.BlobID}
}

func unionPaths(trees ...map[string]TreeEntry) []string {
	set := map[string]struct{}{}

	for _, t := range trees {
		for p := range t {
			set[p] = struct{}{}
		}
	}

	return slices.Sorted(maps.Keys(set))
}

// evaluateMergeLocked merges source into destination against their merge base.
func (b *InMemoryBackend) evaluateMergeLocked(repo, srcID, dstID string, s MergeSettings) *mergeEvaluation {
	baseID := b.mergeBase(repo, srcID, dstID)
	baseTree := map[string]TreeEntry{}

	if baseID != "" {
		baseTree = b.treeOfLocked(repo, baseID)
	}

	srcTree, dstTree := b.treeOfLocked(repo, srcID), b.treeOfLocked(repo, dstID)
	ev := &mergeEvaluation{
		baseID:   baseID,
		byPath:   map[string]*fileMerge{},
		tree:     map[string]TreeEntry{},
		newBlobs: map[string][]byte{},
	}
	rs := newResolver(s)

	for _, path := range unionPaths(baseTree, srcTree, dstTree) {
		fm := mergeFile(
			path, b.sideOf(repo, baseTree, path), b.sideOf(repo, srcTree, path), b.sideOf(repo, dstTree, path), rs,
		)
		ev.byPath[path] = fm

		if fm.conflict {
			ev.unresolved = append(ev.unresolved, fm)
		}

		ev.place(fm)
	}

	ev.flagObjectTypeConflicts(baseTree, srcTree, dstTree)

	if s.KeepEmptyFolders {
		ev.keepEmptyFolders(srcTree, dstTree)
	}

	return ev
}

// place records fm's resolved result in the merged tree.
func (ev *mergeEvaluation) place(fm *fileMerge) {
	if fm.result.absent {
		return
	}

	blobID := fm.result.blobID
	if blobID == "" {
		blobID = uuid.NewString()
		ev.newBlobs[blobID] = fm.result.content
	}

	ev.tree[fm.path] = TreeEntry{BlobID: blobID, Mode: fm.result.mode}
}

// flagObjectTypeConflicts marks a file whose path is also a folder in the result.
func (ev *mergeEvaluation) flagObjectTypeConflicts(base, src, dst map[string]TreeEntry) {
	dirs := map[string]bool{}

	for path := range ev.tree {
		for d := parentDir(path); d != "" && !dirs[d]; d = parentDir(d) {
			dirs[d] = true
		}
	}

	for path := range ev.tree {
		fm := ev.byPath[path]
		if !dirs[path] || fm == nil || fm.meta.ObjectTypeConflict {
			continue
		}

		if !fm.conflict {
			ev.unresolved = append(ev.unresolved, fm)
		}

		fm.conflict = true
		fm.meta.ObjectTypeConflict = true
		fm.meta.NumberOfConflicts++
		fm.meta.ObjectTypes = &ObjectTypes{
			Source: objectTypeIn(src, path), Destination: objectTypeIn(dst, path), Base: objectTypeIn(base, path),
		}
	}
}

func objectTypeIn(tree map[string]TreeEntry, path string) string {
	if _, ok := tree[path]; ok {
		return objectTypeFile
	}

	if treeHasUnder(tree, path) {
		return objectTypeDirectory
	}

	return ""
}

// keepEmptyFolders adds a .gitkeep for each folder the merge emptied.
func (ev *mergeEvaluation) keepEmptyFolders(srcTree, dstTree map[string]TreeEntry) {
	seen := map[string]bool{}

	for _, path := range unionPaths(srcTree, dstTree) {
		if _, kept := ev.tree[path]; kept {
			continue
		}

		folder := parentFolder(path)
		if folder == "" || seen[folder] {
			continue
		}

		seen[folder] = true

		if treeHasUnder(ev.tree, folder) {
			continue
		}

		blobID := uuid.NewString()
		ev.newBlobs[blobID] = []byte{}
		ev.tree[folder+"/"+gitkeepFileName] = TreeEntry{BlobID: blobID, Mode: fileModeDefault}
	}
}

// sortedUnresolved returns the unresolved conflicts ordered by path.
func (ev *mergeEvaluation) sortedUnresolved() []*fileMerge {
	out := slices.Clone(ev.unresolved)
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })

	return out
}

func modeOrDefault(m string) string {
	if m == "" {
		return fileModeDefault
	}

	return m
}

func changeOp(base, other side) string {
	switch {
	case other.absent:
		return changeDeleted
	case base.absent:
		return changeAdded
	default:
		return changeModified
	}
}

func sideType(s side) string {
	if s.absent {
		return ""
	}

	return objectTypeFile
}

func sizeOf(s side) int64 {
	if s.absent {
		return 0
	}

	return int64(len(s.content))
}

func baseMetadata(path string, base, src, dst side) ConflictMetadata {
	return ConflictMetadata{
		FilePath:  path,
		FileSizes: &FileSizes{Source: sizeOf(src), Destination: sizeOf(dst), Base: sizeOf(base)},
		FileModes: &FileModes{Source: src.mode, Destination: dst.mode, Base: base.mode},
		IsBinaryFile: FileBinaryStatus{
			Source:      isBinaryContent(src.content),
			Destination: isBinaryContent(dst.content),
			Base:        isBinaryContent(base.content),
		},
		ObjectTypes: &ObjectTypes{Source: sideType(src), Destination: sideType(dst), Base: sideType(base)},
		MergeOperations: &MergeOperations{
			Source: changeOp(base, src), Destination: changeOp(base, dst),
		},
	}
}

// mergeFile three-way merges one path.
func mergeFile(path string, base, src, dst side, rs *resolver) *fileMerge {
	fm := &fileMerge{path: path, meta: baseMetadata(path, base, src, dst)}

	switch {
	case src.sameAs(dst), base.sameAs(src):
		fm.result = dst
		if src.sameAs(dst) {
			fm.result = src
		}
	case base.sameAs(dst):
		fm.result = src
	default:
		mergeBothChanged(fm, base, src, dst, rs)
	}

	return fm
}

func mergeBothChanged(fm *fileMerge, base, src, dst side, rs *resolver) {
	if src.absent || dst.absent {
		fm.meta.ContentConflict = true
		fm.meta.NumberOfConflicts = 1
		fm.conflict = true
		fm.result = dst

		rs.resolve(fm, base, src, dst)

		return
	}

	mode, modeConflict := mergeMode(base, src, dst)
	content, contentConflict := mergeContent(fm, base, src, dst, rs)
	fm.result = side{content: content, mode: mode, blobID: reuseBlob(content, src, dst)}
	fm.meta.FileModeConflict = modeConflict

	if modeConflict {
		fm.meta.NumberOfConflicts++
	}

	fm.conflict = modeConflict || contentConflict
	if fm.conflict {
		rs.resolve(fm, base, src, dst)
	}
}

func reuseBlob(content []byte, src, dst side) string {
	switch {
	case bytes.Equal(content, src.content):
		return src.blobID
	case bytes.Equal(content, dst.content):
		return dst.blobID
	default:
		return ""
	}
}

func mergeMode(base, src, dst side) (string, bool) {
	switch {
	case src.mode == dst.mode:
		return src.mode, false
	case !base.absent && base.mode == src.mode:
		return dst.mode, false
	case !base.absent && base.mode == dst.mode:
		return src.mode, false
	default:
		return dst.mode, true
	}
}

// mergeContent merges file content; an unresolved conflict reports true and
// leaves the destination content in place.
func mergeContent(fm *fileMerge, base, src, dst side, rs *resolver) ([]byte, bool) {
	switch {
	case src.sameContent(dst):
		return src.content, false
	case !base.absent && bytes.Equal(base.content, src.content):
		return dst.content, false
	case !base.absent && bytes.Equal(base.content, dst.content):
		return src.content, false
	}

	if rs.lineLevel && !isBinaryContent(src.content) && !isBinaryContent(dst.content) &&
		!isBinaryContent(base.content) {
		return mergeLineLevel(fm, base, src, dst, rs)
	}

	fm.meta.ContentConflict = true
	fm.meta.NumberOfConflicts++
	fm.hunks = []MergeHunk{wholeFileHunk(base, src, dst)}

	return dst.content, true
}

func mergeLineLevel(fm *fileMerge, base, src, dst side, rs *resolver) ([]byte, bool) {
	pick := rs.pickFor()
	lm := threeWayLines(splitLines(base.content), splitLines(src.content), splitLines(dst.content), pick)
	fm.hunks = lm.hunks

	if lm.conflicts > 0 {
		fm.meta.ContentConflict = true
		fm.meta.NumberOfConflicts += lm.conflicts

		return dst.content, true
	}

	return []byte(strings.Join(lm.merged, "")), false
}

func wholeFileHunk(base, src, dst side) MergeHunk {
	detail := func(s side) *MergeHunkDetail { return hunkDetail(0, splitLines(s.content)) }

	return MergeHunk{Source: detail(src), Destination: detail(dst), Base: detail(base), IsConflict: true}
}
