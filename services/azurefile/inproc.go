package azurefile

import (
	"bytes"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/sparse"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Accounts lists the storage accounts that currently hold at least one share.
func (b *InMemoryBackend) Accounts() []string {
	b.mu.RLock("Accounts")
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.accounts))

	for name, a := range b.accounts {
		if len(a.Shares) > 0 {
			out = append(out, name)
		}
	}

	sort.Strings(out)

	return out
}

// ErrShareNotFound is returned when a share named by ExportShare or ApplyShare does not exist.
var ErrShareNotFound = errors.New("azurefile: share not found")

// ShareTree is a share's files (slash paths to contents) and directories.
type ShareTree struct {
	Files map[string][]byte
	Dirs  []string
}

func (b *InMemoryBackend) liveShare(accountName, shareName string) (*share, bool) {
	a := b.peek(accountName)
	if a == nil {
		return nil, false
	}

	sh, ok := a.Shares[strs.Fold(shareName)]
	if !ok || sh.Deleted || sh.SnapshotID != "" {
		return nil, false
	}

	return sh, true
}

// ExportShare returns a copy of the share's files and directories.
func (b *InMemoryBackend) ExportShare(accountName, shareName string) (ShareTree, error) {
	b.mu.RLock("ExportShare")
	defer b.mu.RUnlock()

	sh, ok := b.liveShare(accountName, shareName)
	if !ok {
		return ShareTree{}, ErrShareNotFound
	}

	tree := ShareTree{Files: map[string][]byte{}}
	walkNodes(sh.Root, "", &tree)
	sort.Strings(tree.Dirs)

	return tree, nil
}

func walkNodes(n *node, prefix string, tree *ShareTree) {
	for _, c := range n.Children {
		p := prefix + c.Name
		if c.Dir {
			tree.Dirs = append(tree.Dirs, p)
			walkNodes(c, p+"/", tree)

			continue
		}

		tree.Files[p] = c.Data.Read(0, c.Size)
	}
}

// ApplyShare makes the share's contents match tree: new files are created, changed files rewritten, and
// files and directories absent from tree removed. Unchanged files keep their properties.
func (b *InMemoryBackend) ApplyShare(accountName, shareName string, tree ShareTree) error {
	b.mu.Lock("ApplyShare")
	defer b.mu.Unlock()

	sh, ok := b.liveShare(accountName, shareName)
	if !ok {
		return ErrShareNotFound
	}

	now := time.Now().UTC()
	dirs := make(map[string]bool, len(tree.Dirs))

	for _, d := range tree.Dirs {
		dirs[d] = true
	}

	b.pruneNodes(sh.Root, "", tree.Files, dirs)

	for _, d := range tree.Dirs {
		b.ensureDir(sh.Root, strings.Split(d, "/"), now)
	}

	for p, data := range tree.Files {
		segs := strings.Split(p, "/")
		parent := b.ensureDir(sh.Root, segs[:len(segs)-1], now)
		leaf := segs[len(segs)-1]
		cur := parent.child(leaf)

		if cur != nil && !cur.Dir && bytes.Equal(cur.Data.Read(0, cur.Size), data) {
			continue
		}

		if cur == nil {
			cur = b.newNode(parent, leaf, false, now)
			parent.Children[strs.Fold(leaf)] = cur
		}

		cur.Data = sparse.Extents(nil).Write(0, data)
		cur.Size = int64(len(data))
		cur.LastWrite, cur.LastModified, cur.Changed, cur.ETag = now, now, now, b.etag()
	}

	return nil
}

func (b *InMemoryBackend) ensureDir(root *node, segs []string, now time.Time) *node {
	cur := root

	for _, s := range segs {
		next := cur.child(s)
		if next == nil || !next.Dir {
			next = b.newNode(cur, s, true, now)
			cur.Children[strs.Fold(s)] = next
		}

		cur = next
	}

	return cur
}

func (b *InMemoryBackend) pruneNodes(n *node, prefix string, files map[string][]byte, dirs map[string]bool) {
	for k, c := range n.Children {
		p := prefix + c.Name

		switch {
		case c.Dir && !dirs[p]:
			delete(n.Children, k)
		case c.Dir:
			b.pruneNodes(c, p+"/", files, dirs)
		default:
			if _, keep := files[p]; !keep {
				delete(n.Children, k)
			}
		}
	}
}
