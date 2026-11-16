package azurefile

import (
	"net/http"
	"strings"
)

func errChangedSinceSnapshot() *apiError {
	return newErr(http.StatusConflict, "FileChangedSincePreviousSnapshot",
		"The file was deleted, recreated, copied over or moved since the previous share snapshot.")
}

// prevSnapshotNode finds n's counterpart in the previous share snapshot by file id.
func (h *Handler) prevSnapshotNode(rc *reqCtx, n *node, prev string) (*node, *apiError) {
	ps, aerr := h.Backend.peek(rc.acct).findShare(rc.share, prev, false)
	if aerr != nil {
		return nil, aerr
	}

	renameOK := strings.EqualFold(rc.hdr("x-ms-file-support-rename"), "true")

	if atPath, perr := ps.lookup(rc.path); perr == nil && !atPath.Dir {
		if atPath.ID != n.ID || !atPath.Born.Equal(n.Born) {
			return nil, errChangedSinceSnapshot()
		}

		return atPath, nil
	}

	moved := ps.Root.findFileID(n.ID)
	if moved == nil {
		return nil, errResourceNotFound()
	}

	if !renameOK || !moved.Born.Equal(n.Born) {
		return nil, errChangedSinceSnapshot()
	}

	return moved, nil
}

func (n *node) findFileID(id string) *node {
	var found *node

	n.eachFileID(id, func(o *node) {
		if found == nil {
			found = o
		}
	})

	return found
}
