package azurearm

import (
	"sync"

	"github.com/gorilla/websocket"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// ReadACITreeForTest exposes readACITree with caller-chosen bounds.
func ReadACITreeForTest(root string, maxFiles int, maxBytes int64) (ACIShareTree, error) {
	return readACITree(root, maxFiles, maxBytes)
}

// WriteACIDirForTest exposes writeACIDir for an empty directory.
func WriteACIDirForTest(dir string, tree ACIShareTree) error {
	return writeACIDir(dir, tree, ACIShareTree{})
}

// WriteWSFrameForTest writes one exec output frame through the session writer.
func WriteWSFrameForTest(conn *websocket.Conn, b []byte) error {
	_, err := wsWriter{conn}.Write(b)

	return err
}

// SyncMountsForTest runs engine-level share syncs for one run with the given mounts, as the loop and container
// exit handlers do.
func SyncMountsForTest(shares ACIFileShares, dir, account, share string, passes int) {
	run := &aciRun{syncMu: lockmetrics.New("test"), shares: []shareMount{{
		dir: dir, account: account, share: share, base: newShareBase(ACIShareTree{}),
	}}}
	e := &ACIEngine{mu: lockmetrics.New("test"), shares: shares}

	var wg sync.WaitGroup

	for range passes {
		wg.Go(func() { e.syncShares(run) })
	}

	wg.Wait()
}
