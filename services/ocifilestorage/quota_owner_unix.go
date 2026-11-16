//go:build unix

package ocifilestorage

import (
	"io/fs"
	"syscall"
)

func ownerOf(info fs.FileInfo) (int, int) {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return int(st.Uid), int(st.Gid)
	}

	return 0, 0
}
