//go:build !unix

package ocifilestorage

import "io/fs"

func ownerOf(fs.FileInfo) (int, int) { return 0, 0 }
