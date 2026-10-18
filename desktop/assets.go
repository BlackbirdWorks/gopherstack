package main

import (
	"io/fs"
)

func loadingFS() fs.FS {
	sub, err := fs.Sub(loading, "loading")
	if err != nil {
		panic(err)
	}

	return sub
}
