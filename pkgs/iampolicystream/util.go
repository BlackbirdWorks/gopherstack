package iampolicystream

import (
	"iter"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

func equalFold(a, b string) bool { return strs.Equal(a, b) }

func mapKeys[V any](m map[string]V) iter.Seq[string] { return maps.Keys(m) }
