package iam

import (
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// pageForm applies the request's Marker/MaxItems to an already-ordered slice.
func pageForm[T any](items []T, vals url.Values) page.Page[T] {
	return page.New(items, vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")), iamDefaultMaxItems)
}
