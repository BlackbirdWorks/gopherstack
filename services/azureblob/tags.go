package azureblob

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

const (
	maxBlobTags      = 10
	maxTagKeyLen     = 128
	maxTagValueLen   = 256
	maxTagsHeaderLen = 2048
	maxTagPredicate  = 10
)

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type tagSetXML struct {
	Tag []tagXML `xml:"Tag"`
}

// blobTagsXML is the Set/Get Blob Tags body and the Tags element of a Find
// Blobs by Tags item.
type blobTagsXML struct {
	XMLName xml.Name  `xml:"Tags"`
	TagSet  tagSetXML `xml:"TagSet"`
}

type filterBlobXML struct {
	Tags          blobTagsXML `xml:"Tags"`
	Name          string      `xml:"Name"`
	ContainerName string      `xml:"ContainerName"`
}

type filterBlobsXML struct {
	XMLName         xml.Name        `xml:"EnumerationResults"`
	ServiceEndpoint string          `xml:"ServiceEndpoint,attr"`
	Where           string          `xml:"Where"`
	NextMarker      string          `xml:"NextMarker"`
	Blobs           []filterBlobXML `xml:"Blobs>Blob"`
}

func tagsToXML(tags map[string]string) blobTagsXML {
	out := blobTagsXML{TagSet: tagSetXML{Tag: make([]tagXML, 0, len(tags))}}

	for _, k := range sortedKeys(tags) {
		out.TagSet.Tag = append(out.TagSet.Tag, tagXML{Key: k, Value: tags[k]})
	}

	return out
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

func tagCharOK(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	default:
		return strings.ContainsRune(" +-./:=_", r)
	}
}

func tagStringOK(s string) bool {
	for _, r := range s {
		if !tagCharOK(r) {
			return false
		}
	}

	return true
}

func errInvalidTag() *apiError {
	return &apiError{
		status: http.StatusBadRequest, code: "InvalidTag",
		message: "The tags specified are invalid. It contains characters that are not permitted.",
	}
}

func errTagsTooLarge() *apiError {
	return &apiError{
		status: http.StatusBadRequest, code: "TagsTooLarge",
		message: "The tags specified exceed the maximum permissible limit.",
	}
}

// validateTags enforces the Set Blob Tags limits: at most 10 tags, keys of
// 1-128 and values of 0-256 characters from the permitted set.
func validateTags(tags map[string]string) *apiError {
	if len(tags) > maxBlobTags {
		return errTagsTooLarge()
	}

	for k, v := range tags {
		if k == "" || len([]rune(k)) > maxTagKeyLen || len([]rune(v)) > maxTagValueLen ||
			!tagStringOK(k) || !tagStringOK(v) {
			return errInvalidTag()
		}
	}

	return nil
}

// tagsFromHeader parses the query-string encoded x-ms-tags header; nil when
// absent.
func tagsFromHeader(r *http.Request) (map[string]string, *apiError) {
	raw := r.Header.Get("X-Ms-Tags")
	if raw == "" {
		return nil, nil
	}

	if len(raw) > maxTagsHeaderLen {
		return nil, errTagsTooLarge()
	}

	vals, err := url.ParseQuery(raw)
	if err != nil {
		return nil, errInvalidTag()
	}

	tags := make(map[string]string, len(vals))

	for k, v := range vals {
		if len(v) != 1 {
			return nil, errInvalidTag()
		}

		tags[k] = v[0]
	}

	if apiErr := validateTags(tags); apiErr != nil {
		return nil, apiErr
	}

	return tags, nil
}

// tagsFromBody parses a Set Blob Tags request body.
func tagsFromBody(body []byte) (map[string]string, *apiError) {
	var doc blobTagsXML
	if err := xml.Unmarshal(body, &doc); err != nil {
		return nil, &apiError{
			status: http.StatusBadRequest, code: "InvalidXmlDocument",
			message: "XML specified is not syntactically valid.",
		}
	}

	if len(doc.TagSet.Tag) > maxBlobTags {
		return nil, errTagsTooLarge()
	}

	tags := make(map[string]string, len(doc.TagSet.Tag))

	for _, t := range doc.TagSet.Tag {
		if _, dup := tags[t.Key]; dup {
			return nil, errInvalidTag()
		}

		tags[t.Key] = t.Value
	}

	if apiErr := validateTags(tags); apiErr != nil {
		return nil, apiErr
	}

	return tags, nil
}
