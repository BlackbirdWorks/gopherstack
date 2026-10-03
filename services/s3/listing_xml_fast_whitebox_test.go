package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// oldEncode is the pre-optimization reflection-based path, kept only here as
// the byte-equivalence reference for listing_xml_fast.go's hand-written encoder.
func oldEncode(t *testing.T, payload any) []byte {
	t.Helper()

	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	require.NoError(t, xml.NewEncoder(&buf).Encode(payload))

	return buf.Bytes()
}

func testOwner() *Owner {
	return &Owner{ID: "gopherstack", DisplayName: "gopherstack"}
}

// invalidUTF8AndIllegalRunesKey mixes raw invalid bytes, a truncated
// multi-byte sequence, \x0B, U+FFFE, and a lone surrogate encoded as bytes.
const invalidUTF8AndIllegalRunesKey = "a\xff\xfeb\xe4\xb8c\x0bd￾e\xed\xa0\x80f"

func TestListBucketV2XML_GoldenEquivalence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		resp ListBucketV2Result
	}{
		{
			name: "empty_list",
			resp: ListBucketV2Result{Name: "b", Prefix: "", MaxKeys: 1000, KeyCount: 0},
		},
		{
			name: "escaping_special_chars",
			resp: ListBucketV2Result{
				Name: "b",
				Contents: []ObjectXML{
					{
						Key:          `a&b<c>d"e'f`,
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         `"abc123"`,
						StorageClass: "STANDARD",
						Size:         5,
					},
				},
				KeyCount: 1,
				MaxKeys:  1000,
			},
		},
		{
			name: "control_characters",
			resp: ListBucketV2Result{
				Name: "b",
				Contents: []ObjectXML{
					{
						Key:          "tab\ttab\nnewline\rcr\x00null\x01soh",
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         "etag",
						StorageClass: "STANDARD",
						Size:         1,
					},
				},
				KeyCount: 1,
				MaxKeys:  1000,
			},
		},
		{
			name: "unicode_keys",
			resp: ListBucketV2Result{
				Name: "b",
				Contents: []ObjectXML{
					{
						Key:          "日本語/文件-é-😀",
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         "etag",
						StorageClass: "STANDARD",
						Size:         2,
					},
				},
				KeyCount: 1,
				MaxKeys:  1000,
			},
		},
		{
			name: "invalid_utf8_and_illegal_runes",
			resp: ListBucketV2Result{
				Name: "b",
				Contents: []ObjectXML{
					{
						Key:          invalidUTF8AndIllegalRunesKey,
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         "etag",
						StorageClass: "STANDARD",
						Size:         2,
					},
				},
				KeyCount: 1,
				MaxKeys:  1000,
			},
		},
		{
			name: "encoding_type_url",
			resp: ListBucketV2Result{
				Name:         "b",
				Prefix:       "a%2Fb",
				EncodingType: "url",
				Contents: []ObjectXML{
					{
						Key:          "a%2Fb%2Fkey+with+spaces",
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         "etag",
						StorageClass: "STANDARD",
						Size:         3,
					},
				},
				KeyCount: 1,
				MaxKeys:  1000,
			},
		},
		{
			name: "delimiter_and_common_prefixes",
			resp: ListBucketV2Result{
				Name:      "b",
				Delimiter: "/",
				CommonPrefixes: []CommonPrefixXML{
					{Prefix: "dir1/"},
					{Prefix: "dir2/"},
				},
				Contents: []ObjectXML{
					{
						Key:          "root-key",
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         "etag",
						StorageClass: "STANDARD",
						Size:         4,
					},
				},
				KeyCount: 3,
				MaxKeys:  1000,
			},
		},
		{
			name: "owner_present",
			resp: ListBucketV2Result{
				Name: "b",
				Contents: []ObjectXML{
					{
						Owner:        testOwner(),
						Key:          "key-with-owner",
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         "etag",
						StorageClass: "STANDARD",
						Size:         6,
					},
				},
				KeyCount: 1,
				MaxKeys:  1000,
			},
		},
		{
			name: "owner_absent",
			resp: ListBucketV2Result{
				Name: "b",
				Contents: []ObjectXML{
					{
						Owner:        nil,
						Key:          "key-without-owner",
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         "etag",
						StorageClass: "STANDARD",
						Size:         7,
					},
				},
				KeyCount: 1,
				MaxKeys:  1000,
			},
		},
		{
			name: "checksum_algorithms",
			resp: ListBucketV2Result{
				Name: "b",
				Contents: []ObjectXML{
					{
						Key: "k1", LastModified: "t", ETag: "e1",
						StorageClass: "STANDARD", ChecksumAlgorithm: "CRC32", Size: 1,
					},
					{
						Key: "k2", LastModified: "t", ETag: "e2",
						StorageClass: "STANDARD", ChecksumAlgorithm: "SHA256", Size: 2,
					},
					{
						Key: "k3", LastModified: "t", ETag: "e3",
						StorageClass: "STANDARD", ChecksumAlgorithm: "", Size: 3,
					},
				},
				KeyCount: 3,
				MaxKeys:  1000,
			},
		},
		{
			name: "truncated_with_continuation_token",
			resp: ListBucketV2Result{
				Name:                  "b",
				StartAfter:            "start-key",
				ContinuationToken:     "cont-token",
				NextContinuationToken: "next-token",
				IsTruncated:           true,
				Contents: []ObjectXML{
					{Key: "k1", LastModified: "t", ETag: "e1", StorageClass: "STANDARD", Size: 1},
				},
				KeyCount: 1,
				MaxKeys:  1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want := oldEncode(t, tt.resp)

			var got bytes.Buffer
			got.WriteString(xml.Header)
			writeListBucketV2XML(&got, &tt.resp)

			require.Equal(t, string(want), got.String())
		})
	}
}

func TestListBucketXML_GoldenEquivalence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		resp ListBucketResult
	}{
		{
			name: "empty_list",
			resp: ListBucketResult{Name: "b", Prefix: "", MaxKeys: 1000},
		},
		{
			name: "escaping_special_chars",
			resp: ListBucketResult{
				Name: "b",
				Contents: []ObjectXML{
					{
						Owner:        testOwner(),
						Key:          `a&b<c>d"e'f`,
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         `"abc123"`,
						StorageClass: "STANDARD",
						Size:         5,
					},
				},
				MaxKeys: 1000,
			},
		},
		{
			name: "control_characters_and_unicode",
			resp: ListBucketResult{
				Name: "b",
				Contents: []ObjectXML{
					{
						Owner:        testOwner(),
						Key:          "tab\ttab\nnewline\rcr\x00null-日本語-😀",
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         "etag",
						StorageClass: "STANDARD",
						Size:         1,
					},
				},
				MaxKeys: 1000,
			},
		},
		{
			name: "invalid_utf8_and_illegal_runes",
			resp: ListBucketResult{
				Name: "b",
				Contents: []ObjectXML{
					{
						Owner:        testOwner(),
						Key:          invalidUTF8AndIllegalRunesKey,
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         "etag",
						StorageClass: "STANDARD",
						Size:         1,
					},
				},
				MaxKeys: 1000,
			},
		},
		{
			name: "encoding_type_url_with_marker",
			resp: ListBucketResult{
				Name:         "b",
				Prefix:       "a%2Fb",
				Marker:       "marker%2Fkey",
				NextMarker:   "next%2Fmarker",
				EncodingType: "url",
				Contents: []ObjectXML{
					{
						Owner:        testOwner(),
						Key:          "a%2Fb%2Fkey+with+spaces",
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         "etag",
						StorageClass: "STANDARD",
						Size:         3,
					},
				},
				MaxKeys:     1000,
				IsTruncated: true,
			},
		},
		{
			name: "delimiter_and_common_prefixes",
			resp: ListBucketResult{
				Name:      "b",
				Delimiter: "/",
				CommonPrefixes: []CommonPrefixXML{
					{Prefix: "dir1/"},
					{Prefix: "dir2/"},
				},
				Contents: []ObjectXML{
					{
						Owner:        testOwner(),
						Key:          "root-key",
						LastModified: "2024-01-01T00:00:00Z",
						ETag:         "etag",
						StorageClass: "STANDARD",
						Size:         4,
					},
				},
				MaxKeys: 1000,
			},
		},
		{
			name: "checksum_algorithms",
			resp: ListBucketResult{
				Name: "b",
				Contents: []ObjectXML{
					{
						Owner: testOwner(), Key: "k1", LastModified: "t", ETag: "e1",
						StorageClass: "STANDARD", ChecksumAlgorithm: "CRC32C", Size: 1,
					},
					{
						Owner: testOwner(), Key: "k2", LastModified: "t", ETag: "e2",
						StorageClass: "STANDARD", ChecksumAlgorithm: "", Size: 2,
					},
				},
				MaxKeys: 1000,
			},
		},
		{
			name: "truncated",
			resp: ListBucketResult{
				Name:        "b",
				NextMarker:  "next-key",
				IsTruncated: true,
				Contents: []ObjectXML{
					{Owner: testOwner(), Key: "k1", LastModified: "t", ETag: "e1", StorageClass: "STANDARD", Size: 1},
				},
				MaxKeys: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want := oldEncode(t, tt.resp)

			var got bytes.Buffer
			got.WriteString(xml.Header)
			writeListBucketXML(&got, &tt.resp)

			require.Equal(t, string(want), got.String())
		})
	}
}

func TestEscapeXMLString_MatchesStdlib(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
	}{
		{name: "empty", in: ""},
		{name: "ascii", in: "hello-world"},
		{name: "all_special", in: `&<>"'`},
		{name: "control_chars", in: "\t\n\r\x00\x1f"},
		{name: "unicode", in: "日本語😀é"},
		{name: "invalid_replacement_char", in: "a�b"},
		{name: "invalid_utf8_bytes", in: "a\xff\xfeb"},
		{name: "truncated_multibyte_sequence", in: "a\xe4\xb8b"},
		{name: "vertical_tab_illegal_xml_char", in: "a\x0bb"},
		{name: "noncharacter_ufffe", in: "a￾b"},
		{name: "lone_surrogate_as_bytes", in: "a\xed\xa0\x80b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			type wrapper struct {
				XMLName xml.Name `xml:"W"`
				V       string   `xml:"V"`
			}

			want := oldEncode(t, wrapper{V: tt.in})

			var got bytes.Buffer
			got.WriteString(xml.Header)
			got.WriteString("<W>")
			writeXMLElem(&got, "V", tt.in)
			got.WriteString("</W>")

			require.Equal(t, string(want), got.String())
		})
	}
}

// fillNonZero recursively sets every exported, settable field of v to a
// distinct non-zero value, so a filled struct exercises every field.
func fillNonZero(v reflect.Value, seed *int) {
	switch v.Kind() { //nolint:exhaustive // only kinds used by the XML response types
	case reflect.String:
		*seed++
		v.SetString(fmt.Sprintf("v%d", *seed))
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		*seed++
		v.SetInt(int64(*seed))
	case reflect.Slice:
		elem := reflect.New(v.Type().Elem()).Elem()
		fillNonZero(elem, seed)
		s := reflect.MakeSlice(v.Type(), 1, 1)
		s.Index(0).Set(elem)
		v.Set(s)
	case reflect.Pointer:
		p := reflect.New(v.Type().Elem())
		fillNonZero(p.Elem(), seed)
		v.Set(p)
	case reflect.Struct:
		fillStructFields(v, seed)
	}
}

// fillStructFields skips XMLName: its runtime value never affects a
// tag-named root element (verified against encoding/xml's own behavior).
func fillStructFields(v reflect.Value, seed *int) {
	for i := range v.NumField() {
		f := v.Type().Field(i)
		if f.Name == "XMLName" || !v.Field(i).CanSet() {
			continue
		}
		fillNonZero(v.Field(i), seed)
	}
}

// newFilled builds a fully non-zero-filled *T via reflection, so no field
// can stay accidentally zero/unexercised in the drift-guard test below.
func newFilled[T any]() *T {
	v := reflect.New(reflect.TypeOf(*new(T))).Elem()
	seed := 0
	fillNonZero(v, &seed)

	return v.Addr().Interface().(*T) //nolint:forcetypeassert // v was constructed from T above
}

// Fills every field via reflection: a field the hand-written encoder doesn't
// know about shows up here as a byte diff instead of silently vanishing.
func TestListXML_DriftGuard_ReflectiveFill(t *testing.T) {
	t.Parallel()

	t.Run("list_bucket_v2_result", func(t *testing.T) {
		t.Parallel()

		resp := newFilled[ListBucketV2Result]()
		want := oldEncode(t, resp)

		var got bytes.Buffer
		got.WriteString(xml.Header)
		writeListBucketV2XML(&got, resp)

		require.Equal(
			t,
			string(want),
			got.String(),
			"a field on ListBucketV2Result or a nested type changed -- update writeListBucketV2XML in listing_xml_fast.go",
		)
	})

	t.Run("list_bucket_result", func(t *testing.T) {
		t.Parallel()

		resp := newFilled[ListBucketResult]()
		want := oldEncode(t, resp)

		var got bytes.Buffer
		got.WriteString(xml.Header)
		writeListBucketXML(&got, resp)

		require.Equal(t, string(want), got.String(),
			"a field on ListBucketResult or a nested type changed -- update writeListBucketXML in listing_xml_fast.go")
	})
}

// assertFieldNames is the fallback drift guard: a field rename/add/remove
// fails loudly here even without the reflective-fill test above.
func assertFieldNames(t *testing.T, typ reflect.Type, want []string) {
	t.Helper()

	got := make([]string, 0, typ.NumField())
	for field := range typ.Fields() {
		got = append(got, field.Name)
	}

	require.Equal(t, want, got,
		"%s's fields changed -- update listing_xml_fast.go's XML writer for %s, then update this pinned list",
		typ.Name(), typ.Name())
}

func TestListXMLStructs_FieldNamesPinned(t *testing.T) {
	t.Parallel()

	assertFieldNames(t, reflect.TypeFor[ListBucketV2Result](), []string{
		"XMLName", "StartAfter", "Prefix", "Delimiter", "ContinuationToken",
		"NextContinuationToken", "Name", "EncodingType", "Contents",
		"CommonPrefixes", "KeyCount", "MaxKeys", "IsTruncated",
	})
	assertFieldNames(t, reflect.TypeFor[ListBucketResult](), []string{
		"XMLName", "Name", "Prefix", "Delimiter", "Marker", "NextMarker",
		"EncodingType", "Contents", "CommonPrefixes", "MaxKeys", "IsTruncated",
	})
	assertFieldNames(t, reflect.TypeFor[ObjectXML](), []string{
		"Owner", "Key", "LastModified", "ETag", "StorageClass", "ChecksumAlgorithm", "Size",
	})
	assertFieldNames(t, reflect.TypeFor[Owner](), []string{"ID", "DisplayName"})
	assertFieldNames(t, reflect.TypeFor[CommonPrefixXML](), []string{"Prefix"})
}
