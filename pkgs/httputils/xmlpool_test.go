package httputils_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

type xmlNSItem struct {
	XMLName xml.Name `xml:"urn:example Item"`
	ID      string   `xml:"id,attr"`
	Ref     string   `xml:"urn:other ref,attr"`
	Name    string   `xml:"Name"`
}

type xmlNSDoc struct {
	XMLName xml.Name    `xml:"urn:example Doc"`
	Note    *string     `xml:"Note,omitempty"`
	Items   []xmlNSItem `xml:"Items>Item"`
	Count   int         `xml:"Count"`
}

func referenceXML(t *testing.T, payload any) []byte {
	t.Helper()

	var buf bytes.Buffer

	buf.WriteString(xml.Header)
	require.NoError(t, xml.NewEncoder(&buf).Encode(payload))

	return buf.Bytes()
}

func TestMarshalXMLByteEqual(t *testing.T) {
	t.Parallel()

	note := "n&<>"
	big := xmlNSDoc{Count: 3}

	for i := range 400 {
		big.Items = append(big.Items, xmlNSItem{ID: "a", Ref: "b", Name: string(rune('a' + i%26))})
	}

	tests := []struct {
		payload any
		name    string
	}{
		{struct {
			XMLName xml.Name `xml:"root"`
			Val     string   `xml:"val"`
		}{Val: "x<y"}, "plain"},
		{xmlNSDoc{
			Items: []xmlNSItem{{ID: "1", Ref: "r", Name: "one"}, {ID: "2", Ref: "r", Name: "two"}}, Note: &note,
		}, "namespaced"},
		{xmlNSDoc{}, "empty"},
		{big, "large_not_pooled_or_pooled"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want := referenceXML(t, tt.payload)

			for range 3 {
				got, err := httputils.MarshalXML(tt.payload)
				require.NoError(t, err)
				assert.Equal(t, want, got)

				rec := httptest.NewRecorder()
				httputils.WriteXML(context.Background(), rec, 200, tt.payload)
				assert.Equal(t, want, rec.Body.Bytes())
			}
		})
	}
}

func TestMarshalXMLErrorDoesNotPoisonPool(t *testing.T) {
	t.Parallel()

	good := xmlNSDoc{Items: []xmlNSItem{{ID: "1", Ref: "r", Name: "one"}}}
	want := referenceXML(t, good)

	tests := []struct {
		bad  any
		name string
	}{
		{struct{ F func() }{}, "unsupported_func"},
		{struct{ C chan int }{}, "unsupported_chan"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for range 5 {
				_, err := httputils.MarshalXML(tt.bad)
				require.Error(t, err)

				got, err := httputils.MarshalXML(good)
				require.NoError(t, err)
				assert.Equal(t, want, got)
			}
		})
	}
}
