package httputils_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func TestRawQueryMatchesParseQuery(t *testing.T) {
	t.Parallel()

	queries := []string{
		"", "x-id=GetObject", "a=1&b=2", "a=1&a=2", "a", "a=", "=v", "&&a=1&", "a=1;b=2&c=3",
		"a=%41&b=%zz", "%61=x", "a+b=c+d", "a=b%2Fc", "tagging&x-id=PutObject", "k=%", "versionId=abc&versionid=x",
		"X-Amz-Signature=sig&a=%20", "a%3Db=1",
	}
	names := []string{"a", "b", "c", "x-id", "tagging", "k", "versionId", "X-Amz-Signature", "a b", "a=b", "", "A"}

	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			t.Parallel()

			want, _ := url.ParseQuery(q)

			for _, n := range names {
				assert.Equal(t, want.Get(n), httputils.RawQueryGet(q, n), "get %q", n)
				assert.Equal(t, want.Has(n), httputils.RawQueryHas(q, n), "has %q", n)
			}
		})
	}
}
