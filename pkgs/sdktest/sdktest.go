// Package sdktest holds AWS SDK HTTP client helpers; only test files may import it.
package sdktest

import (
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

type plainBodyDoer struct{ inner aws.HTTPClient }

func (d plainBodyDoer) Do(r *http.Request) (*http.Response, error) {
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = struct {
			io.Reader
			io.Closer
		}{r.Body, r.Body}
	}

	return d.inner.Do(r)
}

// PlainBody hides the request body's io.WriterTo from net/http: smithy-go's WriteTo
// returns io.EOF after close, which tears down live event streams (gopherstack-8wa8j).
func PlainBody(inner aws.HTTPClient) aws.HTTPClient {
	return plainBodyDoer{inner: inner}
}

// PlainBodyHTTPClient returns a PlainBody client with keep-alives off, avoiding stale-conn reuse.
func PlainBodyHTTPClient() aws.HTTPClient {
	return PlainBody(awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.DisableKeepAlives = true
	}))
}
