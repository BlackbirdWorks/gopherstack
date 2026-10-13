package cloudfront_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

func distConfigWithAlias(callerRef, alias string) []byte {
	return fmt.Appendf(nil,
		`<DistributionConfig><CallerReference>%s</CallerReference><Enabled>false</Enabled>`+
			`<Aliases><Quantity>1</Quantity><Items><CNAME>%s</CNAME></Items></Aliases></DistributionConfig>`,
		callerRef, alias)
}

func TestCNAMEConflicts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		second  string
		wantErr bool
	}{
		{name: "same alias", second: "www.example.com", wantErr: true},
		{name: "case insensitive", second: "WWW.Example.COM", wantErr: true},
		{name: "different alias", second: "api.example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			_, err := b.CreateDistribution("r1", "", false, distConfigWithAlias("r1", "www.example.com"))
			require.NoError(t, err)

			_, err = b.CreateDistribution("r2", "", false, distConfigWithAlias("r2", tt.second))
			if tt.wantErr {
				require.ErrorIs(t, err, cloudfront.ErrCNAMEAlreadyExists)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestUpdateKeepsOwnCNAME(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	d, err := b.CreateDistribution("r1", "", false, distConfigWithAlias("r1", "www.example.com"))
	require.NoError(t, err)

	_, err = b.UpdateDistribution(d.ID, "c", false, distConfigWithAlias("r1", "www.example.com"))
	require.NoError(t, err)

	other, err := b.CreateDistribution("r2", "", false, distConfigWithAlias("r2", "other.example.com"))
	require.NoError(t, err)

	_, err = b.UpdateDistribution(other.ID, "c", false, distConfigWithAlias("r2", "www.example.com"))
	require.ErrorIs(t, err, cloudfront.ErrCNAMEAlreadyExists)
}

func TestResourceIDShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		make func(t *testing.T, b *cloudfront.InMemoryBackend) string
		name string
		re   string
	}{
		{
			name: "distribution", re: `^E[A-Z0-9]{13}$`,
			make: func(t *testing.T, b *cloudfront.InMemoryBackend) string {
				t.Helper()
				d, err := b.CreateDistribution("r1", "", false, minimalDistConfig("r1", "", false))
				require.NoError(t, err)

				return d.ID
			},
		},
		{
			name: "invalidation", re: `^I[A-Z0-9]{13}$`,
			make: func(t *testing.T, b *cloudfront.InMemoryBackend) string {
				t.Helper()
				d, err := b.CreateDistribution("r1", "", false, minimalDistConfig("r1", "", false))
				require.NoError(t, err)
				inv, err := b.CreateInvalidation(d.ID, "ir1", []string{"/a"})
				require.NoError(t, err)

				return inv.ID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Regexp(t, tt.re, tt.make(t, newTestBackend(t)))
		})
	}
}

func TestListMaxItemsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{name: "zero", query: "?MaxItems=0", want: http.StatusBadRequest},
		{name: "negative", query: "?MaxItems=-3", want: http.StatusBadRequest},
		{name: "not a number", query: "?MaxItems=abc", want: http.StatusBadRequest},
		{name: "valid", query: "?MaxItems=5", want: http.StatusOK},
		{name: "absent", query: "", want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := cfRequest(t, newTestHandler(t), http.MethodGet, "/2020-05-31/distribution"+tt.query, "")
			assert.Equal(t, tt.want, rec.Code)

			if tt.want == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "<Code>InvalidArgument</Code>")
			}
		})
	}
}

func TestErrorMessageOmitsCode(t *testing.T) {
	t.Parallel()

	rec := cfRequest(t, newTestHandler(t), http.MethodGet, "/2020-05-31/distribution/ENOPE", "")
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>NoSuchDistribution</Code>")
	assert.NotContains(t, rec.Body.String(), "NoSuchDistribution: ")
	assert.NotContains(t, rec.Body.String(), "<Message>NoSuchDistribution")
}

func TestDistributionIfMatchSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ifMatch  func(etag string) string
		name     string
		wantErr  string
		wantCode int
	}{
		{
			name: "missing", ifMatch: func(string) string { return "" },
			wantCode: http.StatusBadRequest, wantErr: "InvalidIfMatchVersion",
		},
		{
			name: "stale", ifMatch: func(string) string { return "stale" },
			wantCode: http.StatusPreconditionFailed, wantErr: "PreconditionFailed",
		},
		{name: "current", ifMatch: func(etag string) string { return etag }, wantCode: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			d, err := h.Backend.CreateDistribution("r1", "", false, minimalDistConfig("r1", "", false))
			require.NoError(t, err)

			headers := map[string]string{}
			if v := tt.ifMatch(d.ETag); v != "" {
				headers["If-Match"] = v
			}

			rec := cfRequestWithHeader(t, h, http.MethodDelete, "/2020-05-31/distribution/"+d.ID, headers)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantErr != "" {
				assert.Contains(t, rec.Body.String(), "<Code>"+tt.wantErr+"</Code>")
			}
		})
	}
}
