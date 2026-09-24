package sts_test

// Golden fixture for the gopherstack perf sweep's STS investigation
// (2026-09-24, see perf_sweep_bench_test.go): pins AssumeRole's XML response
// byte-for-byte (modulo the randomly/wall-clock generated fields below) as a
// regression baseline across the sessionEvictSweepInterval debounce change in
// services/sts/store.go.
//
// To regenerate: STS_GOLDEN_UPDATE=1 go test -run TestGolden ./services/sts/...

import (
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func goldenSTSPath(name string) string {
	return filepath.Join("testdata", name)
}

func checkGoldenSTS(t *testing.T, name string, got []byte) {
	t.Helper()

	path := goldenSTSPath(name)
	if os.Getenv("STS_GOLDEN_UPDATE") != "" {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, got, 0o600))

		return
	}

	want, err := os.ReadFile(path)
	require.NoError(t, err, "golden file %s missing -- run with STS_GOLDEN_UPDATE=1 first", path)
	require.Equal(t, string(want), string(got), "response for %s changed", name)
}

var (
	xmlAccessKeyIDRe = regexp.MustCompile(`<AccessKeyId>[^<]*</AccessKeyId>`)
	xmlSecretKeyRe   = regexp.MustCompile(`<SecretAccessKey>[^<]*</SecretAccessKey>`)
	xmlSessionTokRe  = regexp.MustCompile(`<SessionToken>[^<]*</SessionToken>`)
	xmlExpirationRe  = regexp.MustCompile(`<Expiration>[^<]*</Expiration>`)
	xmlAssumedRoleRe = regexp.MustCompile(`<AssumedRoleId>[^<]*</AssumedRoleId>`)
	xmlSTSRequestRe  = regexp.MustCompile(`<RequestId>[^<]*</RequestId>`)
)

// redactAssumeRoleXML blanks AssumeRole's randomly generated or wall-clock
// fields (access key, secret key, session token, expiration, assumed-role ID,
// request ID), leaving everything else -- including the response shape and
// the deterministic Arn -- byte-comparable.
func redactAssumeRoleXML(body []byte) []byte {
	body = xmlAccessKeyIDRe.ReplaceAll(body, []byte("<AccessKeyId>REDACTED</AccessKeyId>"))
	body = xmlSecretKeyRe.ReplaceAll(body, []byte("<SecretAccessKey>REDACTED</SecretAccessKey>"))
	body = xmlSessionTokRe.ReplaceAll(body, []byte("<SessionToken>REDACTED</SessionToken>"))
	body = xmlExpirationRe.ReplaceAll(body, []byte("<Expiration>REDACTED</Expiration>"))
	body = xmlAssumedRoleRe.ReplaceAll(body, []byte("<AssumedRoleId>REDACTED</AssumedRoleId>"))
	body = xmlSTSRequestRe.ReplaceAll(body, []byte("<RequestId>REDACTED</RequestId>"))

	return body
}

// TestGolden_AssumeRoleResponse asserts AssumeRole's XML response is
// byte-identical (modulo the redacted fields) before and after the perf
// sweep, both below and above sessionEvictThreshold (where the debounced
// sweep is armed).
func TestGolden_AssumeRoleResponse(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)

	rec := postForm(t, e, h, url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::123456789012:role/golden-role"},
		"RoleSessionName": {"golden-session"},
	})
	require.Equal(t, 200, rec.Code, rec.Body.String())

	checkGoldenSTS(t, "golden_assume_role_response.xml", redactAssumeRoleXML(rec.Body.Bytes()))
}
