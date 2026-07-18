package ecr_test

// signing_test.go — verifies signing.go: Put/Get/Delete SigningConfiguration
// and DescribeImageSigningStatus, including profile ARN reflection.

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSigningConfiguration_PutGetDelete(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	profileArn := "arn:aws:signer:us-east-1:123456789012:/signing-profiles/MyProfile"
	putRec := doAccuracy(t, h, "PutSigningConfiguration", map[string]any{
		"signingConfiguration": map[string]any{
			"rules": []map[string]any{
				{
					"signingProfileArn": profileArn,
					"repositoryFilters": []map[string]any{
						{"filter": "prod/*", "filterType": "PREFIX_MATCH"},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doAccuracy(t, h, "GetSigningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)
	out := parseAccuracy(t, getRec)
	cfg, _ := out["signingConfiguration"].(map[string]any)
	rules, _ := cfg["rules"].([]any)
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	assert.Equal(t, profileArn, rule["signingProfileArn"])

	delRec := doAccuracy(t, h, "DeleteSigningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, delRec.Code)
}

func TestDescribeImageSigningStatus_NoSigningConfig_StatusComplete(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "signing-repo-no-cfg")
	digest := mustPutImage(t, h, "signing-repo-no-cfg", "v1", `{"schemaVersion":2,"sign":"none"}`)

	rec := doAccuracy(t, h, "DescribeImageSigningStatus", map[string]any{
		"repositoryName": "signing-repo-no-cfg",
		"imageId": map[string]any{
			"imageDigest": digest,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	statuses, _ := out["signingStatuses"].([]any)
	require.NotEmpty(t, statuses)
	s0 := statuses[0].(map[string]any)
	assert.Equal(t, "COMPLETE", s0["status"])
}

func TestDescribeImageSigningStatus_WithSigningConfig_ProfileArnReflected(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "signing-repo-with-cfg")
	digest := mustPutImage(t, h, "signing-repo-with-cfg", "v2", `{"schemaVersion":2,"sign":"cfg"}`)

	profileArn := "arn:aws:signer:us-east-1:123456789012:/signing-profiles/MyProfile"
	doAccuracy(t, h, "PutSigningConfiguration", map[string]any{
		"signingConfiguration": map[string]any{
			"rules": []map[string]any{
				{
					"signingProfileArn": profileArn,
					"repositoryFilters": []map[string]any{
						{"filter": "*", "filterType": "WILDCARD"},
					},
				},
			},
		},
	})

	rec := doAccuracy(t, h, "DescribeImageSigningStatus", map[string]any{
		"repositoryName": "signing-repo-with-cfg",
		"imageId": map[string]any{
			"imageDigest": digest,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	statuses, _ := out["signingStatuses"].([]any)
	require.NotEmpty(t, statuses)
	s0 := statuses[0].(map[string]any)
	assert.Equal(t, profileArn, s0["signingProfileArn"],
		"signing profile ARN must be reflected in DescribeImageSigningStatus when signing config is set")
}
