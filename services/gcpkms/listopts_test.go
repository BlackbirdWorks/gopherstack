package gcpkms_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func drain[T any](next func() (T, error), name func(T) string) ([]string, error) {
	var out []string

	for {
		v, err := next()
		if errors.Is(err, iterator.Done) {
			return out, nil
		}

		if err != nil {
			return nil, err
		}

		out = append(out, name(v))
	}
}

func TestListKeyRingsFilterAndOrder(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	for _, id := range []string{"alpha", "beta", "gamma"} {
		e.ring(t, id)
		e.clk.advance(time.Hour)
	}

	p := location + "/keyRings/"
	cut := e.clk.now().Add(-90 * time.Minute).Format(time.RFC3339)

	tests := []struct {
		name, filter, order string
		want                []string
		code                codes.Code
	}{
		{name: "none", want: []string{p + "alpha", p + "beta", p + "gamma"}},
		{name: "eq", filter: `name = "` + p + `beta"`, want: []string{p + "beta"}},
		{name: "ne", filter: `name != "` + p + `beta"`, want: []string{p + "alpha", p + "gamma"}},
		{name: "has", filter: "name:amm", want: []string{p + "gamma"}},
		{name: "or", filter: "name:alp OR name:gam", want: []string{p + "alpha", p + "gamma"}},
		{name: "and", filter: "name:a AND name:m", want: []string{p + "gamma"}},
		{name: "parens", filter: "(name:alp OR name:bet) AND name:a", want: []string{p + "alpha", p + "beta"}},
		{name: "snake case time", filter: "create_time > " + cut, want: []string{p + "gamma"}},
		{name: "desc", order: "name desc", want: []string{p + "gamma", p + "beta", p + "alpha"}},
		{name: "create time desc", order: "createTime desc", want: []string{p + "gamma", p + "beta", p + "alpha"}},
		{name: "multi", order: "createTime asc, name desc", want: []string{p + "alpha", p + "beta", p + "gamma"}},
		{name: "unknown field", filter: "bogus = x", code: codes.InvalidArgument},
		{name: "unknown op", filter: "name ~ x", code: codes.InvalidArgument},
		{name: "relational on string", filter: "name > a", code: codes.InvalidArgument},
		{name: "bad time", filter: "createTime > yesterday", code: codes.InvalidArgument},
		{name: "unbalanced", filter: "(name:a", code: codes.InvalidArgument},
		{name: "trailing", filter: "name:a name:b", code: codes.InvalidArgument},
		{name: "unterminated quote", filter: `name = "a`, code: codes.InvalidArgument},
		{name: "bad order field", order: "labels", code: codes.InvalidArgument},
		{name: "bad order dir", order: "name sideways", code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.client.ListKeyRings(t.Context(), &kmspb.ListKeyRingsRequest{
				Parent: location, Filter: tt.filter, OrderBy: tt.order, PageSize: 2,
			})

			got, err := drain(it.Next, (*kmspb.KeyRing).GetName)
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestListCryptoKeysFilter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	e.key(t, ring, "mac1", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256,
		func(k *kmspb.CryptoKey) { k.Labels = map[string]string{"env": "prod"} })
	e.key(t, ring, "mac2", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA512,
		func(k *kmspb.CryptoKey) { k.Labels = map[string]string{"env": "dev"} })
	e.key(t, ring, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION)

	p := ring + "/cryptoKeys/"

	tests := []struct {
		name, filter, order string
		want                []string
	}{
		{"purpose", "purpose = MAC", "name desc", []string{p + "mac2", p + "mac1"}},
		{"label", "labels.env = prod", "", []string{p + "mac1"}},
		{"label ne", "labels.env != prod", "", []string{p + "mac2", p + "sym"}},
		{"algorithm", "versionTemplate.algorithm = HMAC_SHA512", "", []string{p + "mac2"}},
		{
			"level and purpose",
			"version_template.protection_level = SOFTWARE AND purpose = ENCRYPT_DECRYPT",
			"",
			[]string{p + "sym"},
		},
		{"import only", "importOnly = false", "", []string{p + "mac1", p + "mac2", p + "sym"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.client.ListCryptoKeys(t.Context(), &kmspb.ListCryptoKeysRequest{
				Parent: ring, Filter: tt.filter, OrderBy: tt.order,
			})

			got, err := drain(it.Next, (*kmspb.CryptoKey).GetName)
			require.NoError(t, err)

			if tt.order == "" {
				assert.ElementsMatch(t, tt.want, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestListVersionsAndJobsFilter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	key := e.key(t, ring, "k", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256)

	for range 2 {
		_, err := e.client.CreateCryptoKeyVersion(
			t.Context(),
			&kmspb.CreateCryptoKeyVersionRequest{Parent: key.GetName()},
		)
		require.NoError(t, err)
	}

	_, err := e.client.UpdateCryptoKeyVersion(t.Context(), &kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{
			Name:  key.GetName() + "/cryptoKeyVersions/2",
			State: kmspb.CryptoKeyVersion_DISABLED,
		},
		UpdateMask: fieldMask("state"),
	})
	require.NoError(t, err)

	v := func(n int) string { return fmt.Sprintf("%s/cryptoKeyVersions/%d", key.GetName(), n) }

	vtests := []struct {
		name, filter, order string
		want                []string
	}{
		{"state", "state = DISABLED", "", []string{v(2)}},
		{"not state", "state != DISABLED", "name desc", []string{v(3), v(1)}},
		{"algorithm", "algorithm = HMAC_SHA256 AND state = ENABLED", "", []string{v(1), v(3)}},
	}

	for _, tt := range vtests {
		t.Run("versions "+tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.client.ListCryptoKeyVersions(t.Context(), &kmspb.ListCryptoKeyVersionsRequest{
				Parent: key.GetName(), Filter: tt.filter, OrderBy: tt.order,
			})

			got, derr := drain(it.Next, (*kmspb.CryptoKeyVersion).GetName)
			require.NoError(t, derr)
			assert.Equal(t, tt.want, got)
		})
	}

	for _, id := range []string{"j1", "j2"} {
		method := kmspb.ImportJob_RSA_OAEP_3072_SHA256
		if id == "j2" {
			method = kmspb.ImportJob_RSA_OAEP_4096_SHA256
		}

		_, err = e.client.CreateImportJob(t.Context(), &kmspb.CreateImportJobRequest{
			Parent: ring, ImportJobId: id,
			ImportJob: &kmspb.ImportJob{ImportMethod: method, ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE},
		})
		require.NoError(t, err)
	}

	jobs := e.client.ListImportJobs(t.Context(), &kmspb.ListImportJobsRequest{
		Parent: ring, Filter: "importMethod = RSA_OAEP_4096_SHA256", OrderBy: "name",
	})
	got, err := drain(jobs.Next, (*kmspb.ImportJob).GetName)
	require.NoError(t, err)
	assert.Equal(t, []string{ring + "/importJobs/j2"}, got)

	bad := e.client.ListImportJobs(t.Context(), &kmspb.ListImportJobsRequest{Parent: ring, OrderBy: "state"})
	_, err = bad.Next()
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
