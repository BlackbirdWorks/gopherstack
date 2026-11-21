package gcpsecretmanager_test

import (
	"testing"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegionalLocationValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	for name, tc := range map[string]struct {
		location string
		code     codes.Code
	}{
		"region":        {"europe-west4", codes.OK},
		"multi region":  {"us", codes.OK},
		"unknown":       {"mars-north1", codes.InvalidArgument},
		"global":        {"global", codes.InvalidArgument},
		"uppercase":     {"US-CENTRAL1", codes.InvalidArgument},
		"zone not name": {"us-central1-a", codes.InvalidArgument},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			parent := proj + "/locations/" + tc.location

			_, err := e.client.CreateSecret(t.Context(), &secretmanagerpb.CreateSecretRequest{
				Parent: parent, SecretId: "loc",
			})
			assert.Equal(t, tc.code, status.Code(err))

			_, err = e.client.GetSecret(t.Context(), &secretmanagerpb.GetSecretRequest{Name: parent + "/secrets/loc"})
			if tc.code == codes.OK {
				require.NoError(t, err)
			} else {
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
			}

			_, err = e.client.ListSecrets(t.Context(), &secretmanagerpb.ListSecretsRequest{Parent: parent}).Next()
			if tc.code != codes.OK {
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
			}
		})
	}
}
