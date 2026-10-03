package acm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmsdk "github.com/aws/aws-sdk-go-v2/service/acm"
	"github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_RequestCertificate_WeakKeyIsInvalidParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  types.KeyAlgorithm
	}{
		{name: "rsa_1024", key: types.KeyAlgorithmRsa1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestACMClient(t, newACMHandler())

			_, err := client.RequestCertificate(t.Context(), &acmsdk.RequestCertificateInput{
				DomainName:   aws.String("weakkey.example.com"),
				KeyAlgorithm: tt.key,
			})
			require.Error(t, err)

			var ipe *types.InvalidParameterException
			require.ErrorAs(t, err, &ipe)
			assert.Contains(t, err.Error(), "RSA_1024")
		})
	}
}
