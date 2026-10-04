package rolesanywhere_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rasdk "github.com/aws/aws-sdk-go-v2/service/rolesanywhere"
	ratypes "github.com/aws/aws-sdk-go-v2/service/rolesanywhere/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteAttributeMapping_EncodedSpecifiers_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		delete []string
		remain []string
	}{
		{name: "plain", delete: []string{"CN"}, remain: []string{"O U", "a/b"}},
		{name: "space", delete: []string{"O U"}, remain: []string{"CN", "a/b"}},
		{name: "slash", delete: []string{"a/b"}, remain: []string{"CN", "O U"}},
		{name: "several", delete: []string{"CN", "O U"}, remain: []string{"a/b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestRolesAnywhereClient(t, newTestRolesAnywhereHandler())
			ctx := t.Context()

			prof, err := client.CreateProfile(ctx, &rasdk.CreateProfileInput{
				Name:     aws.String("enc-profile"),
				RoleArns: []string{"arn:aws:iam::123456789012:role/my-role"},
			})
			require.NoError(t, err)

			id := prof.Profile.ProfileId

			_, err = client.PutAttributeMapping(ctx, &rasdk.PutAttributeMappingInput{
				ProfileId:        id,
				CertificateField: ratypes.CertificateFieldX509Subject,
				MappingRules: []ratypes.MappingRule{
					{Specifier: aws.String("CN")},
					{Specifier: aws.String("O U")},
					{Specifier: aws.String("a/b")},
				},
			})
			require.NoError(t, err)

			out, err := client.DeleteAttributeMapping(ctx, &rasdk.DeleteAttributeMappingInput{
				ProfileId:        id,
				CertificateField: ratypes.CertificateFieldX509Subject,
				Specifiers:       tt.delete,
			})
			require.NoError(t, err)
			require.Len(t, out.Profile.AttributeMappings, 1)

			got := make([]string, 0)
			for _, r := range out.Profile.AttributeMappings[0].MappingRules {
				got = append(got, aws.ToString(r.Specifier))
			}

			assert.ElementsMatch(t, tt.remain, got)
		})
	}
}
