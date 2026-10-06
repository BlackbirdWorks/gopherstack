package rolesanywhere_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rasdk "github.com/aws/aws-sdk-go-v2/service/rolesanywhere"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

func TestProfile_DefaultDurationAndPartialUpdate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		duration     *int32
		name         string
		wantDuration int32
	}{
		{name: "omitted duration defaults to 3600", wantDuration: 3600},
		{name: "explicit duration kept by a name-only update", duration: aws.Int32(900), wantDuration: 900},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestRolesAnywhereClient(t, newTestRolesAnywhereHandler())

			created, err := client.CreateProfile(ctx, &rasdk.CreateProfileInput{
				Name:            aws.String("p"),
				RoleArns:        []string{"arn:aws:iam::000000000000:role/r"},
				DurationSeconds: tc.duration,
			})
			require.NoError(t, err)

			_, err = client.UpdateProfile(ctx, &rasdk.UpdateProfileInput{
				ProfileId: created.Profile.ProfileId, Name: aws.String("p2"),
			})
			require.NoError(t, err)

			got, err := client.GetProfile(ctx, &rasdk.GetProfileInput{ProfileId: created.Profile.ProfileId})
			require.NoError(t, err)
			assert.Equal(t, "p2", aws.ToString(got.Profile.Name))
			assert.Equal(t, tc.wantDuration, aws.ToInt32(got.Profile.DurationSeconds))
			assert.Equal(t, []string{"arn:aws:iam::000000000000:role/r"}, got.Profile.RoleArns)
		})
	}
}

func TestPutAttributeMapping_AdvancesProfileUpdatedAt(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
	}{{name: "put and delete advance updatedAt"}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			b := newBackend(t)

			p, err := b.CreateProfile(ctx, "p", []string{"r"}, nil, nil, nil, "", false, nil, nil)
			require.NoError(t, err)

			put, err := b.PutAttributeMapping(ctx, p.ProfileID, "x509Subject",
				[]rolesanywhere.MappingRule{{Specifier: "CN"}})
			require.NoError(t, err)
			assert.True(t, put.UpdatedAt.After(p.UpdatedAt))

			del, err := b.DeleteAttributeMapping(ctx, p.ProfileID, "x509Subject", nil)
			require.NoError(t, err)
			assert.True(t, del.UpdatedAt.After(put.UpdatedAt))
		})
	}
}
