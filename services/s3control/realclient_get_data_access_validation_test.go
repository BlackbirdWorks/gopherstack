package s3control_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

// TestRealClient_GetDataAccessValidation proves GetDataAccess's
// DurationSeconds/Privilege query params are now read and validated
// (gopherstack-xhu2t) -- previously neither was decoded at all, so any
// value (including one outside the documented 900-43200 second range, or
// an invalid Privilege enum) silently had no effect.
func TestRealClient_GetDataAccessValidation(t *testing.T) {
	t.Parallel()

	backend := s3control.NewInMemoryBackend()
	backend.CreateAccessGrantsInstance("123456789012", "")
	loc := backend.CreateAccessGrantsLocation("123456789012", "s3://bucket", "arn:aws:iam::123456789012:role/r")
	_, err := backend.CreateAccessGrant(
		"123456789012", loc.AccessGrantsLocationID, "IAM", "arn:aws:iam::123456789012:role/g", "READ", "", "",
	)
	require.NoError(t, err)

	client := newTestS3ControlClient(t, s3control.NewHandler(backend))
	ctx := t.Context()

	cases := []struct {
		name      string
		duration  *int32
		privilege types.Privilege
		wantErr   bool
	}{
		{
			name:      "valid duration and privilege",
			duration:  aws.Int32(900),
			privilege: types.PrivilegeMinimal,
			wantErr:   false,
		},
		{name: "duration below minimum rejected", duration: aws.Int32(899), wantErr: true},
		{name: "duration above maximum rejected", duration: aws.Int32(43201), wantErr: true},
		{name: "duration at maximum accepted", duration: aws.Int32(43200), wantErr: false},
		{
			name:      "invalid privilege rejected",
			privilege: types.Privilege("NotAPrivilege"),
			wantErr:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, gdErr := client.GetDataAccess(ctx, &s3csdk.GetDataAccessInput{
				AccountId:       aws.String("123456789012"),
				Target:          aws.String("s3://bucket/prefix/"),
				Permission:      types.PermissionRead,
				DurationSeconds: tc.duration,
				Privilege:       tc.privilege,
			})
			if tc.wantErr {
				require.Error(t, gdErr)
			} else {
				require.NoError(t, gdErr)
			}
		})
	}
}

func TestRealClient_GetDataAccessCredentials(t *testing.T) {
	t.Parallel()

	backend := s3control.NewInMemoryBackend()
	backend.CreateAccessGrantsInstance("123456789012", "")
	loc := backend.CreateAccessGrantsLocation("123456789012", "s3://bucket", "arn:aws:iam::123456789012:role/r")
	_, err := backend.CreateAccessGrant(
		"123456789012", loc.AccessGrantsLocationID, "IAM", "arn:aws:iam::123456789012:role/g", "READ", "", "",
	)
	require.NoError(t, err)
	client := newTestS3ControlClient(t, s3control.NewHandler(backend))

	out, err := client.GetDataAccess(t.Context(), &s3csdk.GetDataAccessInput{
		AccountId: aws.String("123456789012"), Target: aws.String("s3://bucket/k"), Permission: types.PermissionRead,
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	require.NotNil(t, out.Credentials.Expiration)
	require.NotEmpty(t, aws.ToString(out.Credentials.AccessKeyId))
	require.NotEmpty(t, aws.ToString(out.Credentials.SecretAccessKey))
	require.NotEmpty(t, aws.ToString(out.Credentials.SessionToken))
	require.NotNil(t, out.Grantee)
	require.Equal(t, "arn:aws:iam::123456789012:role/g", aws.ToString(out.Grantee.GranteeIdentifier))

	_, err = client.GetDataAccess(t.Context(), &s3csdk.GetDataAccessInput{
		AccountId: aws.String("123456789012"), Target: aws.String("s3://other/k"), Permission: types.PermissionRead,
	})
	require.ErrorContains(t, err, "AccessDenied")
}
