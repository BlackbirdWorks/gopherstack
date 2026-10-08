package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateDevEndpoint_CustomLibraries(t *testing.T) {
	t.Parallel()

	libs := &types.DevEndpointCustomLibraries{
		ExtraJarsS3Path:       aws.String("s3://b/new.jar"),
		ExtraPythonLibsS3Path: aws.String("s3://b/new.py"),
	}

	tests := []struct {
		libs       *types.DevEndpointCustomLibraries
		name       string
		wantJars   string
		wantPython string
		update     bool
	}{
		{name: "flag_unset_ignores_libraries", libs: libs, wantJars: "s3://b/old.jar", wantPython: "s3://b/old.py"},
		{name: "flag_set_applies", libs: libs, update: true, wantJars: "s3://b/new.jar", wantPython: "s3://b/new.py"},
		{name: "flag_set_without_libraries_clears", update: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateDevEndpoint(ctx, &gluesdk.CreateDevEndpointInput{
				EndpointName:          aws.String("dev"),
				RoleArn:               aws.String("arn:aws:iam::123456789012:role/r"),
				ExtraJarsS3Path:       aws.String("s3://b/old.jar"),
				ExtraPythonLibsS3Path: aws.String("s3://b/old.py"),
			})
			require.NoError(t, err)

			_, err = client.UpdateDevEndpoint(ctx, &gluesdk.UpdateDevEndpointInput{
				EndpointName:       aws.String("dev"),
				CustomLibraries:    tt.libs,
				UpdateEtlLibraries: tt.update,
			})
			require.NoError(t, err)

			got, err := client.GetDevEndpoint(ctx, &gluesdk.GetDevEndpointInput{EndpointName: aws.String("dev")})
			require.NoError(t, err)
			assert.Equal(t, tt.wantJars, aws.ToString(got.DevEndpoint.ExtraJarsS3Path))
			assert.Equal(t, tt.wantPython, aws.ToString(got.DevEndpoint.ExtraPythonLibsS3Path))
		})
	}
}
