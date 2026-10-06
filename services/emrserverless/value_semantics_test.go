package emrserverless_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	emrserverlesssdk "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplication_AutoConfigDefaultsAndArchitectureUpdate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		create      *emrserverlesssdk.CreateApplicationInput
		update      *emrserverlesssdk.UpdateApplicationInput
		name        string
		wantArch    types.Architecture
		wantStartOn bool
		wantStopOn  bool
		wantIdleMin int32
	}{
		{
			name:        "omitted configs get documented defaults",
			create:      &emrserverlesssdk.CreateApplicationInput{},
			update:      &emrserverlesssdk.UpdateApplicationInput{},
			wantArch:    types.ArchitectureArm64,
			wantStartOn: true, wantStopOn: true, wantIdleMin: 15,
		},
		{
			name: "partial stop config fills the rest",
			create: &emrserverlesssdk.CreateApplicationInput{
				AutoStopConfiguration: &types.AutoStopConfig{IdleTimeoutMinutes: aws.Int32(30)},
			},
			update:      &emrserverlesssdk.UpdateApplicationInput{Architecture: types.ArchitectureX8664},
			wantArch:    types.ArchitectureX8664,
			wantStartOn: true, wantStopOn: true, wantIdleMin: 30,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEMRServerlessSDKClient(t, newTestHandler(t))
			ctx := t.Context()

			tc.create.Name, tc.create.Type = aws.String("app"), aws.String("SPARK")
			tc.create.ReleaseLabel = aws.String("emr-6.6.0")
			tc.create.Architecture = types.ArchitectureArm64

			created, err := client.CreateApplication(ctx, tc.create)
			require.NoError(t, err)

			tc.update.ApplicationId = created.ApplicationId
			_, err = client.UpdateApplication(ctx, tc.update)
			require.NoError(t, err)

			got, err := client.GetApplication(
				ctx,
				&emrserverlesssdk.GetApplicationInput{ApplicationId: created.ApplicationId},
			)
			require.NoError(t, err)

			app := got.Application
			assert.Equal(t, tc.wantArch, app.Architecture)
			require.NotNil(t, app.AutoStartConfiguration)
			assert.Equal(t, tc.wantStartOn, aws.ToBool(app.AutoStartConfiguration.Enabled))
			require.NotNil(t, app.AutoStopConfiguration)
			assert.Equal(t, tc.wantStopOn, aws.ToBool(app.AutoStopConfiguration.Enabled))
			assert.Equal(t, tc.wantIdleMin, aws.ToInt32(app.AutoStopConfiguration.IdleTimeoutMinutes))
		})
	}
}
