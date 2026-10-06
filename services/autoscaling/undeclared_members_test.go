package autoscaling_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_LaunchConfigurationEbsOmitsKmsKeyId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kms  string
	}{
		{name: "with_key", kms: "arn:aws:kms:us-east-1:000000000000:key/abc"},
		{name: "without_key", kms: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			body := "Action=CreateLaunchConfiguration&Version=2011-01-01&LaunchConfigurationName=lc" +
				"&ImageId=ami-123&InstanceType=t3.micro" +
				"&BlockDeviceMappings.member.1.DeviceName=/dev/xvda" +
				"&BlockDeviceMappings.member.1.Ebs.VolumeSize=20&BlockDeviceMappings.member.1.Ebs.Encrypted=true"
			if tc.kms != "" {
				body += "&BlockDeviceMappings.member.1.Ebs.KmsKeyId=" + tc.kms
			}

			require.Equal(t, http.StatusOK, postAutoscalingForm(t, h, body).Code)

			rec := postAutoscalingForm(t, h, "Action=DescribeLaunchConfigurations&Version=2011-01-01")
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "<VolumeSize>20</VolumeSize>")
			assert.NotContains(t, rec.Body.String(), "KmsKeyId")
		})
	}
}
