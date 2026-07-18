package cloudformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
)

// TestResourceCreator_Extra_GetAtt verifies Fn::GetAtt resolution for phase-5 resource types.
func TestResourceCreator_Extra_GetAtt(t *testing.T) {
	t.Parallel()

	const (
		account = "000000000000"
		region  = "us-east-1"
	)

	tests := []struct {
		name     string
		resType  string
		physID   string
		attrName string
		want     string
	}{
		{name: "volume_id", resType: "AWS::EC2::Volume", physID: "vol-abc", attrName: "VolumeId", want: "vol-abc"},
		{name: "eni_id", resType: "AWS::EC2::NetworkInterface", physID: "eni-abc", attrName: "Id", want: "eni-abc"},
		{
			name: "activity_arn", resType: "AWS::StepFunctions::Activity",
			physID: "arn:aws:states:us-east-1:000000000000:activity:proc", attrName: "Arn",
			want: "arn:aws:states:us-east-1:000000000000:activity:proc",
		},
		{
			name: "activity_name", resType: "AWS::StepFunctions::Activity",
			physID: "arn:aws:states:us-east-1:000000000000:activity:proc", attrName: "Name", want: "proc",
		},
		{
			name: "connection_arn", resType: "AWS::Events::Connection", physID: "my-conn", attrName: "Arn",
			want: "arn:aws:events:us-east-1:000000000000:connection/my-conn",
		},
		{
			name: "logstream_name", resType: "AWS::Logs::LogStream", physID: "/grp|stream-1",
			attrName: "LogStreamName", want: "stream-1",
		},
		{
			name: "kms_alias_arn", resType: "AWS::KMS::Alias", physID: "alias/x", attrName: "Arn",
			want: "arn:aws:kms:us-east-1:000000000000:alias/x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudformation.GetResourceAttribute(tt.resType, tt.physID, tt.attrName, account, region)
			assert.Equal(t, tt.want, got)
		})
	}
}
