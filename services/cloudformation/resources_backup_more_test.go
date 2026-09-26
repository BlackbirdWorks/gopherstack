package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_BackupMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testBackupFramework, "framework"},
		{testBackupReportPlan, "report_plan"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testBackupFramework(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "FW": {
    "Type": "AWS::Backup::Framework",
    "Properties": {
      "FrameworkName": "my-framework",
      "FrameworkDescription": "test framework",
      "FrameworkControls": [
        {"ControlName": "BACKUP_RECOVERY_POINT_MINIMUM_RETENTION_CHECK"}
      ]
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "FW"}},
  "Status": {"Value": {"Fn::GetAtt": ["FW", "FrameworkStatus"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "bk-fw-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "framework:my-framework")
	assert.NotEmpty(t, outputs["Status"])

	fw, err := backends.Backup.Backend.DescribeFramework("my-framework")
	require.NoError(t, err)
	assert.Equal(t, "test framework", fw.FrameworkDescription)
	require.Len(t, fw.FrameworkControls, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("bk-fw-stack")})
	require.NoError(t, err)

	_, err = backends.Backup.Backend.DescribeFramework("my-framework")
	require.Error(t, err)
}

func testBackupReportPlan(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "RP": {
    "Type": "AWS::Backup::ReportPlan",
    "Properties": {
      "ReportPlanName": "my-report-plan",
      "ReportPlanDescription": "test report plan",
      "ReportDeliveryChannel": {
        "S3BucketName": "my-report-bucket",
        "Formats": ["CSV"]
      },
      "ReportSetting": {
        "ReportTemplate": "BACKUP_JOB_REPORT"
      }
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "RP"}},
  "Arn": {"Value": {"Fn::GetAtt": ["RP", "ReportPlanArn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "bk-rp-stack", tmpl)
	assert.Equal(t, "my-report-plan", outputs["Ref"])
	assert.NotEmpty(t, outputs["Arn"])

	rp, err := backends.Backup.Backend.DescribeReportPlan("my-report-plan")
	require.NoError(t, err)
	assert.Equal(t, "test report plan", rp.ReportPlanDescription)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("bk-rp-stack")})
	require.NoError(t, err)

	_, err = backends.Backup.Backend.DescribeReportPlan("my-report-plan")
	require.Error(t, err)
}
