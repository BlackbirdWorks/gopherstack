package elasticbeanstalk_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_Batch1ApplicationVersionState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		create            string
		contains          []string
		verifyApplication bool
	}{
		{
			name: "source bundle and processing",
			create: "&ApplicationName=bundle-app&VersionLabel=v1&Process=true&AutoCreateApplication=true" +
				"&SourceBundle.S3Bucket=src-bucket&SourceBundle.S3Key=releases%2Fapp.zip",
			contains: []string{
				"<Status>Processed</Status>", "<S3Bucket>src-bucket</S3Bucket>",
				"<S3Key>releases/app.zip</S3Key>",
			},
		},
		{
			name: "codecommit source and automatic application",
			create: "&ApplicationName=source-app&VersionLabel=v2&AutoCreateApplication=true&Process=true" +
				"&SourceBuildInformation.SourceType=CodeCommit" +
				"&SourceBuildInformation.SourceRepository=demo-repo" +
				"&SourceBuildInformation.SourceLocation=main",
			contains: []string{
				"<Status>Processed</Status>", "<SourceType>CodeCommit</SourceType>",
				"<SourceRepository>demo-repo</SourceRepository>", "<SourceLocation>main</SourceLocation>",
			},
			verifyApplication: true,
		},
		{
			name:     "sample application remains unprocessed by default",
			create:   "&ApplicationName=sample-app&VersionLabel=v3&AutoCreateApplication=true",
			contains: []string{"<Status>Unprocessed</Status>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, "Version=2010-12-01&Action=CreateApplicationVersion"+tt.create)
			assert.Equal(t, http.StatusOK, rec.Code)
			for _, expected := range tt.contains {
				assert.Contains(t, rec.Body.String(), expected)
			}
			if tt.verifyApplication {
				apps := postEBForm(
					t,
					h,
					"Version=2010-12-01&Action=DescribeApplications&ApplicationNames.member.1=source-app",
				)
				assert.Contains(t, apps.Body.String(), "<ApplicationName>source-app</ApplicationName>")
			}
		})
	}
}

func TestHandler_Batch1EnvironmentState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		create   string
		update   string
		action   string
		contains []string
	}{
		{
			name: "create selection fields and tier",
			create: "&CNAMEPrefix=customer-url&PlatformArn=arn%3Aaws%3Aelasticbeanstalk%3Aus-east-1%3Aplatform%2Fgo" +
				"&VersionLabel=v1&Tier.Name=Worker&Tier.Type=SQS%2FHTTP&Tier.Version=1.1",
			action: "DescribeEnvironments&ApplicationName=app&EnvironmentNames.member.1=env1",
			contains: []string{
				"customer-url.us-east-1.elasticbeanstalk.com", "<PlatformArn>arn:aws:elasticbeanstalk:",
				"<VersionLabel>v1</VersionLabel>", "<Name>Worker</Name>", "<Version>1.1</Version>",
			},
		},
		{
			name: "update options and remove original",
			create: "&SolutionStackName=stack-a&OptionSettings.member.1.Namespace=aws%3Aec2%3Avpc" +
				"&OptionSettings.member.1.OptionName=VPCId&OptionSettings.member.1.Value=vpc-old" +
				"&OptionSettings.member.2.Namespace=aws%3Aelasticbeanstalk%3Aenvironment" +
				"&OptionSettings.member.2.OptionName=EnvironmentType&OptionSettings.member.2.Value=SingleInstance",
			update: "&SolutionStackName=stack-b&OptionSettings.member.1.Namespace=aws%3Aec2%3Avpc" +
				"&OptionSettings.member.1.OptionName=Subnets&OptionSettings.member.1.Value=subnet-new" +
				"&OptionsToRemove.member.1.Namespace=aws%3Aec2%3Avpc&OptionsToRemove.member.1.OptionName=VPCId",
			action: "DescribeConfigurationSettings&ApplicationName=app&EnvironmentName=env1",
			contains: []string{
				"<SolutionStackName>stack-b</SolutionStackName>", "<OptionName>Subnets</OptionName>",
				"<Value>subnet-new</Value>", "<OptionName>EnvironmentType</OptionName>",
				"<Value>SingleInstance</Value>",
			},
		},
		{
			name:   "worker topology includes queue",
			create: "&Tier.Name=Worker",
			action: "DescribeEnvironmentResources&EnvironmentName=env1",
			contains: []string{
				"<AutoScalingGroups><member><Name>env1-asg</Name>", "<Queues><member><URL>https://sqs.",
				"env1</URL>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			create := "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1" + tt.create
			assert.Equal(t, http.StatusOK, postEBForm(t, h, create).Code)
			if tt.update != "" {
				update := "Version=2010-12-01&Action=UpdateEnvironment&ApplicationName=app&EnvironmentName=env1" + tt.update
				assert.Equal(t, http.StatusOK, postEBForm(t, h, update).Code)
			}

			rec := postEBForm(t, h, "Version=2010-12-01&Action="+tt.action)
			assert.Equal(t, http.StatusOK, rec.Code)
			for _, expected := range tt.contains {
				assert.Contains(t, rec.Body.String(), expected)
			}
			if tt.name == "update options and remove original" {
				assert.NotContains(t, rec.Body.String(), "vpc-old")
			}
		})
	}
}
