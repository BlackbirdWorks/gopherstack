package cloudformation_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestListTypes_PublisherAndAWSTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filters       *cfntypes.TypeFilters
		name          string
		wantPublisher string
		wantNames     []string
		wantEmpty     bool
	}{
		{
			name: "aws_types",
			filters: &cfntypes.TypeFilters{
				Category:       cfntypes.CategoryAwsTypes,
				TypeNamePrefix: aws.String("AWS::S3"),
			},
			wantNames: []string{"AWS::S3::Bucket"},
		},
		{
			name:      "aws_types_with_publisher",
			filters:   &cfntypes.TypeFilters{Category: cfntypes.CategoryAwsTypes, PublisherId: aws.String("pub-1")},
			wantEmpty: true,
		},
		{
			name:          "publisher_match",
			filters:       &cfntypes.TypeFilters{PublisherId: aws.String("pub-1")},
			wantNames:     []string{"Acme::Svc::Thing"},
			wantPublisher: "pub-1",
		},
		{
			name:      "publisher_other",
			filters:   &cfntypes.TypeFilters{PublisherId: aws.String("pub-2")},
			wantEmpty: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			_, err := client.ActivateType(t.Context(), &cfnsdk.ActivateTypeInput{
				TypeName:    aws.String("Acme::Svc::Thing"),
				Type:        cfntypes.ThirdPartyTypeResource,
				PublisherId: aws.String("pub-1"),
			})
			require.NoError(t, err)

			out, err := client.ListTypes(t.Context(), &cfnsdk.ListTypesInput{Filters: tc.filters})
			require.NoError(t, err)

			var names []string
			for _, s := range out.TypeSummaries {
				names = append(names, aws.ToString(s.TypeName))
				assert.Equal(t, tc.wantPublisher, aws.ToString(s.PublisherId))
			}

			if tc.wantEmpty {
				assert.Empty(t, names)

				return
			}

			assert.Equal(t, tc.wantNames, names)
		})
	}
}

func TestStackPolicy_NotActionNotResource(t *testing.T) {
	t.Parallel()

	withoutOther := `{"Resources":{
		"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig-bucket"}},
		"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":30}}
	}}`
	modifyQueue := strings.Replace(stackPolicyBaseTemplate, `"VisibilityTimeout":30`, `"VisibilityTimeout":60`, 1)

	tests := []struct {
		name     string
		policy   string
		template string
		wantErr  string
	}{
		{
			name: "not_resource_denies_others",
			policy: `{"Statement":[{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"},` +
				`{"Effect":"Deny","Action":"Update:*","Principal":"*","NotResource":"LogicalResourceId/MyQueue"}]}`,
			template: withoutOther,
			wantErr:  "OtherQueue",
		},
		{
			name: "not_resource_spares_listed",
			policy: `{"Statement":[{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"},` +
				`{"Effect":"Deny","Action":"Update:*","Principal":"*","NotResource":"LogicalResourceId/MyQueue"}]}`,
			template: modifyQueue,
		},
		{
			name: "not_action_denies_delete",
			policy: `{"Statement":[{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"},` +
				`{"Effect":"Deny","NotAction":"Update:Modify","Principal":"*","Resource":"*"}]}`,
			template: withoutOther,
			wantErr:  "Update:Delete",
		},
		{
			name: "not_action_spares_modify",
			policy: `{"Statement":[{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"},` +
				`{"Effect":"Deny","NotAction":"Update:Modify","Principal":"*","Resource":"*"}]}`,
			template: modifyQueue,
		},
		{
			name:     "allow_not_action_alone_denies_rest",
			policy:   `{"Statement":[{"Effect":"Allow","NotAction":"Update:Delete","Principal":"*","Resource":"*"}]}`,
			template: withoutOther,
			wantErr:  "Update:Delete",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			createPolicyStack(t, client, "notpol")
			_, err := client.SetStackPolicy(t.Context(), &cfnsdk.SetStackPolicyInput{
				StackName: aws.String("notpol"), StackPolicyBody: aws.String(tc.policy),
			})
			require.NoError(t, err)

			_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName: aws.String("notpol"), TemplateBody: aws.String(tc.template),
			})
			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestS3URLResolution(t *testing.T) {
	t.Parallel()

	policy := `{"Statement":[{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"}]}`
	tests := []struct {
		name    string
		url     string
		wantErr string
	}{
		{name: "virtual_hosted", url: "https://tpl.s3.us-east-1.amazonaws.com/dir/t.json"},
		{name: "path_style", url: "https://s3.amazonaws.com/tpl/dir/t.json"},
		{name: "localhost_path", url: "http://localhost:4566/tpl/dir/t.json"},
		{name: "missing_key", url: "https://tpl.s3.amazonaws.com/nope.json", wantErr: "S3 error"},
		{name: "no_object", url: "https://tpl.s3.amazonaws.com/", wantErr: "S3 error"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends(t)
			backend := cloudformation.NewInMemoryBackendWithConfig(
				"000000000000", "us-east-1", cloudformation.NewResourceCreator(backends),
			)
			client := newTestClientForBackend(t, backend)

			_, err := backends.S3.Backend.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String("tpl")})
			require.NoError(t, err)

			for key, body := range map[string]string{"dir/t.json": simpleTemplate, "pol.json": policy} {
				_, err = backends.S3.Backend.PutObject(t.Context(), &awss3.PutObjectInput{
					Bucket: aws.String("tpl"), Key: aws.String(key), Body: strings.NewReader(body),
				})
				require.NoError(t, err)
			}

			_, err = client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName: aws.String("urlstack"), TemplateURL: aws.String(tc.url),
			})
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)

			got, err := client.GetTemplate(t.Context(), &cfnsdk.GetTemplateInput{StackName: aws.String("urlstack")})
			require.NoError(t, err)
			assert.JSONEq(t, simpleTemplate, aws.ToString(got.TemplateBody))

			_, err = client.SetStackPolicy(t.Context(), &cfnsdk.SetStackPolicyInput{
				StackName:      aws.String("urlstack"),
				StackPolicyURL: aws.String("https://tpl.s3.amazonaws.com/pol.json"),
			})
			require.NoError(t, err)

			stored, err := client.GetStackPolicy(
				t.Context(),
				&cfnsdk.GetStackPolicyInput{StackName: aws.String("urlstack")},
			)
			require.NoError(t, err)
			assert.JSONEq(t, policy, aws.ToString(stored.StackPolicyBody))
		})
	}
}

func TestS3URLResolution_NoS3Backend(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("nos3"), TemplateURL: aws.String("https://b.s3.amazonaws.com/k.json"),
	})
	require.ErrorContains(t, err, "S3 error")
}

func TestCreateStackInstances_AccountsURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		file string
		want []string
	}{
		{name: "comma_separated", file: "111111111111,222222222222", want: []string{"111111111111", "222222222222"}},
		{
			name: "newline_separated",
			file: "333333333333\n444444444444\n",
			want: []string{"333333333333", "444444444444"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends(t)
			backend := cloudformation.NewInMemoryBackendWithConfig(
				"000000000000", "us-east-1", cloudformation.NewResourceCreator(backends),
			)
			client := newTestClientForBackend(t, backend)

			_, err := backends.S3.Backend.CreateBucket(
				t.Context(),
				&awss3.CreateBucketInput{Bucket: aws.String("acct")},
			)
			require.NoError(t, err)
			_, err = backends.S3.Backend.PutObject(t.Context(), &awss3.PutObjectInput{
				Bucket: aws.String("acct"), Key: aws.String("a.csv"), Body: strings.NewReader(tc.file),
			})
			require.NoError(t, err)

			_, err = client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
				StackSetName: aws.String("ss"), TemplateBody: aws.String(simpleTemplate),
			})
			require.NoError(t, err)
			_, err = client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
				StackSetName: aws.String("ss"),
				Regions:      []string{"us-east-1"},
				DeploymentTargets: &cfntypes.DeploymentTargets{
					AccountsUrl: aws.String("https://acct.s3.amazonaws.com/a.csv"),
				},
			})
			require.NoError(t, err)

			out, err := client.ListStackInstances(
				t.Context(),
				&cfnsdk.ListStackInstancesInput{StackSetName: aws.String("ss")},
			)
			require.NoError(t, err)

			got := make([]string, 0, len(out.Summaries))
			for _, s := range out.Summaries {
				got = append(got, aws.ToString(s.Account))
			}

			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestNestedStack_TemplateURL(t *testing.T) {
	t.Parallel()

	backends := newServiceBackends(t)
	backend := cloudformation.NewInMemoryBackendWithConfig(
		"000000000000", "us-east-1", cloudformation.NewResourceCreator(backends),
	)
	client := newTestClientForBackend(t, backend)

	_, err := backends.S3.Backend.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String("nested")})
	require.NoError(t, err)
	_, err = backends.S3.Backend.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("nested"), Key: aws.String("child.json"), Body: strings.NewReader(simpleTemplate),
	})
	require.NoError(t, err)

	_, err = client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("parent"),
		TemplateBody: aws.String(`{"Resources":{"Child":{"Type":"AWS::CloudFormation::Stack",` +
			`"Properties":{"TemplateURL":"https://nested.s3.amazonaws.com/child.json"}}}}`),
	})
	require.NoError(t, err)

	res, err := client.DescribeStackResources(t.Context(), &cfnsdk.DescribeStackResourcesInput{
		StackName: aws.String("Child"),
	})
	require.NoError(t, err)
	require.Len(t, res.StackResources, 1)
	assert.Equal(t, "MyBucket", aws.ToString(res.StackResources[0].LogicalResourceId))
}
