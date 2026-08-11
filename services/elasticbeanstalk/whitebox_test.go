package elasticbeanstalk

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnvironmentResourceDescType_XMLShape marshal-tests
// environmentResourceDescType directly (gopherstack-5pim): the backend only
// ever constructs 0 or 1 elements per list, so this is the only way to prove
// the multi-item shape without fabricating backend behavior.
func TestEnvironmentResourceDescType_XMLShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		in   environmentResourceDescType
	}{
		{
			name: "single item per list matches pre-fix byte shape",
			in: environmentResourceDescType{
				EnvironmentName:      "my-env",
				AutoScalingGroups:    []asgMemberType{{Name: "my-env-asg"}},
				Instances:            []instanceMemberType{{ID: "i-1234"}},
				LaunchConfigurations: []launchConfigMemberType{{Name: "my-env-lc"}},
				LoadBalancers:        []loadBalancerMemberType{{Name: "my-env-lb"}},
			},
			want: `<environmentResourceDescType><EnvironmentName>my-env</EnvironmentName>` +
				`<AutoScalingGroups><member><Name>my-env-asg</Name></member></AutoScalingGroups>` +
				`<Instances><member><Id>i-1234</Id></member></Instances>` +
				`<LaunchConfigurations><member><Name>my-env-lc</Name></member></LaunchConfigurations>` +
				`<LaunchTemplates></LaunchTemplates>` +
				`<LoadBalancers><member><Name>my-env-lb</Name></member></LoadBalancers>` +
				`<Queues></Queues><Triggers></Triggers></environmentResourceDescType>`,
		},
		{
			name: "two autoscaling groups repeat member per element",
			in: environmentResourceDescType{
				EnvironmentName:   "my-env",
				AutoScalingGroups: []asgMemberType{{Name: "asg-1"}, {Name: "asg-2"}},
			},
			want: `<environmentResourceDescType><EnvironmentName>my-env</EnvironmentName>` +
				`<AutoScalingGroups><member><Name>asg-1</Name></member><member><Name>asg-2</Name></member></AutoScalingGroups>` +
				`<Instances></Instances><LaunchConfigurations></LaunchConfigurations>` +
				`<LaunchTemplates></LaunchTemplates><LoadBalancers></LoadBalancers>` +
				`<Queues></Queues><Triggers></Triggers></environmentResourceDescType>`,
		},
		{
			name: "two queues carry both name and url per member",
			in: environmentResourceDescType{
				EnvironmentName: "my-env",
				Queues: []queueMemberType{
					{Name: "q1", URL: "https://sqs.us-east-1.amazonaws.com/q1"},
					{Name: "q2", URL: "https://sqs.us-east-1.amazonaws.com/q2"},
				},
			},
			want: `<environmentResourceDescType><EnvironmentName>my-env</EnvironmentName>` +
				`<AutoScalingGroups></AutoScalingGroups><Instances></Instances>` +
				`<LaunchConfigurations></LaunchConfigurations><LaunchTemplates></LaunchTemplates>` +
				`<LoadBalancers></LoadBalancers>` +
				`<Queues><member><Name>q1</Name><URL>https://sqs.us-east-1.amazonaws.com/q1</URL></member>` +
				`<member><Name>q2</Name><URL>https://sqs.us-east-1.amazonaws.com/q2</URL></member></Queues>` +
				`<Triggers></Triggers></environmentResourceDescType>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := xml.Marshal(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(out))
		})
	}
}

// TestDescribeEnvironmentResources_SDKRoundTrip_MultiItem proves the fixed
// shape survives a real client, not just this package's own marshaler
// (gopherstack-5pim). The backend cannot construct more than one element per
// list (handleDescribeEnvironmentResources), so this serves a
// hand-constructed multi-item envelope directly rather than going through
// the handler -- it is not fabricating backend behavior, only proving the
// wire shape a future multi-item backend would also produce.
func TestDescribeEnvironmentResources_SDKRoundTrip_MultiItem(t *testing.T) {
	t.Parallel()

	resp := describeEnvironmentResourcesResponse{
		Xmlns: ebXMLNS,
		DescribeEnvironmentResourcesResult: describeEnvironmentResourcesResult{
			EnvironmentResources: environmentResourceDescType{
				EnvironmentName: "my-env",
				AutoScalingGroups: []asgMemberType{
					{Name: "asg-1"}, {Name: "asg-2"},
				},
				Instances: []instanceMemberType{
					{ID: "i-1"}, {ID: "i-2"}, {ID: "i-3"},
				},
			},
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-env-resources"},
	}
	body, err := xml.Marshal(resp)
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := ebsdk.NewFromConfig(cfg, func(o *ebsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	out, err := client.DescribeEnvironmentResources(t.Context(), &ebsdk.DescribeEnvironmentResourcesInput{
		EnvironmentName: aws.String("my-env"),
	})
	require.NoError(t, err)

	require.Len(t, out.EnvironmentResources.AutoScalingGroups, 2)
	assert.Equal(t, "asg-1", aws.ToString(out.EnvironmentResources.AutoScalingGroups[0].Name))
	assert.Equal(t, "asg-2", aws.ToString(out.EnvironmentResources.AutoScalingGroups[1].Name))

	require.Len(t, out.EnvironmentResources.Instances, 3)
	assert.Equal(t, "i-1", aws.ToString(out.EnvironmentResources.Instances[0].Id))
	assert.Equal(t, "i-2", aws.ToString(out.EnvironmentResources.Instances[1].Id))
	assert.Equal(t, "i-3", aws.ToString(out.EnvironmentResources.Instances[2].Id))
}
