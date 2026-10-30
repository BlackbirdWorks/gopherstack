package cloudformation_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	cloudhsmv2backend "github.com/blackbirdworks/gopherstack/services/cloudhsmv2"
)

type hsmFakeNet struct{ seq atomic.Int64 }

func (f *hsmFakeNet) SubnetInfo(_ string, ids []string) map[string]cloudhsmv2backend.Subnet {
	out := map[string]cloudhsmv2backend.Subnet{}
	for _, id := range ids {
		if id == "subnet-a" {
			out[id] = cloudhsmv2backend.Subnet{VpcID: "vpc-1", Zone: "us-east-1a"}
		}
	}

	return out
}

func (f *hsmFakeNet) CreateSecurityGroup(_, _, _, _ string) (string, error) {
	return fmt.Sprintf("sg-%04d", f.seq.Add(1)), nil
}

func (f *hsmFakeNet) DeleteSecurityGroup(_, _ string) {}

func (f *hsmFakeNet) CreateENI(_, _, ip, _, _ string) (string, string, error) {
	n := f.seq.Add(1)
	if ip == "" {
		ip = fmt.Sprintf("10.0.0.%d", n)
	}

	return fmt.Sprintf("eni-%04d", n), ip, nil
}

func (f *hsmFakeNet) DeleteENI(_, _ string) {}

type cloudHSMStackEnv struct {
	cfn     *cfnsdk.Client
	backend *cloudhsmv2backend.InMemoryBackend
}

func newCloudHSMStackEnv(t *testing.T) cloudHSMStackEnv {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	hsmBackend := cloudhsmv2backend.NewInMemoryBackend("000000000000", "us-east-1")
	hsmBackend.SetNetwork(&hsmFakeNet{})
	hsmBackend.SetLifecycleDelay(time.Millisecond)
	backends.CloudHSMV2 = cloudhsmv2backend.NewHandler(hsmBackend)

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

	return cloudHSMStackEnv{cfn: newTestClientForBackend(t, backend), backend: hsmBackend}
}

const cloudHSMTemplate = `{
"Resources": {
  "Cluster": {"Type": "AWS::CloudHSM::Cluster", "Properties": {
    "HsmType": "hsm1.medium",
    "SubnetIds": ["subnet-a"],
    "BackupRetentionPolicy": {"Type": "DAYS", "Value": "%s"},
    "Tags": [{"Key": "Team", "Value": "%s"}]
  }}
},
"Outputs": {
  "ClusterRef": {"Value": {"Ref": "Cluster"}},
  "ClusterId": {"Value": {"Fn::GetAtt": ["Cluster", "ClusterId"]}},
  "Arn": {"Value": {"Fn::GetAtt": ["Cluster", "Arn"]}},
  "SecurityGroup": {"Value": {"Fn::GetAtt": ["Cluster", "SecurityGroup"]}},
  "VpcId": {"Value": {"Fn::GetAtt": ["Cluster", "VpcId"]}}
}
}`

func TestCreateStack_CloudHSM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, env cloudHSMStackEnv)
		name string
	}{
		{name: "create ref and getatt", run: func(t *testing.T, env cloudHSMStackEnv) {
			t.Helper()

			out := createStackAndGetOutputs(t, env.cfn, "hsm-create", fmt.Sprintf(cloudHSMTemplate, "10", "emu"))

			assert.Regexp(t, `^cluster-`, out["ClusterRef"])
			assert.Equal(t, out["ClusterRef"], out["ClusterId"])
			assert.Equal(t, "arn:aws:cloudhsm:us-east-1:000000000000:cluster/"+out["ClusterRef"], out["Arn"])
			assert.Equal(t, "vpc-1", out["VpcId"])
			assert.Regexp(t, `^sg-`, out["SecurityGroup"])

			p, err := env.backend.DescribeClusters(t.Context(), nil, "", nil)
			require.NoError(t, err)
			require.Len(t, p.Data, 1)
			assert.Equal(t, "10", p.Data[0].BackupRetentionPolicy.Value)

			tags, err := env.backend.ListTags(t.Context(), out["ClusterRef"], "", nil)
			require.NoError(t, err)
			require.Len(t, tags.Data, 1)
			assert.Equal(t, "emu", tags.Data[0].Value)
		}},
		{name: "update in place", run: func(t *testing.T, env cloudHSMStackEnv) {
			t.Helper()

			out := createStackAndGetOutputs(t, env.cfn, "hsm-update", fmt.Sprintf(cloudHSMTemplate, "10", "emu"))

			updated := fmt.Sprintf(cloudHSMTemplate, "20", "dev")
			_, err := env.cfn.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName: aws.String("hsm-update"), TemplateBody: aws.String(updated),
			})
			require.NoError(t, err)

			desc, err := env.cfn.DescribeStacks(
				t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("hsm-update")},
			)
			require.NoError(t, err)
			require.Len(t, desc.Stacks, 1)
			stack := desc.Stacks[0]
			reason := aws.ToString(stack.StackStatusReason)
			require.Equal(t, cfntypes.StackStatusUpdateComplete, stack.StackStatus, reason)

			p, err := env.backend.DescribeClusters(t.Context(), nil, "", nil)
			require.NoError(t, err)
			require.Len(t, p.Data, 1)
			assert.Equal(t, out["ClusterRef"], p.Data[0].ClusterID, "cluster was updated, not replaced")
			assert.Equal(t, "20", p.Data[0].BackupRetentionPolicy.Value)

			tags, err := env.backend.ListTags(t.Context(), out["ClusterRef"], "", nil)
			require.NoError(t, err)
			require.Len(t, tags.Data, 1)
			assert.Equal(t, "dev", tags.Data[0].Value)
		}},
		{name: "delete stack", run: func(t *testing.T, env cloudHSMStackEnv) {
			t.Helper()

			createStackAndGetOutputs(t, env.cfn, "hsm-delete", fmt.Sprintf(cloudHSMTemplate, "10", "emu"))

			_, err := env.cfn.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("hsm-delete")})
			require.NoError(t, err)

			p, err := env.backend.DescribeClusters(t.Context(), nil, "", nil)
			require.NoError(t, err)
			assert.Empty(t, p.Data)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				tc.run(t, newCloudHSMStackEnv(t))
			})
		})
	}
}
