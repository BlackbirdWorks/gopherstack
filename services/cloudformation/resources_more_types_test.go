package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	wafv2backend "github.com/blackbirdworks/gopherstack/services/wafv2"
)

// newMoreTypesServiceBackends creates a ServiceBackends with all phase-4 backends populated.
// IAM and EC2 backends are already set by newDependentServiceBackends (via newExtendedServiceBackends).
func newMoreTypesServiceBackends(t *testing.T) *cloudformation.ServiceBackends {
	t.Helper()

	b := newDependentServiceBackends(t)
	b.ELBv2 = elbv2backend.NewHandler(elbv2backend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.WAFv2 = wafv2backend.NewHandler(wafv2backend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.Backup = backupbackend.NewHandler(backupbackend.NewInMemoryBackend("000000000000", "us-east-1"))

	return b
}

// TestResourceCreator_MoreTypes_NilBackends ensures all phase-4 resource types return a stub
// physical ID when the corresponding backend is nil.
func TestResourceCreator_MoreTypes_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name:         "ec2_instance",
			logicalID:    "MyInstance",
			resourceType: "AWS::EC2::Instance",
			props:        map[string]any{"ImageId": "ami-test", "InstanceType": "t3.micro"},
		},
		{
			name:         "ec2_vpc_gateway_attachment",
			logicalID:    "MyIGWAttach",
			resourceType: "AWS::EC2::VPCGatewayAttachment",
			props:        map[string]any{"VpcId": "vpc-test", "InternetGatewayId": "igw-test"},
		},
		{
			name:         "ec2_subnet_rta",
			logicalID:    "MyRTA",
			resourceType: "AWS::EC2::SubnetRouteTableAssociation",
			props:        map[string]any{"SubnetId": "subnet-test", "RouteTableId": "rtb-test"},
		},
		{
			name:         "iam_user",
			logicalID:    "MyUser",
			resourceType: "AWS::IAM::User",
			props:        map[string]any{"UserName": "stub-user"},
		},
		{
			name:         "iam_group",
			logicalID:    "MyGroup",
			resourceType: "AWS::IAM::Group",
			props:        map[string]any{"GroupName": "stub-group"},
		},
		{
			name:         "elbv2_load_balancer",
			logicalID:    "MyLB",
			resourceType: "AWS::ElasticLoadBalancingV2::LoadBalancer",
			props:        map[string]any{"Name": "stub-lb"},
		},
		{
			name:         "elbv2_target_group",
			logicalID:    "MyTG",
			resourceType: "AWS::ElasticLoadBalancingV2::TargetGroup",
			props:        map[string]any{"Name": "stub-tg"},
		},
		{
			name:         "elbv2_listener",
			logicalID:    "MyListener",
			resourceType: "AWS::ElasticLoadBalancingV2::Listener",
			props:        map[string]any{"LoadBalancerArn": "arn:stub", "Protocol": "HTTP", "Port": float64(80)},
		},
		{
			name:         "wafv2_web_acl",
			logicalID:    "MyWebACL",
			resourceType: "AWS::WAFv2::WebACL",
			props:        map[string]any{"Name": "stub-acl", "Scope": "REGIONAL"},
		},
		{
			name:         "wafv2_ip_set",
			logicalID:    "MyIPSet",
			resourceType: "AWS::WAFv2::IPSet",
			props:        map[string]any{"Name": "stub-ipset", "Scope": "REGIONAL"},
		},
		{
			name:         "wafv2_rule_group",
			logicalID:    "MyRuleGroup",
			resourceType: "AWS::WAFv2::RuleGroup",
			props:        map[string]any{"Name": "stub-rg", "Scope": "REGIONAL", "Capacity": float64(100)},
		},
		{
			name:         "backup_vault",
			logicalID:    "MyVault",
			resourceType: "AWS::Backup::BackupVault",
			props:        map[string]any{"BackupVaultName": "stub-vault"},
		},
		{
			name:         "backup_plan",
			logicalID:    "MyPlan",
			resourceType: "AWS::Backup::BackupPlan",
			props:        map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "stub-plan"}},
		},
		{
			name:         "backup_selection",
			logicalID:    "MySelection",
			resourceType: "AWS::Backup::BackupSelection",
			props: map[string]any{
				"BackupPlanId": "plan-id",
				"BackupSelection": map[string]any{
					"SelectionName": "stub-sel",
					"IamRoleArn":    "arn:aws:iam::000000000000:role/AWSBackupDefault",
				},
			},
		},
		{
			name:         "rds_db_cluster",
			logicalID:    "MyCluster",
			resourceType: "AWS::RDS::DBCluster",
			props:        map[string]any{"DBClusterIdentifier": "stub-cluster", "Engine": "aurora-postgresql"},
		},
		{
			name:         "rds_db_cluster_pg",
			logicalID:    "MyClusterPG",
			resourceType: "AWS::RDS::DBClusterParameterGroup",
			props: map[string]any{
				"DBClusterParameterGroupName": "stub-cpg",
				"Family":                      "aurora-postgresql15",
				"Description":                 "Stub cluster parameter group",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// newServiceBackends() leaves all phase-4 backends nil → stub path.
			backends := newServiceBackends()
			backends.EC2 = nil // also nil EC2 backend for EC2 stubs
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			// Delete should also be a no-op without a backend.
			err = rc.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}

// TestResourceCreator_MoreTypes_RealBackends validates create and delete for all phase-4
// resource types with real in-memory backends.
func TestResourceCreator_MoreTypes_RealBackends(t *testing.T) {
	t.Parallel()

	backends := newMoreTypesServiceBackends(t)

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
		wantNotEmpty bool
	}{
		{
			name:         "iam_user",
			logicalID:    "MyUser",
			resourceType: "AWS::IAM::User",
			props:        map[string]any{"UserName": "unit-cfn-user"},
			wantNotEmpty: true,
		},
		{
			name:         "iam_group",
			logicalID:    "MyGroup",
			resourceType: "AWS::IAM::Group",
			props:        map[string]any{"GroupName": "unit-cfn-group"},
			wantNotEmpty: true,
		},
		{
			name:         "elbv2_load_balancer",
			logicalID:    "MyLB",
			resourceType: "AWS::ElasticLoadBalancingV2::LoadBalancer",
			props: map[string]any{
				"Name":   "unit-cfn-lb",
				"Scheme": "internet-facing",
				"Type":   "application",
			},
			wantNotEmpty: true,
		},
		{
			name:         "elbv2_target_group",
			logicalID:    "MyTG",
			resourceType: "AWS::ElasticLoadBalancingV2::TargetGroup",
			props: map[string]any{
				"Name":     "unit-cfn-tg",
				"Protocol": "HTTP",
				"Port":     float64(80),
				"VpcId":    "vpc-test",
			},
			wantNotEmpty: true,
		},
		{
			name:         "wafv2_web_acl",
			logicalID:    "MyWebACL",
			resourceType: "AWS::WAFv2::WebACL",
			props: map[string]any{
				"Name":          "unit-cfn-acl",
				"Scope":         "REGIONAL",
				"DefaultAction": "ALLOW",
			},
			wantNotEmpty: true,
		},
		{
			name:         "wafv2_ip_set",
			logicalID:    "MyIPSet",
			resourceType: "AWS::WAFv2::IPSet",
			props: map[string]any{
				"Name":             "unit-cfn-ipset",
				"Scope":            "REGIONAL",
				"IPAddressVersion": "IPV4",
				"Addresses":        []any{"10.0.0.0/8"},
			},
			wantNotEmpty: true,
		},
		{
			name:         "wafv2_rule_group",
			logicalID:    "MyRuleGroup",
			resourceType: "AWS::WAFv2::RuleGroup",
			props: map[string]any{
				"Name":     "unit-cfn-rg",
				"Scope":    "REGIONAL",
				"Capacity": float64(100),
			},
			wantNotEmpty: true,
		},
		{
			name:         "backup_vault",
			logicalID:    "MyVault",
			resourceType: "AWS::Backup::BackupVault",
			props:        map[string]any{"BackupVaultName": "unit-cfn-vault"},
			wantNotEmpty: true,
		},
		{
			name:         "backup_plan",
			logicalID:    "MyPlan",
			resourceType: "AWS::Backup::BackupPlan",
			props: map[string]any{
				"BackupPlan": map[string]any{
					"BackupPlanName": "unit-cfn-plan",
				},
			},
			wantNotEmpty: true,
		},
		{
			name:         "rds_db_cluster",
			logicalID:    "MyRDSCluster",
			resourceType: "AWS::RDS::DBCluster",
			props: map[string]any{
				"DBClusterIdentifier": "unit-cfn-cluster",
				"Engine":              "aurora-postgresql",
				"MasterUsername":      "admin",
			},
			wantNotEmpty: true,
		},
		{
			name:         "rds_db_cluster_pg",
			logicalID:    "MyClusterPG",
			resourceType: "AWS::RDS::DBClusterParameterGroup",
			props: map[string]any{
				"DBClusterParameterGroupName": "unit-cfn-cpg",
				"Family":                      "aurora-postgresql15",
				"Description":                 "Unit test cluster parameter group",
			},
			wantNotEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			err = rc.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}
