package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestAuthorizeSecurityGroupIngress_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		rule    ec2.SecurityGroupRule
	}{
		{
			name: "valid_tcp_rule",
			rule: ec2.SecurityGroupRule{Protocol: "tcp", FromPort: 80, ToPort: 80, IPRange: "0.0.0.0/0"},
		},
		{
			name: "valid_all_protocol",
			rule: ec2.SecurityGroupRule{Protocol: "-1", IPRange: "10.0.0.0/8"},
		},
		{
			name: "valid_icmp",
			rule: ec2.SecurityGroupRule{Protocol: "icmp", FromPort: -1, ToPort: -1, IPRange: "0.0.0.0/0"},
		},
		{
			name:    "invalid_protocol",
			rule:    ec2.SecurityGroupRule{Protocol: "banana", FromPort: 80, ToPort: 80, IPRange: "0.0.0.0/0"},
			wantErr: ec2.ErrInvalidParameter,
		},
		{
			name:    "from_greater_than_to",
			rule:    ec2.SecurityGroupRule{Protocol: "tcp", FromPort: 443, ToPort: 80, IPRange: "0.0.0.0/0"},
			wantErr: ec2.ErrInvalidParameter,
		},
		{
			name:    "port_out_of_range",
			rule:    ec2.SecurityGroupRule{Protocol: "tcp", FromPort: 0, ToPort: 70000, IPRange: "0.0.0.0/0"},
			wantErr: ec2.ErrInvalidParameter,
		},
		{
			name:    "invalid_cidr",
			rule:    ec2.SecurityGroupRule{Protocol: "tcp", FromPort: 80, ToPort: 80, IPRange: "not-a-cidr"},
			wantErr: ec2.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			sg, err := b.CreateSecurityGroup("sg-"+tt.name, "test", "vpc-default")
			require.NoError(t, err)

			err = b.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{tt.rule})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAuthorizeSecurityGroupIngress_Duplicate(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	sg, err := b.CreateSecurityGroup("dup-sg", "test", "vpc-default")
	require.NoError(t, err)

	rule := ec2.SecurityGroupRule{Protocol: "tcp", FromPort: 22, ToPort: 22, IPRange: "0.0.0.0/0"}

	require.NoError(t, b.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{rule}))

	// Re-authorizing the identical rule must fail with InvalidPermission.Duplicate.
	err = b.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{rule})
	require.ErrorIs(t, err, ec2.ErrDuplicatePermission)

	// The duplicate must not have been appended.
	sgs := b.DescribeSecurityGroups([]string{sg.ID})
	require.Len(t, sgs, 1)
	assert.Len(t, sgs[0].IngressRules, 1)
}

func TestAuthorizeSecurityGroupEgress_Validation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	sg, err := b.CreateSecurityGroup("egress-sg", "test", "vpc-default")
	require.NoError(t, err)

	// Invalid egress rule is rejected.
	err = b.AuthorizeSecurityGroupEgress(sg.ID, []ec2.SecurityGroupRule{
		{Protocol: "tcp", FromPort: 100, ToPort: 50, IPRange: "0.0.0.0/0"},
	})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	// Valid egress rule succeeds.
	err = b.AuthorizeSecurityGroupEgress(sg.ID, []ec2.SecurityGroupRule{
		{Protocol: "tcp", FromPort: 443, ToPort: 443, IPRange: "0.0.0.0/0"},
	})
	require.NoError(t, err)
}
