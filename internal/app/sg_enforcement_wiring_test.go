package app

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
)

func TestWireSecurityGroupEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
		withEC2 bool
		want    bool
	}{
		{"disabled leaves rds open", false, true, false},
		{"enabled installs enforcer", true, true, true},
		{"enabled without ec2 stays open", true, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rdsH := rdsbackend.NewHandler(rdsbackend.NewInMemoryBackend("123456789012", "us-east-1"))
			byName := map[string]service.Registerable{"RDS": rdsH}

			if tt.withEC2 {
				byName["EC2"] = ec2backend.NewHandler(ec2backend.NewInMemoryBackend("123456789012", "us-east-1"))
			}

			wireSecurityGroupEnforcement(tt.enabled, byName)

			assert.Equal(t, tt.want, rdsH.Backend.SecurityGroupEnforcer() != nil)
		})
	}
}

func TestEnforceSecurityGroupsDefaultsOff(t *testing.T) {
	t.Parallel()

	assert.False(t, (&CLI{}).EnforceSecurityGroups)
}
