package app

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/docdb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/blackbirdworks/gopherstack/services/ecs"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/blackbirdworks/gopherstack/services/kafka"
	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/blackbirdworks/gopherstack/services/mq"
	"github.com/blackbirdworks/gopherstack/services/neptune"
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
			ec2B := ec2backend.NewInMemoryBackend("123456789012", "us-east-1")

			if tt.withEC2 {
				byName["EC2"] = ec2backend.NewHandler(ec2B)
			}

			wireSecurityGroupEnforcement(tt.enabled, byName)

			assert.Equal(t, tt.want, rdsH.Backend.SecurityGroupEnforcer() != nil)
			assert.Equal(t, tt.want, ec2B.SecurityGroupEnforcer() != nil)
		})
	}
}

func TestEnforceSecurityGroupsDefaultsOff(t *testing.T) {
	t.Parallel()

	assert.False(t, (&CLI{}).EnforceSecurityGroups)
}

// Every service that fronts container endpoints must accept the enforcer, or --enforce-security-groups would
// silently skip it.
var (
	_ securityGroupEnforced = (*docdb.Handler)(nil)
	_ securityGroupEnforced = (*kafka.Handler)(nil)
	_ securityGroupEnforced = (*elasticache.Handler)(nil)
	_ securityGroupEnforced = (*memorydb.Handler)(nil)
	_ securityGroupEnforced = (*mq.Handler)(nil)
	_ securityGroupEnforced = (*neptune.Handler)(nil)
	_ securityGroupEnforced = (*ecs.Handler)(nil)
	_ securityGroupEnforced = (*ec2backend.Handler)(nil)
	_ securityGroupEnforced = (*rdsbackend.Handler)(nil)
)

func TestEnforcedServicesList(t *testing.T) {
	t.Parallel()

	assert.ElementsMatch(t,
		[]string{"RDS", "EC2", "DocDB", "Kafka", "ElastiCache", "MemoryDB", "MQ", "Neptune", "ECS"},
		enforcedServices())
}
