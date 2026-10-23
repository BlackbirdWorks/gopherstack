package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// securityGroupEnforced is implemented by every service that fronts its container or embedded endpoints
// with a security-group check.
type securityGroupEnforced interface {
	SetSecurityGroupEnforcer(e *sgfilter.Enforcer)
}

// enforcedServices lists the registered names of those services. Lambda is absent: AWS does not put
// security groups in front of function invocation.
func enforcedServices() []string {
	return []string{"RDS", "EC2", "DocDB", "Kafka", "ElastiCache", "MemoryDB", "MQ", "Neptune", "ECS"}
}

// wireSecurityGroupEnforcement makes container-backed endpoints refuse clients that EC2 security groups do
// not admit. It does nothing unless enabled, and needs EC2 as the source of group rules.
func wireSecurityGroupEnforcement(enabled bool, byName map[string]service.Registerable) {
	if !enabled {
		return
	}

	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	if !ok {
		return
	}

	enf := sgfilter.New()
	enf.SetSource(ec2H.SecurityGroupSource())

	for _, name := range enforcedServices() {
		if h, hok := byName[name].(securityGroupEnforced); hok {
			h.SetSecurityGroupEnforcer(enf)
		}
	}
}
