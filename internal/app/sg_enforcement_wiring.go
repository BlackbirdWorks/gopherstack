package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
)

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

	if h, hok := byName["RDS"].(*rdsbackend.Handler); hok {
		h.SetSecurityGroupEnforcer(enf)
	}
}
