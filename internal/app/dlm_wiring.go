package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	dlmbackend "github.com/blackbirdworks/gopherstack/services/dlm"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// wireDLMCompute lets DLM policies create and expire EC2 snapshots and AMIs.
func wireDLMCompute(byName map[string]service.Registerable) {
	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	if !ok {
		return
	}

	if h, hok := byName["DLM"].(*dlmbackend.Handler); hok {
		h.SetCompute(dlmbackend.NewEC2Compute(ec2H))
	}
}
