package main

import (
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

// buildServiceRoleAuthorizer returns the evaluator for service-initiated calls under IAM enforcement.
func buildServiceRoleAuthorizer(services []service.Registerable) roleauth.Authorizer {
	byName := serviceByName(services)

	iamH, ok := byName["IAM"].(*iambackend.Handler)
	if !ok {
		return nil
	}

	roles, ok := iamH.Backend.(iambackend.RoleBackend)
	if !ok {
		return nil
	}

	var trust iambackend.ServiceRoleTrust

	if stsH, stsOk := byName["STS"].(*stsbackend.Handler); stsOk {
		if stsBk, bkOk := stsH.Backend.(*stsbackend.InMemoryBackend); bkOk {
			trust = stsBk
		}
	}

	return iambackend.NewRoleAuthorizer(roles, trust, buildResourcePolicyProviders(services))
}

// wireServiceRoleAuthorizer runs Step Functions legacy integrations, Scheduler, EventBridge
// rule targets and Pipes under their customer role's policies when IAM enforcement is on.
func wireServiceRoleAuthorizer(services []service.Registerable, enforceIAM bool) {
	if !enforceIAM {
		return
	}

	auth := buildServiceRoleAuthorizer(services)
	if auth == nil {
		return
	}

	byName := serviceByName(services)

	if h, ok := byName["StepFunctions"].(*sfnbackend.Handler); ok {
		if bk, bkOk := h.Backend.(*sfnbackend.InMemoryBackend); bkOk {
			bk.SetRoleAuthorizer(auth)
		}
	}

	if h, ok := byName["Scheduler"].(*schedulerbackend.Handler); ok {
		h.GetRunner().SetRoleAuthorizer(auth)
	}

	if h, ok := byName["EventBridge"].(*ebbackend.Handler); ok {
		if bk, bkOk := h.Backend.(*ebbackend.InMemoryBackend); bkOk {
			bk.SetRoleAuthorizer(auth)
		}
	}

	if h, ok := byName["Pipes"].(*pipesbackend.Handler); ok {
		h.GetRunner().SetRoleAuthorizer(auth)
	}
}
