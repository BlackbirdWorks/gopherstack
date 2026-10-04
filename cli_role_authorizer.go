package main

import (
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
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

// wireServiceRoleAuthorizer runs service-initiated calls under their customer role's policies
// and destination resource policies when IAM enforcement is on.
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

	wireDeliveryRoleAuthorizer(byName, auth)
}

type roleAuthSetter interface {
	SetRoleAuthorizer(a roleauth.Authorizer)
}

// wireDeliveryRoleAuthorizer applies the authorizer to Firehose, CloudWatch Logs, IoT, SNS and SQS
// deliveries and to S3 notification validation.
func wireDeliveryRoleAuthorizer(byName map[string]service.Registerable, auth roleauth.Authorizer) {
	var backends []any

	if h, ok := byName["Firehose"].(*firehosebackend.Handler); ok {
		backends = append(backends, h.Backend)
	}

	if h, ok := byName["CloudWatchLogs"].(*cwlogsbackend.Handler); ok {
		backends = append(backends, h.Backend)
	}

	if h, ok := byName["IoT"].(*iotbackend.Handler); ok {
		backends = append(backends, h.Backend)
	}

	if h, ok := byName["SNS"].(*snsbackend.Handler); ok {
		backends = append(backends, h.Backend)
	}

	if h, ok := byName["SQS"].(*sqsbackend.Handler); ok {
		backends = append(backends, h.Backend)
	}

	for _, bk := range backends {
		if setter, ok := bk.(roleAuthSetter); ok {
			setter.SetRoleAuthorizer(auth)
		}
	}

	if h, ok := byName["S3"].(*s3backend.S3Handler); ok {
		h.SetNotificationAuthorizer(auth)
	}
}
