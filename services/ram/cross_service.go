package ram

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"

	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
)

// ramSLRAWSServiceName is the AWS service principal RAM's service-linked
// role trusts (iam@v1.42.5 CreateServiceLinkedRole's AWSServiceName
// parameter; matches the real role's AssumeRolePolicyDocument principal).
const ramSLRAWSServiceName = "ram.amazonaws.com"

// ramSLRRoleName and ramSLRPath are the real, fixed name/path AWS uses for
// RAM's service-linked role -- unlike most services, RAM's role name
// ("AWSServiceRoleForResourceAccessManager") doesn't derive mechanically
// from the service principal, so it can't reuse IAM's generic
// CreateServiceLinkedRole(awsServiceName) name-derivation helper.
const (
	ramSLRRoleName = "AWSServiceRoleForResourceAccessManager"
	ramSLRPath     = "/aws-service-role/" + ramSLRAWSServiceName + "/"
	// ramSLRManagedPolicyARN is the AWS-managed policy real RAM's
	// service-linked role has attached.
	ramSLRManagedPolicyARN = "arn:aws:iam::aws:policy/aws-service-role/AWSResourceAccessManagerServiceRolePolicy"
)

// siblingServices is the subset of *CLI's method set this backend needs to
// reach the IAM backend, matched structurally against *CLI (no import of the
// top-level package, which would cycle) -- see services/grafana/cross_service.go,
// the pattern this file follows.
type siblingServices interface {
	GetIAMHandler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config value Provider.Init
// received, so this backend can resolve the IAM backend on demand. See
// services/grafana/cross_service.go's SetAppConfig for why this must be
// resolved lazily rather than at construction time.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.appConfig = cfg
}

func (b *InMemoryBackend) siblings() (siblingServices, bool) {
	s, ok := b.appConfig.(siblingServices)

	return s, ok
}

// iamBackend returns the emulator's IAM backend, if wired.
func (b *InMemoryBackend) iamBackend() (iambackend.StorageBackend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetIAMHandler().(*iambackend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.Backend, true
}

// EnableSharingWithAwsOrganization creates the RAM service-linked role in
// the IAM backend, matching real AWS: enabling org sharing creates
// AWSServiceRoleForResourceAccessManager as a side effect (this is what
// terraform-provider-aws's aws_ram_sharing_with_organization Read looks up
// via iam:GetRole). Idempotent: a role that already exists (e.g. a second
// EnableSharingWithAwsOrganization call) is left untouched, not an error.
func (b *InMemoryBackend) EnableSharingWithAwsOrganization() error {
	iamBk, ok := b.iamBackend()
	if !ok {
		return nil
	}

	role, err := iamBk.CreateRole(ramSLRRoleName, ramSLRPath, ramSLRTrustPolicy(), "")
	if err != nil {
		if errors.Is(err, iambackend.ErrRoleAlreadyExists) {
			return nil
		}

		return err
	}

	return iamBk.AttachRolePolicy(role.RoleName, ramSLRManagedPolicyARN)
}

// ramSLRTrustPolicy returns the AssumeRolePolicyDocument real AWS attaches
// to RAM's service-linked role, matching the shape IAM's own
// CreateServiceLinkedRole builds for a same-named AWSServiceName.
func ramSLRTrustPolicy() string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"` + ramSLRAWSServiceName + `"},"Action":"sts:AssumeRole"}]}`
}
