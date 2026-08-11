package grafana

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	identitystorebackend "github.com/blackbirdworks/gopherstack/services/identitystore"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	ssoadminbackend "github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// siblingServices is the subset of *CLI's method set this backend needs to
// reach the IAM/EC2/Organizations/SSO Admin/Identity Store backends and the
// shared chaos fault store, so that CreateWorkspace/UpdateWorkspace can
// reject a WorkspaceRoleArn/VpcConfiguration/WorkspaceOrganizationalUnits
// that doesn't exist -- matching real AWS Managed Grafana rejecting
// references it can't resolve. Matched structurally against *CLI (no import
// of the top-level package, which would cycle); see SetAppConfig's doc
// comment for why this is resolved lazily instead of at construction time.
type siblingServices interface {
	GetIAMHandler() service.Registerable
	GetEC2Handler() service.Registerable
	GetOrganizationsHandler() service.Registerable
	GetSsoAdminHandler() service.Registerable
	GetIdentityStoreHandler() service.Registerable
	GetFaultStore() *chaos.FaultStore
}

// SetAppConfig records the service.AppContext.Config value Provider.Init
// received, so this backend can resolve sibling service handlers on demand.
//
// It cannot resolve them at Init time: gopherstack's startup sequence
// constructs every service provider independently in one pass (see cli.go's
// initIndependentServices) and only wires each provider's own CLI-struct
// field afterward, once every provider has returned. Capturing the *CLI
// pointer now and calling its Get*Handler methods lazily -- on the first
// real request, well after startup has finished -- gets around that
// ordering without needing a second, cross-service-aware init phase.
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

// ec2Backend returns the emulator's EC2 backend, if wired.
func (b *InMemoryBackend) ec2Backend() (ec2backend.Backend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetEC2Handler().(*ec2backend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.Backend, true
}

// organizationsBackend returns the emulator's Organizations backend, if wired.
func (b *InMemoryBackend) organizationsBackend() (organizationsbackend.StorageBackend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetOrganizationsHandler().(*organizationsbackend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.Backend, true
}

// ssoadminBackend returns the emulator's SSO Admin backend, if wired.
func (b *InMemoryBackend) ssoadminBackend() (ssoadminbackend.StorageBackend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetSsoAdminHandler().(*ssoadminbackend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.Backend, true
}

// identitystoreBackend returns the emulator's Identity Store backend, if wired.
func (b *InMemoryBackend) identitystoreBackend() (*identitystorebackend.InMemoryBackend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetIdentityStoreHandler().(*identitystorebackend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.Backend, true
}

// chaosFaultStore returns the shared chaos fault store, if wired.
func (b *InMemoryBackend) chaosFaultStore() (*chaos.FaultStore, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	fs := s.GetFaultStore()

	return fs, fs != nil
}

// validateWorkspaceRoleArn rejects a WorkspaceRoleArn that doesn't resolve
// to a real IAM role in this account, mirroring real AWS Managed Grafana's
// rejection of a role it can't assume. A no-op when roleArn is empty (the
// field is optional) or when the IAM backend isn't wired -- e.g. unit tests
// that construct InMemoryBackend directly, with no sibling registry.
func (b *InMemoryBackend) validateWorkspaceRoleArn(roleArn string) error {
	if roleArn == "" {
		return nil
	}

	iamBk, ok := b.iamBackend()
	if !ok {
		return nil
	}

	if _, err := iamBk.GetRoleByArn(roleArn); err != nil {
		return validationError("workspaceRoleArn references a role that does not exist: " + roleArn)
	}

	return nil
}

// validateVpcConfiguration rejects subnet/security-group IDs that don't
// exist in the emulator's EC2 backend, mirroring real AWS Managed Grafana's
// rejection of an unresolvable VPC configuration.
func (b *InMemoryBackend) validateVpcConfiguration(vpc *vpcConfigurationWire) error {
	if vpc == nil {
		return nil
	}

	ec2Bk, ok := b.ec2Backend()
	if !ok {
		return nil
	}

	if subnets := vpc.SubnetIDs; len(subnets) > 0 && len(ec2Bk.DescribeSubnets(subnets)) != len(subnets) {
		return validationError("vpcConfiguration references a subnet that does not exist")
	}

	if sgs := vpc.SecurityGroupIDs; len(sgs) > 0 && len(ec2Bk.DescribeSecurityGroups(sgs)) != len(sgs) {
		return validationError("vpcConfiguration references a security group that does not exist")
	}

	return nil
}

// validateOrganizationalUnits rejects organizational unit IDs that don't
// exist in the emulator's Organizations backend.
func (b *InMemoryBackend) validateOrganizationalUnits(ous []string) error {
	if len(ous) == 0 {
		return nil
	}

	orgBk, ok := b.organizationsBackend()
	if !ok {
		return nil
	}

	for _, ou := range ous {
		if _, err := orgBk.DescribeOrganizationalUnit(ou); err != nil {
			return validationError(
				"workspaceOrganizationalUnits references an organizational unit that does not exist: " + ou,
			)
		}
	}

	return nil
}

// resolveIdentityStoreID returns the account's IAM Identity Center identity
// store ID via the SSO Admin backend's instance registry. Returns ok=false
// when no SSO instance exists in this account/region -- a legitimate,
// unvalidatable state (an account without IAM Identity Center enabled can't
// have AWS_SSO permission grants checked against it either, on real AWS) --
// or when ssoadmin isn't wired.
func (b *InMemoryBackend) resolveIdentityStoreID() (string, bool) {
	ssoBk, ok := b.ssoadminBackend()
	if !ok {
		return "", false
	}

	for _, inst := range ssoBk.ListInstances() {
		if inst.IdentityStoreID != "" {
			return inst.IdentityStoreID, true
		}
	}

	return "", false
}

// validatePermissionUser rejects an SSO_USER/SSO_GROUP permission grant
// whose ID doesn't resolve in the account's IAM Identity Center identity
// store. Non-SSO user types and grants made when no identity store can be
// resolved pass through unvalidated (see resolveIdentityStoreID).
func (b *InMemoryBackend) validatePermissionUser(ctx context.Context, u userWire) error {
	storeID, ok := b.resolveIdentityStoreID()
	if !ok {
		return nil
	}

	isBk, ok := b.identitystoreBackend()
	if !ok {
		return nil
	}

	var err error

	switch u.Type {
	case UserTypeSSOUser:
		_, err = isBk.DescribeUser(ctx, storeID, u.ID)
	case UserTypeSSOGroup:
		_, err = isBk.DescribeGroup(ctx, storeID, u.ID)
	default:
		return nil
	}

	if err != nil {
		return validationError("permission grant references an SSO " + u.Type + " that does not exist: " + u.ID)
	}

	return nil
}
