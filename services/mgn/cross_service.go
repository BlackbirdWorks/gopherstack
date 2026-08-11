package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
)

// siblingServices is the subset of *CLI's method set this backend needs: the
// EC2 backend, so StartTest/StartCutover can launch a real services/ec2
// instance on Job completion instead of minting a synthetic, non-cross-checked
// instance ID; and the Organizations backend, so ListManagedAccounts can
// return this account's real AWS Organizations member accounts when it's a
// registered delegated administrator, instead of always just itself. Matched
// structurally against *CLI (no import of the top-level package, which would
// cycle) -- same pattern as services/grafana's cross_service.go.
type siblingServices interface {
	GetEC2Handler() service.Registerable
	GetOrganizationsHandler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config value Provider.Init
// received, so this backend can resolve the EC2 handler on demand -- see
// services/grafana/cross_service.go's SetAppConfig doc comment for why this
// must be lazy rather than resolved at construction time.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.appConfig = cfg
}

func (b *InMemoryBackend) siblings() (siblingServices, bool) {
	s, ok := b.appConfig.(siblingServices)

	return s, ok
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

// mgnServicePrincipal is the AWS Organizations service principal AWS
// documents delegated MGN administration under. Not confirmed against any
// published AWS source (AWS's own docs return a JS-shell body to automated
// fetches -- PARITY.md's gaps section notes the same failure mode for this
// service's other undocumented values) -- this follows the
// "<endpoint-prefix>.amazonaws.com" convention botocore's service-2.json
// confirms for MGN's endpointPrefix ("mgn"), the same best-effort standard
// PARITY.md already applies to this service's ARN resource-path segments.
const mgnServicePrincipal = "mgn.amazonaws.com"

// resolveManagedAccountsLocked returns the AccountIDs ListManagedAccounts
// should report: every account in this account's AWS Organizations
// organization when this account is that organization's management account
// or a registered delegated administrator for mgnServicePrincipal (real AWS
// Organizations delegated-admin semantics), or just this account's own ID
// when Organizations isn't wired, no organization exists, or neither
// condition holds. Callers must hold b.mu (either lock).
func (b *InMemoryBackend) resolveManagedAccountsLocked() []string {
	orgBk, ok := b.organizationsBackend()
	if !ok {
		return []string{b.accountID}
	}

	org, err := orgBk.DescribeOrganization()
	if err != nil {
		return []string{b.accountID}
	}

	isManagementAccount := org.MasterAccountID == b.accountID
	isDelegatedAdmin := false

	if admins, adminErr := orgBk.ListDelegatedAdministrators(mgnServicePrincipal); adminErr == nil {
		for _, a := range admins {
			if a.AccountID == b.accountID {
				isDelegatedAdmin = true

				break
			}
		}
	}

	if !isManagementAccount && !isDelegatedAdmin {
		return []string{b.accountID}
	}

	accounts, err := orgBk.ListAccounts()
	if err != nil || len(accounts) == 0 {
		return []string{b.accountID}
	}

	ids := make([]string, len(accounts))
	for i, a := range accounts {
		ids[i] = a.ID
	}

	return ids
}

// defaultLaunchInstanceType is used when a participant's LaunchConfiguration
// has no Ec2LaunchTemplateID resolving to a real EC2 launch template --
// real MGN right-sizes the target instance type from the source server's
// CPU/RAM inventory via TargetInstanceTypeRightSizingMethod, an algorithm
// this emulator does not model; a fixed default is a documented
// simplification, not a fabricated real value.
const defaultLaunchInstanceType = "t3.medium"

// launchParticipantInstanceLocked launches a real services/ec2 instance for
// a StartTest/StartCutover participant and returns its real instance ID.
// Falls back to a synthetic, non-cross-checked ID (newSyntheticInstanceID)
// when the EC2 backend isn't wired (e.g. unit tests constructing
// InMemoryBackend directly) or when RunInstances itself fails (e.g. no AMI
// catalogue) -- never blocks Job completion on the cross-service call.
// Callers must hold b.mu.
func (b *InMemoryBackend) launchParticipantInstanceLocked(sourceServerID string) string {
	ec2Bk, ok := b.ec2Backend()
	if !ok {
		return newSyntheticInstanceID()
	}

	imageID, instanceType := b.resolveLaunchSpecLocked(sourceServerID, ec2Bk)
	if imageID == "" {
		return newSyntheticInstanceID()
	}

	subnetID := ""
	if rc, found := b.replicationConfigs.Get(sourceServerID); found {
		subnetID = rc.StagingAreaSubnetID
	}

	instances, err := ec2Bk.RunInstances(imageID, instanceType, subnetID, 1)
	if err != nil || len(instances) == 0 {
		return newSyntheticInstanceID()
	}

	return instances[0].ID
}

// resolveLaunchSpecLocked resolves the AMI/instance type to launch
// sourceServerID's target instance with: the source server's
// LaunchConfiguration.Ec2LaunchTemplateID, if it resolves to a real EC2
// launch template, wins (matching real MGN's launch-template-driven
// launch); otherwise falls back to the EC2 backend's own stub AMI
// catalogue plus defaultLaunchInstanceType. Callers must hold b.mu.
func (b *InMemoryBackend) resolveLaunchSpecLocked(sourceServerID string, ec2Bk ec2backend.Backend) (string, string) {
	if imageID, instanceType, ok := b.resolveLaunchTemplateSpecLocked(sourceServerID, ec2Bk); ok {
		return imageID, instanceType
	}

	if images := ec2Bk.DescribeImages(); len(images) > 0 {
		return images[0].ImageID, defaultLaunchInstanceType
	}

	return "", defaultLaunchInstanceType
}

// resolveLaunchTemplateSpecLocked returns the AMI/instance type from
// sourceServerID's LaunchConfiguration.Ec2LaunchTemplateID, if it names a
// real EC2 launch template with an ImageID set. Callers must hold b.mu.
func (b *InMemoryBackend) resolveLaunchTemplateSpecLocked(
	sourceServerID string,
	ec2Bk ec2backend.Backend,
) (string, string, bool) {
	lc, found := b.launchConfigs.Get(sourceServerID)
	if !found || lc.Ec2LaunchTemplateID == "" {
		return "", "", false
	}

	versions, err := ec2Bk.DescribeLaunchTemplateVersions(lc.Ec2LaunchTemplateID)
	if err != nil || len(versions) == 0 || versions[0].ImageID == "" {
		return "", "", false
	}

	instanceType := defaultLaunchInstanceType
	if versions[0].InstanceType != "" {
		instanceType = versions[0].InstanceType
	}

	return versions[0].ImageID, instanceType, true
}
