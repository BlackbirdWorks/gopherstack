package eks

import "fmt"

// IdcApplicationManager creates and removes the IAM Identity Center application EKS manages for an Argo CD
// capability; the application lives in the Identity Center instance's region (ArgoCdAwsIdcConfig.IdcRegion).
type IdcApplicationManager interface {
	CreateManagedApplication(idcRegion, instanceArn, name string) (string, error)
	DeleteManagedApplication(idcRegion, applicationArn string) error
}

// SetIdcApplicationManager wires Argo CD capabilities to IAM Identity Center; nil leaves the ARN unset.
func (b *InMemoryBackend) SetIdcApplicationManager(m IdcApplicationManager) {
	b.mu.Lock("SetIdcApplicationManager")
	defer b.mu.Unlock()

	b.idcApps = m
}

func idcApplicationName(clusterName, capabilityName string) string {
	return "eks-argocd-" + clusterName + "-" + capabilityName
}

func (b *InMemoryBackend) idcRegionOrOwn(cfg *ArgoCdAwsIdcConfig) string {
	if cfg.IdcRegion != "" {
		return cfg.IdcRegion
	}

	return b.region
}

// createIdcApplicationLocked fills IdcManagedApplicationArn for an Argo CD capability configured with Identity Center.
func (b *InMemoryBackend) createIdcApplicationLocked(clusterName, capabilityName string, cfg *ArgoCdConfig) error {
	if b.idcApps == nil || cfg == nil || cfg.AwsIdc == nil {
		return nil
	}

	appArn, err := b.idcApps.CreateManagedApplication(
		b.idcRegionOrOwn(cfg.AwsIdc), cfg.AwsIdc.IdcInstanceArn, idcApplicationName(clusterName, capabilityName),
	)
	if err != nil {
		return fmt.Errorf("%w: IAM Identity Center application: %w", ErrValidation, err)
	}

	cfg.AwsIdc.IdcManagedApplicationArn = appArn

	return nil
}

func (b *InMemoryBackend) deleteIdcApplicationLocked(capa *Capability) {
	if b.idcApps == nil || capa.Configuration == nil || capa.Configuration.ArgoCd == nil {
		return
	}

	idc := capa.Configuration.ArgoCd.AwsIdc
	if idc == nil || idc.IdcManagedApplicationArn == "" {
		return
	}

	_ = b.idcApps.DeleteManagedApplication(b.idcRegionOrOwn(idc), idc.IdcManagedApplicationArn)
}
