package redshift

import (
	"fmt"
	"net"
	"net/url"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func maintenanceTrackNames() []string { return []string{defaultMaintenanceTrack, "trailing"} }

const (
	defaultMaintenanceTrack = "current"
	defaultIPAddressType    = "ipv4"
	ipAddressTypeDualstack  = "dualstack"
	hsmStatusActive         = "active"
	elasticIPStatusActive   = "active"
	maxDeferMaintenanceDays = 60
	hoursPerDay             = 24
)

// ClusterSettings holds the optional Create/Modify/RestoreFromClusterSnapshot members shared by cluster ops.
type ClusterSettings struct {
	AvailabilityZoneRelocation     *bool
	ManageMasterPassword           *bool
	MaintenanceTrackName           string
	IPAddressType                  string
	ElasticIP                      string
	MasterPasswordSecretKmsKeyID   string
	HsmClientCertificateIdentifier string
	HsmConfigurationIdentifier     string
	MasterUserPassword             string
}

func parseClusterSettings(vals url.Values) ClusterSettings {
	tri := func(key string) *bool {
		v := vals.Get(key)
		if v == "" {
			return nil
		}

		b := v == paramValueTrue

		return &b
	}

	return ClusterSettings{
		AvailabilityZoneRelocation:     tri("AvailabilityZoneRelocation"),
		ManageMasterPassword:           tri("ManageMasterPassword"),
		MaintenanceTrackName:           vals.Get("MaintenanceTrackName"),
		IPAddressType:                  vals.Get("IpAddressType"),
		ElasticIP:                      vals.Get("ElasticIp"),
		MasterPasswordSecretKmsKeyID:   vals.Get("MasterPasswordSecretKmsKeyId"),
		HsmClientCertificateIdentifier: vals.Get("HsmClientCertificateIdentifier"),
		HsmConfigurationIdentifier:     vals.Get("HsmConfigurationIdentifier"),
		MasterUserPassword:             vals.Get("MasterUserPassword"),
	}
}

func (s ClusterSettings) manage() bool {
	return s.ManageMasterPassword != nil && *s.ManageMasterPassword
}

// validateClusterSettingsLocked checks the members that can be rejected before any state changes.
func (b *InMemoryBackend) validateClusterSettingsLocked(s ClusterSettings) error {
	if err := validateClusterSettingsValues(s); err != nil {
		return err
	}

	if s.HsmClientCertificateIdentifier != "" && !b.hsmClientCerts.Has(s.HsmClientCertificateIdentifier) {
		return fmt.Errorf(
			"%w: HSM client certificate %s not found", ErrHsmClientCertNotFound, s.HsmClientCertificateIdentifier,
		)
	}

	if s.HsmConfigurationIdentifier != "" && !b.hsmConfigs.Has(s.HsmConfigurationIdentifier) {
		return fmt.Errorf("%w: HSM configuration %s not found", ErrHsmConfigNotFound, s.HsmConfigurationIdentifier)
	}

	return nil
}

func validateClusterSettingsValues(s ClusterSettings) error {
	if s.MaintenanceTrackName != "" && !slices.Contains(maintenanceTrackNames(), s.MaintenanceTrackName) {
		return fmt.Errorf("%w: maintenance track %q does not exist", ErrInvalidClusterTrack, s.MaintenanceTrackName)
	}

	if s.IPAddressType != "" && s.IPAddressType != defaultIPAddressType && s.IPAddressType != ipAddressTypeDualstack {
		return fmt.Errorf("%w: IpAddressType must be ipv4 or dualstack", ErrInvalidParameter)
	}

	if ip := net.ParseIP(s.ElasticIP); s.ElasticIP != "" && (ip == nil || ip.To4() == nil) {
		return fmt.Errorf("%w: ElasticIp %q is not a valid IPv4 address", ErrInvalidElasticIP, s.ElasticIP)
	}

	if s.MasterPasswordSecretKmsKeyID != "" && s.ManageMasterPassword != nil && !*s.ManageMasterPassword {
		return fmt.Errorf(
			"%w: MasterPasswordSecretKmsKeyId requires ManageMasterPassword",
			ErrInvalidParameterCombination,
		)
	}

	return nil
}

// applyClusterSettingsLocked applies s to a cluster being created or restored.
func (b *InMemoryBackend) applyClusterSettingsLocked(c *Cluster, s ClusterSettings) error {
	if err := b.validateClusterSettingsLocked(s); err != nil {
		return err
	}

	if s.manage() && s.MasterUserPassword != "" {
		return fmt.Errorf(
			"%w: MasterUserPassword can't be specified with ManageMasterPassword",
			ErrInvalidParameterCombination,
		)
	}

	if s.MasterPasswordSecretKmsKeyID != "" && !s.manage() {
		return fmt.Errorf(
			"%w: MasterPasswordSecretKmsKeyId requires ManageMasterPassword",
			ErrInvalidParameterCombination,
		)
	}

	relocation := s.AvailabilityZoneRelocation != nil && *s.AvailabilityZoneRelocation
	if relocation && s.ElasticIP != "" && c.PubliclyAccessible {
		return fmt.Errorf(
			"%w: ElasticIp can't be specified for a publicly accessible cluster with Availability Zone relocation",
			ErrInvalidParameterCombination,
		)
	}

	c.MaintenanceTrackName = firstNonEmpty(s.MaintenanceTrackName, defaultMaintenanceTrack)
	c.IPAddressType = firstNonEmpty(s.IPAddressType, defaultIPAddressType)
	c.ElasticIP = s.ElasticIP
	c.AvailabilityZoneRelocation = relocation
	c.HsmClientCertificateIdentifier = s.HsmClientCertificateIdentifier
	c.HsmConfigurationIdentifier = s.HsmConfigurationIdentifier

	if s.manage() {
		b.setMasterSecretLocked(c, s.MasterPasswordSecretKmsKeyID)
	}

	return nil
}

func (b *InMemoryBackend) validateModifyClusterSettingsLocked(c *Cluster, s ClusterSettings) error {
	if err := b.validateClusterSettingsLocked(s); err != nil {
		return err
	}

	return b.checkModifyMasterSecretLocked(c, s)
}

// applyModifyClusterSettingsLocked applies validated s to an existing cluster; a track change stays pending.
func (b *InMemoryBackend) applyModifyClusterSettingsLocked(c *Cluster, s ClusterSettings) {
	if s.IPAddressType != "" {
		c.IPAddressType = s.IPAddressType
	}

	if s.ElasticIP != "" {
		c.ElasticIP = s.ElasticIP
	}

	if s.AvailabilityZoneRelocation != nil {
		c.AvailabilityZoneRelocation = *s.AvailabilityZoneRelocation
	}

	if s.HsmClientCertificateIdentifier != "" {
		c.HsmClientCertificateIdentifier = s.HsmClientCertificateIdentifier
	}

	if s.HsmConfigurationIdentifier != "" {
		c.HsmConfigurationIdentifier = s.HsmConfigurationIdentifier
	}

	switch {
	case s.manage() && c.MasterPasswordSecretArn == "":
		b.setMasterSecretLocked(c, s.MasterPasswordSecretKmsKeyID)
	case s.ManageMasterPassword != nil && !*s.ManageMasterPassword:
		c.MasterPasswordSecretArn = ""
		c.MasterPasswordSecretKmsKeyID = ""
	case s.MasterPasswordSecretKmsKeyID != "":
		c.MasterPasswordSecretKmsKeyID = s.MasterPasswordSecretKmsKeyID
	}

	if s.MaintenanceTrackName != "" {
		if c.PendingModifiedValues == nil {
			c.PendingModifiedValues = &ClusterPendingModifiedValues{}
		}

		c.PendingModifiedValues.MaintenanceTrackName = s.MaintenanceTrackName
	}
}

func (b *InMemoryBackend) checkModifyMasterSecretLocked(c *Cluster, s ClusterSettings) error {
	managed := c.MasterPasswordSecretArn != ""

	switch {
	case s.manage() && !managed && s.MasterUserPassword != "":
		return fmt.Errorf(
			"%w: MasterUserPassword can't be specified with ManageMasterPassword",
			ErrInvalidParameterCombination,
		)
	case s.ManageMasterPassword != nil && !*s.ManageMasterPassword && managed && s.MasterUserPassword == "":
		return fmt.Errorf(
			"%w: MasterUserPassword is required to stop managing the master user password",
			ErrInvalidParameterCombination,
		)
	case s.MasterPasswordSecretKmsKeyID != "" && !managed && !s.manage():
		return fmt.Errorf(
			"%w: MasterPasswordSecretKmsKeyId requires ManageMasterPassword",
			ErrInvalidParameterCombination,
		)
	}

	return nil
}

func (b *InMemoryBackend) setMasterSecretLocked(c *Cluster, kmsKeyID string) {
	c.MasterPasswordSecretArn = arn.Build(
		"secretsmanager", b.region, b.accountID,
		"secret:redshift!"+c.ClusterIdentifier+"-"+c.MasterUsername+"-"+randomHex(slSecretHexBytes),
	)
	c.MasterPasswordSecretKmsKeyID = kmsKeyID
}

func firstNonEmpty(v, dflt string) string {
	if v == "" {
		return dflt
	}

	return v
}

// ModifyClusterMaintenanceOptions holds ModifyClusterMaintenance's optional members.
type ModifyClusterMaintenanceOptions struct {
	DeferMaintenance           *bool
	StartTime                  *time.Time
	EndTime                    *time.Time
	DeferMaintenanceIdentifier string
	DeferMaintenanceDuration   int
}

// ModifyClusterMaintenance adds or removes a deferred maintenance window on a cluster.
func (b *InMemoryBackend) ModifyClusterMaintenance(id string, opts ModifyClusterMaintenanceOptions) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClusterMaintenance")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	if cluster.Status != clusterStatusAvailable {
		return nil, fmt.Errorf(
			"%w: cluster %s is not in the available state (status %q)", ErrClusterInvalidState, id, cluster.Status,
		)
	}

	if opts.DeferMaintenance != nil && !*opts.DeferMaintenance {
		cluster.DeferredMaintenanceWindows = slices.DeleteFunc(cluster.DeferredMaintenanceWindows,
			func(w DeferredMaintenanceWindow) bool {
				return opts.DeferMaintenanceIdentifier == "" || w.Identifier == opts.DeferMaintenanceIdentifier
			})
	} else if opts.DeferMaintenance != nil {
		window, err := newDeferredWindow(opts, time.Now().UTC())
		if err != nil {
			return nil, err
		}

		cluster.DeferredMaintenanceWindows = append(cluster.DeferredMaintenanceWindows, window)
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}

func newDeferredWindow(opts ModifyClusterMaintenanceOptions, now time.Time) (DeferredMaintenanceWindow, error) {
	if opts.DeferMaintenanceDuration > 0 && opts.EndTime != nil {
		return DeferredMaintenanceWindow{}, fmt.Errorf(
			"%w: DeferMaintenanceDuration and DeferMaintenanceEndTime can't both be specified", ErrInvalidParameter,
		)
	}

	if opts.DeferMaintenanceDuration > maxDeferMaintenanceDays {
		return DeferredMaintenanceWindow{}, fmt.Errorf(
			"%w: DeferMaintenanceDuration must be %d days or less", ErrInvalidParameter, maxDeferMaintenanceDays,
		)
	}

	start := now
	if opts.StartTime != nil {
		start = opts.StartTime.UTC()
	}

	var end time.Time

	switch {
	case opts.EndTime != nil:
		end = opts.EndTime.UTC()
	case opts.DeferMaintenanceDuration > 0:
		end = start.Add(time.Duration(opts.DeferMaintenanceDuration) * hoursPerDay * time.Hour)
	default:
		return DeferredMaintenanceWindow{}, fmt.Errorf(
			"%w: DeferMaintenanceDuration or DeferMaintenanceEndTime is required", ErrInvalidParameter,
		)
	}

	if !end.After(start) {
		return DeferredMaintenanceWindow{}, fmt.Errorf(
			"%w: the deferred maintenance window must end after it starts", ErrInvalidParameter,
		)
	}

	id := opts.DeferMaintenanceIdentifier
	if id == "" {
		id = "dm-" + randomHex(slSecretHexBytes)
	}

	return DeferredMaintenanceWindow{StartTime: start, EndTime: end, Identifier: id}, nil
}

type xmlElasticIPStatus struct {
	ElasticIP string `xml:"ElasticIp"`
	Status    string `xml:"Status"`
}

type xmlHsmStatus struct {
	HsmClientCertificateIdentifier string `xml:"HsmClientCertificateIdentifier,omitempty"`
	HsmConfigurationIdentifier     string `xml:"HsmConfigurationIdentifier,omitempty"`
	Status                         string `xml:"Status"`
}

type xmlPendingModifiedValues struct {
	PubliclyAccessible               *bool  `xml:"PubliclyAccessible,omitempty"`
	AutomatedSnapshotRetentionPeriod *int   `xml:"AutomatedSnapshotRetentionPeriod,omitempty"`
	NumberOfNodes                    *int   `xml:"NumberOfNodes,omitempty"`
	ClusterVersion                   string `xml:"ClusterVersion,omitempty"`
	MaintenanceTrackName             string `xml:"MaintenanceTrackName,omitempty"`
	NodeType                         string `xml:"NodeType,omitempty"`
}

type xmlDeferredMaintenanceWindow struct {
	DeferMaintenanceIdentifier string `xml:"DeferMaintenanceIdentifier"`
	DeferMaintenanceStartTime  string `xml:"DeferMaintenanceStartTime"`
	DeferMaintenanceEndTime    string `xml:"DeferMaintenanceEndTime"`
}

func relocationStatus(enabled bool) string {
	if enabled {
		return "enabled"
	}

	return statusDisabled
}

func elasticIPStatusXML(ip string) *xmlElasticIPStatus {
	if ip == "" {
		return nil
	}

	return &xmlElasticIPStatus{ElasticIP: ip, Status: elasticIPStatusActive}
}

func hsmStatusXML(c *Cluster) *xmlHsmStatus {
	if c.HsmClientCertificateIdentifier == "" && c.HsmConfigurationIdentifier == "" {
		return nil
	}

	return &xmlHsmStatus{
		HsmClientCertificateIdentifier: c.HsmClientCertificateIdentifier,
		HsmConfigurationIdentifier:     c.HsmConfigurationIdentifier,
		Status:                         hsmStatusActive,
	}
}

func pendingModifiedValuesXML(p *ClusterPendingModifiedValues) *xmlPendingModifiedValues {
	if p == nil {
		return nil
	}

	x := &xmlPendingModifiedValues{
		ClusterVersion:       p.ClusterVersion,
		MaintenanceTrackName: p.MaintenanceTrackName,
		NodeType:             p.NodeType,
	}

	if p.NumberOfNodes > 0 {
		x.NumberOfNodes = &p.NumberOfNodes
	}

	if p.AutomatedSnapshotRetentionPeriod > 0 {
		x.AutomatedSnapshotRetentionPeriod = &p.AutomatedSnapshotRetentionPeriod
	}

	if p.PubliclyAccessible {
		x.PubliclyAccessible = &p.PubliclyAccessible
	}

	if *x == (xmlPendingModifiedValues{}) {
		return nil
	}

	return x
}

type xmlDeferredWindows struct {
	Members []xmlDeferredMaintenanceWindow `xml:"DeferredMaintenanceWindow"`
}

func deferredWindowsXML(windows []DeferredMaintenanceWindow) *xmlDeferredWindows {
	if len(windows) == 0 {
		return nil
	}

	out := make([]xmlDeferredMaintenanceWindow, 0, len(windows))

	for _, w := range windows {
		out = append(out, xmlDeferredMaintenanceWindow{
			DeferMaintenanceIdentifier: w.Identifier,
			DeferMaintenanceStartTime:  w.StartTime.UTC().Format(time.RFC3339),
			DeferMaintenanceEndTime:    w.EndTime.UTC().Format(time.RFC3339),
		})
	}

	return &xmlDeferredWindows{Members: out}
}
