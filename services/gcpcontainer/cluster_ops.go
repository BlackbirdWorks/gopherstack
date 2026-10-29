//nolint:staticcheck // legacy request and cluster fields are deprecated in the proto but still served
package gcpcontainer

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"slices"

	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
)

const (
	generatedPasswordBytes = 12
	defaultAdminUser       = "admin"
)

// clusterTarget names the cluster an action applies to.
type clusterTarget struct {
	name    string
	project string
	zone    string
	cluster string
}

// mutateCluster runs apply against a copy of the cluster and commits it with a new operation of type typ.
// apply may also edit the record's own fields, but only after every check has passed.
func (b *InMemoryBackend) mutateCluster(
	t clusterTarget, typ containerpb.Operation_Type, apply func(c *clusterRec, cl *containerpb.Cluster) error,
) (*containerpb.Operation, error) {
	r, err := resolveCluster(t.name, t.project, t.zone, t.cluster)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("mutateCluster")
	defer b.mu.Unlock()

	b.sweepLocked()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	if err = b.conflictLocked(r, ""); err != nil {
		return nil, err
	}

	cl := clone(c.Cluster)
	if err = apply(c, cl); err != nil {
		return nil, err
	}

	c.Cluster = cl
	o := b.newOpLocked(r, typ, kindClusterOp, cl.GetSelfLink(), b.timing.Mutate, false)

	return b.opViewLocked(o), nil
}

// SetLabels replaces the cluster's resource labels.
func (b *InMemoryBackend) SetLabels(req *containerpb.SetLabelsRequest) (*containerpb.Operation, error) {
	t := clusterTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	}

	return b.mutateCluster(t, containerpb.Operation_SET_LABELS, func(_ *clusterRec, cl *containerpb.Cluster) error {
		if req.GetLabelFingerprint() == "" {
			return errInvalid("label_fingerprint", "Label fingerprint must be specified.")
		}

		if err := validLabels("resource_labels", req.GetResourceLabels()); err != nil {
			return err
		}

		if req.GetLabelFingerprint() != cl.GetLabelFingerprint() {
			return errPrecondition(
				cl.GetSelfLink(),
				"Label fingerprint %q is stale; the cluster's labels have changed.",
				req.GetLabelFingerprint(),
			)
		}

		cl.ResourceLabels = req.GetResourceLabels()
		cl.LabelFingerprint = fingerprint(cl.GetResourceLabels())

		return nil
	})
}

func randomPassword() string {
	buf := make([]byte, generatedPasswordBytes)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

// SetMasterAuth changes the cluster's basic-auth username or password.
func (b *InMemoryBackend) SetMasterAuth(req *containerpb.SetMasterAuthRequest) (*containerpb.Operation, error) {
	t := clusterTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	}

	return b.mutateCluster(
		t,
		containerpb.Operation_SET_MASTER_AUTH,
		func(_ *clusterRec, cl *containerpb.Cluster) error {
			cl.MasterAuth = firstPtr(cl.GetMasterAuth(), &containerpb.MasterAuth{})
			ma := cl.GetMasterAuth()

			switch req.GetAction() {
			case containerpb.SetMasterAuthRequest_SET_PASSWORD:
				if err := validPassword(
					req.GetUpdate().GetPassword(),
				); err != nil {
					return err
				}

				ma.Password = req.GetUpdate().
					GetPassword()
					//nolint:staticcheck // deprecated field kept for wire compatibility
				ma.Username = firstNonEmpty(ma.GetUsername(), req.GetUpdate().GetUsername(), defaultAdminUser)
			case containerpb.SetMasterAuthRequest_GENERATE_PASSWORD:
				ma.Password = randomPassword()
				ma.Username = firstNonEmpty(ma.GetUsername(), defaultAdminUser)
			case containerpb.SetMasterAuthRequest_SET_USERNAME:
				if req.GetUpdate().
					GetUsername() ==
					"" {
					return errInvalid("update.username", "Username must be specified.")
				}

				ma.Username = req.GetUpdate().
					GetUsername()
				//nolint:staticcheck // deprecated field kept for wire compatibility
			case containerpb.SetMasterAuthRequest_UNKNOWN:
				return errInvalid("action", "Action must be one of SET_PASSWORD, GENERATE_PASSWORD or SET_USERNAME.")
			}

			return nil
		},
	)
}

// SetLegacyAbac enables or disables legacy ABAC authorization.
func (b *InMemoryBackend) SetLegacyAbac(req *containerpb.SetLegacyAbacRequest) (*containerpb.Operation, error) {
	t := clusterTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	}

	return b.mutateCluster(t, containerpb.Operation_UPDATE_CLUSTER, func(_ *clusterRec, cl *containerpb.Cluster) error {
		if req.GetEnabled() && cl.GetAutopilot().GetEnabled() {
			return errInvalid("enabled", "Legacy ABAC cannot be enabled on Autopilot clusters.")
		}

		cl.LegacyAbac = &containerpb.LegacyAbac{Enabled: req.GetEnabled()}

		return nil
	})
}

// SetNetworkPolicy enables or disables Calico network policy enforcement.
func (b *InMemoryBackend) SetNetworkPolicy(req *containerpb.SetNetworkPolicyRequest) (*containerpb.Operation, error) {
	t := clusterTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	}

	return b.mutateCluster(
		t,
		containerpb.Operation_SET_NETWORK_POLICY,
		func(_ *clusterRec, cl *containerpb.Cluster) error {
			np := req.GetNetworkPolicy()
			if np == nil {
				return errInvalid("network_policy", "Network policy must be specified.")
			}

			if np.GetEnabled() {
				switch {
				case cl.GetAutopilot().GetEnabled():
					return errInvalid("network_policy", "Network policy cannot be enabled on Autopilot clusters.")
				case np.GetProvider() != containerpb.NetworkPolicy_CALICO:
					return errInvalid("network_policy.provider", "Network policy provider must be CALICO.")
				case cl.GetAddonsConfig().GetNetworkPolicyConfig().GetDisabled():
					return errPrecondition(
						cl.GetSelfLink(),
						"The network policy addon must be enabled before network policy is enabled.",
					)
				}
			}

			cl.NetworkPolicy = clone(np)

			return nil
		},
	)
}

// SetMaintenancePolicy replaces the cluster's maintenance policy.
func (b *InMemoryBackend) SetMaintenancePolicy(
	req *containerpb.SetMaintenancePolicyRequest,
) (*containerpb.Operation, error) {
	t := clusterTarget{req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId()}

	return b.mutateCluster(
		t,
		containerpb.Operation_SET_MAINTENANCE_POLICY,
		func(_ *clusterRec, cl *containerpb.Cluster) error {
			mp := req.GetMaintenancePolicy()
			if mp == nil {
				return errInvalid("maintenance_policy", "Maintenance policy must be specified.")
			}

			if mp.GetResourceVersion() == "" {
				return errInvalid(
					"maintenance_policy.resource_version",
					"Resource version must be specified; read it from the cluster.",
				)
			}

			if mp.GetResourceVersion() != cl.GetMaintenancePolicy().GetResourceVersion() {
				return errPrecondition(
					cl.GetSelfLink(),
					"The maintenance policy resource version %q is stale.",
					mp.GetResourceVersion(),
				)
			}

			if err := validMaintenance(mp); err != nil {
				return err
			}

			cl.MaintenancePolicy = withPolicyVersion(clone(mp))

			return nil
		},
	)
}

// StartIPRotation begins an IP (and optionally credential) rotation.
func (b *InMemoryBackend) StartIPRotation(req *containerpb.StartIPRotationRequest) (*containerpb.Operation, error) {
	t := clusterTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	}

	return b.mutateCluster(t, containerpb.Operation_UPDATE_CLUSTER, func(c *clusterRec, cl *containerpb.Cluster) error {
		switch {
		case b.engine != nil:
			return errPrecondition(
				cl.GetSelfLink(),
				"IP rotation is not supported by clusters backed by the docker engine.",
			)
		case c.Rotation != nil:
			return errPrecondition(cl.GetSelfLink(), "IP rotation is already in progress.")
		}

		c.Rotation = &rotationRec{OldEndpoint: cl.GetEndpoint(), Credentials: req.GetRotateCredentials()}
		b.seq++
		cl.Endpoint = documentationAddr(b.seq)

		if req.GetRotateCredentials() {
			ca, err := k3sengine.NewCA("gopherstack-gke-" + cl.GetName() + "-ca")
			if err != nil {
				c.Rotation = nil

				return err
			}

			cl.MasterAuth.ClusterCaCertificate = base64.StdEncoding.EncodeToString(ca.CertPEM)
		}

		return nil
	})
}

// CompleteIPRotation finishes a rotation started by StartIPRotation.
func (b *InMemoryBackend) CompleteIPRotation(
	req *containerpb.CompleteIPRotationRequest,
) (*containerpb.Operation, error) {
	t := clusterTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	}

	return b.mutateCluster(t, containerpb.Operation_UPDATE_CLUSTER, func(c *clusterRec, cl *containerpb.Cluster) error {
		if c.Rotation == nil {
			return errPrecondition(cl.GetSelfLink(), "IP rotation is not in progress.")
		}

		c.Rotation = nil

		return nil
	})
}

func validService(v string, allowed ...string) error {
	if slices.Contains(allowed, v) {
		return nil
	}

	return errInvalid("service", "Invalid service %q; want one of %v.", v, allowed)
}

// SetLoggingService selects the logging backend.
func (b *InMemoryBackend) SetLoggingService(req *containerpb.SetLoggingServiceRequest) (*containerpb.Operation, error) {
	t := clusterTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	}

	return b.mutateCluster(t, containerpb.Operation_UPDATE_CLUSTER, func(_ *clusterRec, cl *containerpb.Cluster) error {
		if err := validService(
			req.GetLoggingService(),
			"logging.googleapis.com/kubernetes",
			"logging.googleapis.com",
			"none",
		); err != nil {
			return err
		}

		cl.LoggingService = req.GetLoggingService()

		return nil
	})
}

// SetMonitoringService selects the monitoring backend.
func (b *InMemoryBackend) SetMonitoringService(
	req *containerpb.SetMonitoringServiceRequest,
) (*containerpb.Operation, error) {
	t := clusterTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	}

	return b.mutateCluster(t, containerpb.Operation_UPDATE_CLUSTER, func(_ *clusterRec, cl *containerpb.Cluster) error {
		if err := validService(
			req.GetMonitoringService(),
			"monitoring.googleapis.com/kubernetes",
			"monitoring.googleapis.com",
			"none",
		); err != nil {
			return err
		}

		cl.MonitoringService = req.GetMonitoringService()

		return nil
	})
}

func mergeAddons(cl *containerpb.Cluster, in *containerpb.AddonsConfig) error {
	if in == nil {
		return errInvalid("addons_config", "Addons config must be specified.")
	}

	merged := clone(cl.GetAddonsConfig())

	in.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		merged.ProtoReflect().Set(fd, v)

		return true
	})

	if merged.GetNetworkPolicyConfig().GetDisabled() && cl.GetNetworkPolicy().GetEnabled() {
		return errPrecondition(
			cl.GetSelfLink(),
			"The network policy addon cannot be disabled while network policy is enabled.",
		)
	}

	cl.AddonsConfig = merged

	return nil
}

// SetAddonsConfig replaces each addon section named in the request into the cluster.
func (b *InMemoryBackend) SetAddonsConfig(req *containerpb.SetAddonsConfigRequest) (*containerpb.Operation, error) {
	t := clusterTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	}

	return b.mutateCluster(t, containerpb.Operation_UPDATE_CLUSTER, func(_ *clusterRec, cl *containerpb.Cluster) error {
		return mergeAddons(cl, req.GetAddonsConfig())
	})
}

// SetLocations changes the zones the cluster's nodes run in.
func (b *InMemoryBackend) SetLocations(req *containerpb.SetLocationsRequest) (*containerpb.Operation, error) {
	t := clusterTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	}

	return b.mutateCluster(t, containerpb.Operation_UPDATE_CLUSTER, func(c *clusterRec, cl *containerpb.Cluster) error {
		return b.setLocations(c, cl, req.GetLocations())
	})
}

func (b *InMemoryBackend) setLocations(c *clusterRec, cl *containerpb.Cluster, want []string) error {
	if len(want) == 0 {
		return errInvalid("locations", "At least one location must be specified.")
	}

	if err := checkLocations(cl.GetLocation(), want); err != nil {
		return err
	}

	zones := clusterZones(cl.GetLocation(), want)
	old := cl.GetLocations()

	for _, p := range c.Pools {
		if equalStrings(p.Pool.GetLocations(), old) {
			p.Pool.Locations = zones
			p.Pool.InstanceGroupUrls = groupURLs(c.Project, zones, cl.GetName(), p.Pool.GetName())
		}
	}

	cl.Locations = zones

	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// UpdateMaster upgrades the cluster's control plane.
func (b *InMemoryBackend) UpdateMaster(req *containerpb.UpdateMasterRequest) (*containerpb.Operation, error) {
	t := clusterTarget{
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	}

	return b.mutateCluster(t, containerpb.Operation_UPGRADE_MASTER, func(_ *clusterRec, cl *containerpb.Cluster) error {
		return upgradeMaster(cl, req.GetMasterVersion())
	})
}

func upgradeMaster(cl *containerpb.Cluster, want string) error {
	if want == "" {
		return errInvalid("master_version", "Master version must be specified.")
	}

	v, err := resolveMasterVersion(want, cl.GetReleaseChannel().GetChannel())
	if err != nil {
		return err
	}

	if compareVersions(v, cl.GetCurrentMasterVersion()) < 0 {
		return errInvalid(
			"master_version",
			"Master version %s is older than the current version %s; downgrades are not supported.",
			v,
			cl.GetCurrentMasterVersion(),
		)
	}

	cl.CurrentMasterVersion = v

	return nil
}
