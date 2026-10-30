package azurearm

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
)

const (
	aciMaxContainers     = 60
	aciMaxInitContainers = 59
	aciProbeDefPeriod    = 10
	aciProbeDefFailure   = 3
	aciProbeDefSuccess   = 1
	aciProbeDefTimeout   = 1
	aciLogTypeInsights   = "ContainerInsights"
	aciLogTypeLogs       = "ContainerInstanceLogs"
	aciDelegationName    = "Microsoft.ContainerInstance/containerGroups"
	aciMaxDNSServers     = 3
)

type aciExecWire struct {
	Command []string `json:"command,omitempty"`
}

type aciHeaderWire struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type aciHTTPGetWire struct {
	Path        string          `json:"path,omitempty"`
	Scheme      string          `json:"scheme,omitempty"`
	HTTPHeaders []aciHeaderWire `json:"httpHeaders,omitempty"`
	Port        int             `json:"port"`
}

type aciProbeWire struct {
	Exec                *aciExecWire    `json:"exec,omitempty"`
	HTTPGet             *aciHTTPGetWire `json:"httpGet,omitempty"`
	InitialDelaySeconds int             `json:"initialDelaySeconds,omitempty"`
	PeriodSeconds       int             `json:"periodSeconds,omitempty"`
	FailureThreshold    int             `json:"failureThreshold,omitempty"`
	SuccessThreshold    int             `json:"successThreshold,omitempty"`
	TimeoutSeconds      int             `json:"timeoutSeconds,omitempty"`
}

type aciInitPropsWire struct {
	Image                string          `json:"image"`
	Command              []string        `json:"command,omitempty"`
	EnvironmentVariables []aciEnvWire    `json:"environmentVariables,omitempty"`
	VolumeMounts         []aciMountWire  `json:"volumeMounts,omitempty"`
	InstanceView         json.RawMessage `json:"instanceView,omitempty"`
}

type aciInitWire struct {
	Name       string           `json:"name"`
	Properties aciInitPropsWire `json:"properties"`
}

type aciGitRepoWire struct {
	Repository string `json:"repository"`
	Directory  string `json:"directory,omitempty"`
	Revision   string `json:"revision,omitempty"`
}

type aciSubnetWire struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
}

type aciDNSWire struct {
	Options       string   `json:"options,omitempty"`
	SearchDomains string   `json:"searchDomains,omitempty"`
	NameServers   []string `json:"nameServers"`
}

type aciLogAnalyticsWire struct {
	Metadata            map[string]string `json:"metadata,omitempty"`
	WorkspaceID         string            `json:"workspaceId"`
	WorkspaceKey        string            `json:"workspaceKey,omitempty"`
	LogType             string            `json:"logType,omitempty"`
	WorkspaceResourceID string            `json:"workspaceResourceId,omitempty"`
}

type aciDiagnosticsWire struct {
	LogAnalytics *aciLogAnalyticsWire `json:"logAnalytics,omitempty"`
}

func (p *aciProbeWire) normalize(owner, kind string) error {
	switch {
	case p.Exec == nil && p.HTTPGet == nil:
		return aciBad("container %q %s needs exec or httpGet", owner, kind)
	case p.Exec != nil && p.HTTPGet != nil:
		return aciBad("container %q %s must define only one of exec and httpGet", owner, kind)
	case p.InitialDelaySeconds < 0 || p.PeriodSeconds < 0 || p.FailureThreshold < 0 || p.SuccessThreshold < 0 ||
		p.TimeoutSeconds < 0:
		return aciBad("container %q %s seconds and thresholds cannot be negative", owner, kind)
	}

	if p.Exec != nil && len(p.Exec.Command) == 0 {
		return aciBad("container %q %s exec needs a command", owner, kind)
	}

	if g := p.HTTPGet; g != nil {
		if err := g.normalize(owner, kind); err != nil {
			return err
		}
	}

	p.PeriodSeconds = defaultInt(p.PeriodSeconds, aciProbeDefPeriod)
	p.FailureThreshold = defaultInt(p.FailureThreshold, aciProbeDefFailure)
	p.SuccessThreshold = defaultInt(p.SuccessThreshold, aciProbeDefSuccess)
	p.TimeoutSeconds = defaultInt(p.TimeoutSeconds, aciProbeDefTimeout)

	return nil
}

func (g *aciHTTPGetWire) normalize(owner, kind string) error {
	if g.Port < 1 || g.Port > aciMaxPort {
		return aciBad("container %q %s httpGet port %d is out of range", owner, kind, g.Port)
	}

	switch strings.ToLower(g.Scheme) {
	case "", acaScaleHTTP:
		g.Scheme = acaScaleHTTP
	case acaSchemeHTTPS:
		g.Scheme = acaSchemeHTTPS
	default:
		return aciBad("container %q %s httpGet scheme %q must be http or https", owner, kind, g.Scheme)
	}

	for _, h := range g.HTTPHeaders {
		if h.Name == "" {
			return aciBad("container %q %s httpGet headers need a name", owner, kind)
		}
	}

	return nil
}

func defaultInt(v, def int) int {
	if v == 0 {
		return def
	}

	return v
}

func (c *aciContainerWire) normalizeProbes() error {
	if p := c.Properties.LivenessProbe; p != nil {
		if err := p.normalize(c.Name, "livenessProbe"); err != nil {
			return err
		}
	}

	if p := c.Properties.ReadinessProbe; p != nil {
		return p.normalize(c.Name, "readinessProbe")
	}

	return nil
}

func (p *aciPropsWire) normalizeInit(vols, names map[string]bool) error {
	if len(p.InitContainers) > aciMaxInitContainers {
		return aciBad("a container group supports at most %d init containers", aciMaxInitContainers)
	}

	for i := range p.InitContainers {
		ic := &p.InitContainers[i]
		key := strings.ToLower(ic.Name)

		switch {
		case ic.Name == "" || len(ic.Name) > aciMaxNameLen || !aciNameRe.MatchString(ic.Name):
			return aciBad("init container name %q must be 1-63 letters, digits or hyphens", ic.Name)
		case names[key]:
			return aciBad("duplicate container name %q", ic.Name)
		case ic.Properties.Image == "":
			return aciBad("init container %q needs an image", ic.Name)
		}

		names[key] = true

		if err := validateEnvMounts(ic.Name, ic.Properties.EnvironmentVariables, ic.Properties.VolumeMounts,
			vols); err != nil {
			return err
		}

		ic.Properties.InstanceView = nil
	}

	return nil
}

func (g *aciGitRepoWire) validate(volume string) error {
	switch {
	case strings.TrimSpace(g.Repository) == "":
		return aciBad("gitRepo volume %q needs a repository", volume)
	case strings.HasPrefix(g.Repository, "-"):
		return aciBad("gitRepo volume %q repository cannot start with '-'", volume)
	case strings.HasPrefix(g.Directory, "/") || strings.Contains(g.Directory, ".."):
		return aciBad("gitRepo volume %q directory must not contain or start with '..' or '/'", volume)
	case strings.HasPrefix(g.Revision, "-"):
		return aciBad("gitRepo volume %q revision cannot start with '-'", volume)
	}

	return nil
}

func normalizeACIZones(zones []string) error {
	if len(zones) == 0 {
		return nil
	}

	if len(zones) > 1 {
		return aciBad("a container group can be placed in only one availability zone")
	}

	switch zones[0] {
	case "1", "2", "3":
		return nil
	}

	return aciBad("zone %q must be 1, 2 or 3", zones[0])
}

func (p *aciPropsWire) normalizeNetwork() error {
	if len(p.SubnetIDs) > 0 {
		for _, s := range p.SubnetIDs {
			if s.ID == "" {
				return aciBad("subnetIds entries need an id")
			}
		}

		if p.IPAddress != nil && p.IPAddress.Type != aciIPPrivate {
			return aciBad("a container group in a virtual network needs ipAddress.type Private")
		}
	}

	d := p.DNSConfig
	if d == nil {
		return nil
	}

	if len(d.NameServers) == 0 || len(d.NameServers) > aciMaxDNSServers {
		return aciBad("dnsConfig.nameServers needs between 1 and %d addresses", aciMaxDNSServers)
	}

	for _, ns := range d.NameServers {
		if _, err := netip.ParseAddr(ns); err != nil {
			return aciBad("dnsConfig.nameServers entry %q is not an IP address", ns)
		}
	}

	if strings.ContainsAny(d.Options+d.SearchDomains, "\r\n") {
		return aciBad("dnsConfig options and searchDomains must be single-line")
	}

	return nil
}

func (p *aciPropsWire) normalizeDiagnostics() error {
	if p.Diagnostics == nil {
		return nil
	}

	la := p.Diagnostics.LogAnalytics
	if la == nil {
		return aciBad("diagnostics needs logAnalytics")
	}

	if la.WorkspaceID == "" || la.WorkspaceKey == "" {
		return aciBad("diagnostics.logAnalytics needs workspaceId and workspaceKey")
	}

	switch strings.ToLower(la.LogType) {
	case "":
	case strings.ToLower(aciLogTypeInsights):
		la.LogType = aciLogTypeInsights
	case strings.ToLower(aciLogTypeLogs):
		la.LogType = aciLogTypeLogs
	default:
		return aciBad("diagnostics.logAnalytics.logType must be %s or %s", aciLogTypeInsights, aciLogTypeLogs)
	}

	return nil
}

func validateEnvMounts(owner string, env []aciEnvWire, mounts []aciMountWire, vols map[string]bool) error {
	for _, e := range env {
		if e.Name == "" || (e.Value == nil) == (e.SecureValue == nil) {
			return aciBad("container %q environment variables need a name and exactly one of value or secureValue",
				owner)
		}
	}

	for _, m := range mounts {
		if !vols[strings.ToLower(m.Name)] || !strings.HasPrefix(m.MountPath, "/") {
			return fmt.Errorf("%w: container %q has a volume mount for an unknown volume or a relative mountPath",
				errACIInvalid, owner)
		}
	}

	return nil
}
