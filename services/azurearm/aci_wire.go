package azurearm

import (
	"bytes"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	aciRestartAlways    = "Always"
	aciRestartOnFailure = "OnFailure"
	aciRestartNever     = "Never"
	aciOSLinux          = "Linux"
	aciIPPublic         = "Public"
	aciIPPrivate        = "Private"
	aciProtoTCP         = "TCP"
	aciMaxNameLen       = 63
	aciMaxPort          = 65535
	aciMaxSecretBytes   = 1 << 20
)

var (
	errACIInvalid = errors.New("azurearm: invalid container group")

	aciCapRe  = regexp.MustCompile(`^[A-Z][A-Z_]*$`)
	aciNameRe = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$`)
	aciDNSRe  = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)
)

type aciEnvWire struct {
	Value       *string `json:"value,omitempty"`
	SecureValue *string `json:"secureValue,omitempty"`
	Name        string  `json:"name"`
}

type aciResourceWire struct {
	CPU        float64 `json:"cpu,omitempty"`
	MemoryInGB float64 `json:"memoryInGB,omitempty"`
}

type aciResourcesWire struct {
	Requests *aciResourceWire `json:"requests,omitempty"`
	Limits   *aciResourceWire `json:"limits,omitempty"`
}

type aciPortKey struct {
	proto string
	port  int
}

type aciPortWire struct {
	Protocol string `json:"protocol,omitempty"`
	Port     int    `json:"port"`
}

type aciMountWire struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

type aciContainerPropsWire struct {
	Resources            *aciResourcesWire `json:"resources,omitempty"`
	LivenessProbe        *aciProbeWire     `json:"livenessProbe,omitempty"`
	ReadinessProbe       *aciProbeWire     `json:"readinessProbe,omitempty"`
	SecurityContext      *aciSecurityWire  `json:"securityContext,omitempty"`
	ConfigMap            *aciConfigMapWire `json:"configMap,omitempty"`
	Image                string            `json:"image"`
	Command              []string          `json:"command,omitempty"`
	Args                 []string          `json:"-"`
	EnvironmentVariables []aciEnvWire      `json:"environmentVariables,omitempty"`
	Ports                []aciPortWire     `json:"ports,omitempty"`
	VolumeMounts         []aciMountWire    `json:"volumeMounts,omitempty"`
	InstanceView         jsontext.Value    `json:"instanceView,omitempty"`
}

type aciConfigMapWire struct {
	KeyValuePairs map[string]string `json:"keyValuePairs,omitempty"`
}

type aciContainerWire struct {
	Name       string                `json:"name"`
	Properties aciContainerPropsWire `json:"properties"`
}

type aciIPWire struct {
	Type         string        `json:"type,omitempty"`
	DNSNameLabel string        `json:"dnsNameLabel,omitempty"`
	IP           string        `json:"ip,omitempty"`
	Fqdn         string        `json:"fqdn,omitempty"`
	Ports        []aciPortWire `json:"ports,omitempty"`
}

type aciCredWire struct {
	Server   string `json:"server"`
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
}

type aciAzureFileWire struct {
	ShareName          string `json:"shareName"`
	StorageAccountName string `json:"storageAccountName"`
	StorageAccountKey  string `json:"storageAccountKey,omitempty"`
	ReadOnly           bool   `json:"readOnly,omitempty"`
}

type aciVolumeWire struct {
	EmptyDir  *jsontext.Value   `json:"emptyDir,omitempty"`
	Secret    map[string]string `json:"secret,omitempty"`
	AzureFile *aciAzureFileWire `json:"azureFile,omitempty"`
	GitRepo   *aciGitRepoWire   `json:"gitRepo,omitempty"`
	Name      string            `json:"name"`
}

// aciPropsWire is the supported subset of containerGroups properties; unknown properties are rejected.
type aciPropsWire struct {
	DNSConfig                *aciDNSWire          `json:"dnsConfig,omitempty"`
	ConfidentialComputeProps *aciConfidentialWire `json:"confidentialComputeProperties,omitempty"`
	EncryptionProperties     *aciEncryptionWire   `json:"encryptionProperties,omitempty"`
	Extensions               []aciExtensionWire   `json:"extensions,omitempty"`
	IPAddress                *aciIPWire           `json:"ipAddress,omitempty"`
	Diagnostics              *aciDiagnosticsWire  `json:"diagnostics,omitempty"`
	RestartPolicy            string               `json:"restartPolicy,omitempty"`
	SKU                      string               `json:"sku,omitempty"`
	Priority                 string               `json:"priority,omitempty"`
	ProvisioningState        string               `json:"provisioningState,omitempty"`
	OSType                   string               `json:"osType,omitempty"`
	InstanceView             jsontext.Value       `json:"instanceView,omitempty"`
	Volumes                  []aciVolumeWire      `json:"volumes,omitempty"`
	InitContainers           []aciInitWire        `json:"initContainers,omitempty"`
	SubnetIDs                []aciSubnetWire      `json:"subnetIds,omitempty"`
	ImageRegistryCredentials []aciCredWire        `json:"imageRegistryCredentials,omitempty"`
	Containers               []aciContainerWire   `json:"containers"`
}

func aciUnsupported(what string) error {
	return fmt.Errorf("%w: %s is not supported by this emulator", errACIInvalid, what)
}

func aciBad(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errACIInvalid, fmt.Sprintf(format, args...))
}

// parseACIProps strictly decodes properties, then normalizes and validates them.
func parseACIProps(raw any) (aciPropsWire, error) {
	var p aciPropsWire

	buf, err := json.Marshal(raw, jsonx.V1())
	if err != nil {
		return p, aciBad("properties: %v", err)
	}

	dec := jsontext.NewDecoder(bytes.NewReader(buf))

	if err = json.UnmarshalDecode(dec, &p, jsonx.V1(), json.RejectUnknownMembers(true)); err != nil {
		if name, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
			return p, aciUnsupported("property " + name)
		}

		return p, aciBad("properties: %v", err)
	}

	return p, p.normalize()
}

func (p *aciPropsWire) normalize() error {
	if err := p.normalizeEnums(); err != nil {
		return err
	}

	p.ProvisioningState, p.InstanceView = "", nil

	if len(p.Containers) == 0 {
		return aciBad("at least one container is required")
	}

	vols, err := p.normalizeVolumes()
	if err != nil {
		return err
	}

	if len(p.Containers) > aciMaxContainers {
		return aciBad("a container group supports at most %d containers", aciMaxContainers)
	}

	names := map[string]bool{}
	ports := map[aciPortKey]bool{}

	for i := range p.Containers {
		if err = p.Containers[i].normalize(vols, names, ports); err != nil {
			return err
		}
	}

	if err = p.checkSpot(); err != nil {
		return err
	}

	if err = p.checkGroupResources(); err != nil {
		return err
	}

	if err = p.normalizeInit(vols, names); err != nil {
		return err
	}

	if err = p.normalizeIP(ports); err != nil {
		return err
	}

	if err = p.normalizeNetwork(); err != nil {
		return err
	}

	if err = p.normalizeDiagnostics(); err != nil {
		return err
	}

	if err = p.normalizeEncryption(); err != nil {
		return err
	}

	if err = p.normalizeExtensions(); err != nil {
		return err
	}

	return p.normalizeCreds()
}

func (p *aciPropsWire) normalizeEnums() error {
	if p.OSType == "" {
		return aciBad("osType is required")
	}

	if !strings.EqualFold(p.OSType, aciOSLinux) {
		return aciUnsupported("osType " + p.OSType)
	}

	p.OSType = aciOSLinux

	switch strings.ToLower(p.RestartPolicy) {
	case "", strings.ToLower(aciRestartAlways):
		p.RestartPolicy = aciRestartAlways
	case strings.ToLower(aciRestartOnFailure):
		p.RestartPolicy = aciRestartOnFailure
	case strings.ToLower(aciRestartNever):
		p.RestartPolicy = aciRestartNever
	default:
		return aciBad("restartPolicy %q must be Always, OnFailure or Never", p.RestartPolicy)
	}

	if err := p.normalizeSKU(); err != nil {
		return err
	}

	return p.normalizePriority()
}

const (
	aciPrioritySpot    = "Spot"
	aciSpotMaxCPU      = 4
	aciSpotMaxMemoryGB = 16
)

// normalizePriority accepts Regular and Spot; the Spot limits are those of learn.microsoft.com/azure/
// container-instances/container-instances-spot-containers-overview (no public IP, no virtual network).
func (p *aciPropsWire) normalizePriority() error {
	switch {
	case p.Priority == "" || strings.EqualFold(p.Priority, "Regular"):
		p.Priority = ""
	case strings.EqualFold(p.Priority, aciPrioritySpot):
		p.Priority = aciPrioritySpot
	default:
		return aciBad("priority %q must be Regular or Spot", p.Priority)
	}

	return nil
}

func (p *aciPropsWire) checkSpot() error {
	if p.Priority != aciPrioritySpot {
		return nil
	}

	if p.SKU == aciSKUConfidential {
		return aciBad("Spot container groups do not support the Confidential SKU")
	}

	if len(p.SubnetIDs) > 0 {
		return aciBad("Spot container groups cannot be deployed behind a virtual network")
	}

	if p.IPAddress != nil && !strings.EqualFold(p.IPAddress.Type, "Private") {
		return aciBad("Spot container groups cannot have a public IP endpoint")
	}

	return nil
}

func (p *aciPropsWire) normalizeVolumes() (map[string]bool, error) {
	vols := map[string]bool{}

	for i := range p.Volumes {
		v := &p.Volumes[i]
		key := strings.ToLower(v.Name)

		if v.Name == "" || vols[key] {
			return nil, aciBad("volume names must be unique and non-empty")
		}

		vols[key] = true

		if err := v.validate(); err != nil {
			return nil, err
		}
	}

	return vols, nil
}

func (v *aciVolumeWire) validate() error {
	kinds := 0

	for _, set := range []bool{v.EmptyDir != nil, v.Secret != nil, v.AzureFile != nil, v.GitRepo != nil} {
		if set {
			kinds++
		}
	}

	switch {
	case kinds != 1:
		return aciBad("volume %q must define exactly one of emptyDir, secret, azureFile, gitRepo", v.Name)
	case v.GitRepo != nil:
		return v.GitRepo.validate(v.Name)
	case v.AzureFile != nil && (v.AzureFile.ShareName == "" || v.AzureFile.StorageAccountName == "" ||
		v.AzureFile.StorageAccountKey == ""):
		return aciBad("azureFile volume %q needs shareName, storageAccountName and storageAccountKey", v.Name)
	}

	for k, val := range v.Secret {
		dec, err := base64.StdEncoding.DecodeString(val)
		if err != nil || len(dec) > aciMaxSecretBytes || k == "" || strings.ContainsAny(k, "/\\") {
			return aciBad("secret volume %q key %q must be a file name with a base64 value", v.Name, k)
		}
	}

	return nil
}

func (c *aciContainerWire) normalize(vols, names map[string]bool, ports map[aciPortKey]bool) error {
	key := strings.ToLower(c.Name)

	switch {
	case c.Name == "" || len(c.Name) > aciMaxNameLen || !aciNameRe.MatchString(c.Name):
		return aciBad("container name %q must be 1-63 letters, digits or hyphens", c.Name)
	case names[key]:
		return aciBad("duplicate container name %q", c.Name)
	case c.Properties.Image == "":
		return aciBad("container %q needs an image", c.Name)
	}

	names[key] = true

	if err := c.validateRuntime(vols); err != nil {
		return err
	}

	for i := range c.Properties.Ports {
		pt := &c.Properties.Ports[i]
		if err := normalizePort(pt); err != nil {
			return err
		}

		ports[aciPortKey{port: pt.Port, proto: pt.Protocol}] = true
	}

	c.Properties.InstanceView = nil

	if err := c.Properties.SecurityContext.validate(c.Name); err != nil {
		return err
	}

	return c.normalizeProbes()
}

// checkGroupResources enforces the per-group maximum of the standard SKU (Learn: container-instances-resource-and-
// quota-limits, Standard Container Resources: 31 vCPU and 240 GB).
func (p *aciPropsWire) checkGroupResources() error {
	var cpu, mem float64

	for _, c := range p.Containers {
		if r := c.Properties.Resources; r != nil && r.Requests != nil {
			cpu += r.Requests.CPU
			mem += r.Requests.MemoryInGB
		}
	}

	maxCPU, maxMem := float64(aciMaxCPU), float64(aciMaxMemoryGB)
	if p.Priority == aciPrioritySpot {
		maxCPU, maxMem = aciSpotMaxCPU, aciSpotMaxMemoryGB
	}

	if cpu > maxCPU || mem > maxMem {
		return aciBad("the container group requests %g CPU and %g GB of memory; the maximum is %g CPU and %g GB",
			cpu, mem, maxCPU, maxMem)
	}

	return nil
}

func (c *aciContainerWire) validateRuntime(vols map[string]bool) error {
	var rq *aciResourceWire
	if r := c.Properties.Resources; r != nil {
		rq = r.Requests
	}

	if rq == nil || rq.CPU <= 0 || rq.MemoryInGB <= 0 {
		return aciBad("container %q needs resources.requests.cpu and memoryInGB", c.Name)
	}

	return validateEnvMounts(c.Name, c.Properties.EnvironmentVariables, c.Properties.VolumeMounts, vols)
}

func normalizePort(pt *aciPortWire) error {
	if pt.Port < 1 || pt.Port > aciMaxPort {
		return aciBad("port %d is out of range", pt.Port)
	}

	switch strings.ToUpper(pt.Protocol) {
	case "", aciProtoTCP:
		pt.Protocol = aciProtoTCP
	case aciProtoUDP:
		pt.Protocol = aciProtoUDP
	default:
		return aciUnsupported("protocol " + pt.Protocol)
	}

	return nil
}

func (p *aciPropsWire) normalizeIP(containerPorts map[aciPortKey]bool) error {
	ip := p.IPAddress
	if ip == nil {
		return nil
	}

	switch strings.ToLower(ip.Type) {
	case strings.ToLower(aciIPPublic):
		ip.Type = aciIPPublic
	case strings.ToLower(aciIPPrivate):
		ip.Type = aciIPPrivate
	default:
		return aciBad("ipAddress.type must be Public or Private")
	}

	if ip.DNSNameLabel != "" && (ip.Type != aciIPPublic || !aciDNSRe.MatchString(ip.DNSNameLabel)) {
		return aciBad("dnsNameLabel is only valid on Public addresses and must be a DNS label")
	}

	if len(ip.Ports) == 0 {
		return aciBad("ipAddress.ports is required")
	}

	for i := range ip.Ports {
		if err := normalizePort(&ip.Ports[i]); err != nil {
			return err
		}

		if !containerPorts[aciPortKey{port: ip.Ports[i].Port, proto: ip.Ports[i].Protocol}] {
			return aciBad(
				"ipAddress port %d/%s is not exposed by any container",
				ip.Ports[i].Port,
				ip.Ports[i].Protocol,
			)
		}
	}

	ip.IP, ip.Fqdn = "", ""

	return nil
}

func (p *aciPropsWire) normalizeCreds() error {
	for _, c := range p.ImageRegistryCredentials {
		if c.Server == "" || c.Username == "" {
			return aciBad("imageRegistryCredentials need server and username")
		}
	}

	return nil
}
