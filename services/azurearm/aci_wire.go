package azurearm

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
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
	Image                string            `json:"image"`
	Command              []string          `json:"command,omitempty"`
	Args                 []string          `json:"-"`
	EnvironmentVariables []aciEnvWire      `json:"environmentVariables,omitempty"`
	Resources            *aciResourcesWire `json:"resources,omitempty"`
	Ports                []aciPortWire     `json:"ports,omitempty"`
	VolumeMounts         []aciMountWire    `json:"volumeMounts,omitempty"`
	InstanceView         json.RawMessage   `json:"instanceView,omitempty"`
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
	EmptyDir  *json.RawMessage  `json:"emptyDir,omitempty"`
	Secret    map[string]string `json:"secret,omitempty"`
	AzureFile *aciAzureFileWire `json:"azureFile,omitempty"`
	GitRepo   *json.RawMessage  `json:"gitRepo,omitempty"`
	Name      string            `json:"name"`
}

// aciPropsWire is the supported subset of containerGroups properties; unknown properties are rejected.
type aciPropsWire struct {
	OSType                   string             `json:"osType,omitempty"`
	RestartPolicy            string             `json:"restartPolicy,omitempty"`
	SKU                      string             `json:"sku,omitempty"`
	Priority                 string             `json:"priority,omitempty"`
	IPAddress                *aciIPWire         `json:"ipAddress,omitempty"`
	ProvisioningState        string             `json:"provisioningState,omitempty"`
	InstanceView             json.RawMessage    `json:"instanceView,omitempty"`
	Containers               []aciContainerWire `json:"containers"`
	ImageRegistryCredentials []aciCredWire      `json:"imageRegistryCredentials,omitempty"`
	Volumes                  []aciVolumeWire    `json:"volumes,omitempty"`
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

	buf, err := json.Marshal(raw)
	if err != nil {
		return p, aciBad("properties: %v", err)
	}

	dec := json.NewDecoder(bytes.NewReader(buf))
	dec.DisallowUnknownFields()

	if err = dec.Decode(&p); err != nil {
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

	names := map[string]bool{}
	ports := map[int]bool{}

	for i := range p.Containers {
		if err = p.Containers[i].normalize(vols, names, ports); err != nil {
			return err
		}
	}

	if err = p.normalizeIP(ports); err != nil {
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

	if p.SKU != "" && !strings.EqualFold(p.SKU, "Standard") {
		return aciUnsupported("sku " + p.SKU)
	}

	if p.Priority != "" && !strings.EqualFold(p.Priority, "Regular") {
		return aciUnsupported("priority " + p.Priority)
	}

	p.SKU, p.Priority = "", ""

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
		return aciBad("volume %q must define exactly one of emptyDir, secret, azureFile", v.Name)
	case v.GitRepo != nil:
		return aciUnsupported("gitRepo volumes")
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

func (c *aciContainerWire) normalize(vols, names map[string]bool, ports map[int]bool) error {
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

		ports[pt.Port] = true
	}

	c.Properties.InstanceView = nil

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

	for _, e := range c.Properties.EnvironmentVariables {
		if e.Name == "" || (e.Value == nil) == (e.SecureValue == nil) {
			return aciBad("container %q environment variables need a name and exactly one of value or secureValue",
				c.Name)
		}
	}

	for _, m := range c.Properties.VolumeMounts {
		if !vols[strings.ToLower(m.Name)] || !strings.HasPrefix(m.MountPath, "/") {
			return aciBad("container %q has a volume mount for an unknown volume or a relative mountPath", c.Name)
		}
	}

	return nil
}

func normalizePort(pt *aciPortWire) error {
	if pt.Port < 1 || pt.Port > aciMaxPort {
		return aciBad("port %d is out of range", pt.Port)
	}

	switch strings.ToUpper(pt.Protocol) {
	case "", aciProtoTCP:
		pt.Protocol = aciProtoTCP
	default:
		return aciUnsupported("protocol " + pt.Protocol)
	}

	return nil
}

func (p *aciPropsWire) normalizeIP(containerPorts map[int]bool) error {
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

		if !containerPorts[ip.Ports[i].Port] {
			return aciBad("ipAddress port %d is not exposed by any container", ip.Ports[i].Port)
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
