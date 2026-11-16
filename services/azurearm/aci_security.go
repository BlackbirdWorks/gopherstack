package azurearm

import (
	"context"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	aciSKUStandard     = "Standard"
	aciSKUDedicated    = "Dedicated"
	aciSKUConfidential = "Confidential"
	aciProtoUDP        = "UDP"
	aciMaxExtensions   = 32
	aciMaxCapabilities = 64
	aciMaxSeccompBytes = 1 << 18
	aciMaxCcePolicy    = 1 << 20
	aciMaxID           = 1 << 31
)

type aciCapsWire struct {
	Add  []string `json:"add,omitempty"`
	Drop []string `json:"drop,omitempty"`
}

// aciSecurityWire is containers[].properties.securityContext (SecurityContextDefinition).
type aciSecurityWire struct {
	AllowPrivilegeEscalation *bool        `json:"allowPrivilegeEscalation,omitempty"`
	Privileged               *bool        `json:"privileged,omitempty"`
	RunAsUser                *int64       `json:"runAsUser,omitempty"`
	RunAsGroup               *int64       `json:"runAsGroup,omitempty"`
	Capabilities             *aciCapsWire `json:"capabilities,omitempty"`
	SeccompProfile           string       `json:"seccompProfile,omitempty"`
}

type aciConfidentialWire struct {
	CcePolicy string `json:"ccePolicy,omitempty"`
}

type aciEncryptionWire struct {
	VaultBaseURL string `json:"vaultBaseUrl"`
	KeyName      string `json:"keyName"`
	KeyVersion   string `json:"keyVersion"`
	Identity     string `json:"identity,omitempty"`
}

type aciExtensionPropsWire struct {
	ExtensionType     string         `json:"extensionType"`
	Version           string         `json:"version"`
	Settings          jsontext.Value `json:"settings,omitempty"`
	ProtectedSettings jsontext.Value `json:"protectedSettings,omitempty"`
}

type aciExtensionWire struct {
	Properties *aciExtensionPropsWire `json:"properties,omitempty"`
	Name       string                 `json:"name"`
}

func (s *aciSecurityWire) validate(container string) error {
	if s == nil {
		return nil
	}

	switch {
	case s.Privileged != nil && *s.Privileged:
		return aciBad("container %q: securityContext.privileged true is not supported by this emulator", container)
	case s.RunAsUser != nil && (*s.RunAsUser < 0 || *s.RunAsUser > aciMaxID):
		return aciBad("container %q: securityContext.runAsUser is out of range", container)
	case s.RunAsGroup != nil && (*s.RunAsGroup < 0 || *s.RunAsGroup > aciMaxID):
		return aciBad("container %q: securityContext.runAsGroup is out of range", container)
	}

	if c := s.Capabilities; c != nil {
		if len(c.Add)+len(c.Drop) > aciMaxCapabilities {
			return aciBad("container %q: too many securityContext capabilities", container)
		}

		for _, n := range append(append([]string(nil), c.Add...), c.Drop...) {
			if !aciCapRe.MatchString(n) {
				return aciBad("container %q: capability %q must be upper-case letters and underscores", container, n)
			}
		}
	}

	return s.validateSeccomp(container)
}

func (s *aciSecurityWire) validateSeccomp(container string) error {
	if s.SeccompProfile == "" {
		return nil
	}

	raw, err := base64.StdEncoding.DecodeString(s.SeccompProfile)
	if err != nil || len(raw) > aciMaxSeccompBytes || !jsontext.Value(raw).IsValid() {
		return aciBad("container %q: securityContext.seccompProfile must be base64 of a JSON document", container)
	}

	var obj map[string]any
	if err = json.Unmarshal(raw, &obj, jsonx.V1()); err != nil {
		return aciBad("container %q: securityContext.seccompProfile must be a JSON object", container)
	}

	return nil
}

// apply maps the security context onto the container spec.
func (s *aciSecurityWire) apply(spec *container.Spec) {
	if s == nil {
		return
	}

	if s.RunAsUser != nil || s.RunAsGroup != nil {
		uid, gid := int64(0), int64(-1)
		if s.RunAsUser != nil {
			uid = *s.RunAsUser
		}

		if s.RunAsGroup != nil {
			gid = *s.RunAsGroup
		}

		spec.User = strconv.FormatInt(uid, 10)
		if gid >= 0 {
			spec.User += ":" + strconv.FormatInt(gid, 10)
		}
	}

	if c := s.Capabilities; c != nil {
		for _, n := range c.Add {
			spec.CapAdd = append(spec.CapAdd, strings.TrimPrefix(n, "CAP_"))
		}

		for _, n := range c.Drop {
			spec.CapDrop = append(spec.CapDrop, strings.TrimPrefix(n, "CAP_"))
		}
	}

	if s.AllowPrivilegeEscalation != nil && !*s.AllowPrivilegeEscalation {
		spec.SecurityOpt = append(spec.SecurityOpt, "no-new-privileges")
	}

	if s.SeccompProfile != "" {
		raw, _ := base64.StdEncoding.DecodeString(s.SeccompProfile)
		spec.SecurityOpt = append(spec.SecurityOpt, "seccomp="+string(raw))
	}
}

func (p *aciPropsWire) normalizeSKU() error {
	switch strings.ToLower(p.SKU) {
	case "":
	case strings.ToLower(aciSKUStandard):
		p.SKU = aciSKUStandard
	case strings.ToLower(aciSKUDedicated):
		p.SKU = aciSKUDedicated
	case strings.ToLower(aciSKUConfidential):
		p.SKU = aciSKUConfidential
	default:
		return aciBad("sku %q must be Standard, Dedicated or Confidential", p.SKU)
	}

	if c := p.ConfidentialComputeProps; c != nil && c.CcePolicy != "" {
		raw, err := base64.StdEncoding.DecodeString(c.CcePolicy)
		if err != nil || len(raw) > aciMaxCcePolicy {
			return aciBad("confidentialComputeProperties.ccePolicy must be base64")
		}
	}

	return nil
}

func (p *aciPropsWire) normalizeEncryption() error {
	e := p.EncryptionProperties
	if e == nil {
		return nil
	}

	u, err := url.Parse(e.VaultBaseURL)

	switch {
	case e.KeyName == "" || e.KeyVersion == "" || e.VaultBaseURL == "":
		return aciBad("encryptionProperties needs vaultBaseUrl, keyName and keyVersion")
	case err != nil || u.Scheme != acaSchemeHTTPS || u.Host == "":
		return aciBad("encryptionProperties.vaultBaseUrl must be an https URL")
	}

	return nil
}

func (p *aciPropsWire) normalizeExtensions() error {
	if len(p.Extensions) > aciMaxExtensions {
		return aciBad("a container group supports at most %d extensions", aciMaxExtensions)
	}

	seen := map[string]bool{}

	for i := range p.Extensions {
		x := &p.Extensions[i]
		key := strings.ToLower(x.Name)

		switch {
		case x.Name == "" || seen[key]:
			return aciBad("extension names must be unique and non-empty")
		case x.Properties == nil || x.Properties.ExtensionType == "" || x.Properties.Version == "":
			return aciBad("extension %q needs properties.extensionType and properties.version", x.Name)
		}

		seen[key] = true
		x.Properties.ProtectedSettings = nil
	}

	return nil
}

// logACIExternalEffects logs the platform features that have no local effect.
func logACIExternalEffects(ctx context.Context, cfg aciConfig) {
	p, log := cfg.Props, logger.Load(ctx)

	if p.SKU == aciSKUConfidential {
		log.InfoContext(ctx, "azurearm: would run the group in a hardware-attested enclave", "group", cfg.Name)
	}

	if p.SKU == aciSKUDedicated {
		log.InfoContext(ctx, "azurearm: would place the group on dedicated hardware", "group", cfg.Name)
	}

	if e := p.EncryptionProperties; e != nil {
		log.InfoContext(ctx, "azurearm: would encrypt group data with a customer-managed key",
			"group", cfg.Name, "vault", e.VaultBaseURL, "key", e.KeyName)
	}

	for _, x := range p.Extensions {
		log.InfoContext(ctx, "azurearm: would deploy the extension sidecar",
			"group", cfg.Name, "extension", x.Name, "type", x.Properties.ExtensionType)
	}
}
