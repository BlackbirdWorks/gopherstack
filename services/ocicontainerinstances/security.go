package ocicontainerinstances

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	capAll          = "ALL"
	maxSecurityID   = 1<<31 - 1
	maxGraceSeconds = 300
)

// grantable reports whether containerinstances.ContainerCapabilityTypeEnum lets a container add c; ALL is refused
// on purpose and none of the others is a privileged capability such as SYS_ADMIN.
func grantable(c string) bool {
	switch c {
	case "CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FSETID", "CAP_FOWNER", "CAP_MKNOD", "CAP_NET_RAW", "CAP_SETGID",
		"CAP_SETUID", "CAP_SETFCAP", "CAP_SETPCAP", "CAP_NET_BIND_SERVICE", "CAP_SYS_CHROOT", "CAP_KILL",
		"CAP_AUDIT_WRITE":
		return true
	}

	return false
}

type linuxSecurity struct {
	Capabilities *struct {
		Add  []string `json:"addCapabilities"`
		Drop []string `json:"dropCapabilities"`
	} `json:"capabilities"`
	RunAsUser    *int   `json:"runAsUser"`
	RunAsGroup   *int   `json:"runAsGroup"`
	FsGroup      *int   `json:"fsGroup"`
	NonRootCheck *bool  `json:"isNonRootUserCheckEnabled"`
	ReadonlyRoot *bool  `json:"isRootFileSystemReadonly"`
	Type         string `json:"securityContextType"`
}

func parseSecurity(raw jsontext.Value) (linuxSecurity, bool) {
	var sc linuxSecurity
	if len(raw) == 0 || string(raw) == "null" {
		return sc, true
	}

	if json.Unmarshal(raw, &sc, jsonx.V1()) != nil {
		return sc, false
	}

	return sc, true
}

func validID(v *int) bool { return v == nil || (*v >= 0 && *v <= maxSecurityID) }

func checkSecurity(raw jsontext.Value) *APIError {
	sc, ok := parseSecurity(raw)
	if !ok || (len(raw) > 0 && string(raw) != "null" && sc.Type != "LINUX") {
		return errParam("securityContext.securityContextType must be LINUX")
	}

	if !validID(sc.RunAsUser) || !validID(sc.RunAsGroup) || !validID(sc.FsGroup) {
		return errParam("securityContext runAsUser, runAsGroup and fsGroup must be between 0 and 2147483647")
	}

	if sc.NonRootCheck != nil && *sc.NonRootCheck && sc.RunAsUser != nil && *sc.RunAsUser == 0 {
		return errParam("securityContext.isNonRootUserCheckEnabled forbids runAsUser 0")
	}

	return checkCaps(sc)
}

func checkCaps(sc linuxSecurity) *APIError {
	if sc.Capabilities == nil {
		return nil
	}

	for _, c := range sc.Capabilities.Add {
		if !grantable(c) {
			return errParam("securityContext.capabilities.addCapabilities cannot contain " + c)
		}
	}

	for _, c := range sc.Capabilities.Drop {
		if c != capAll && !grantable(c) {
			return errParam("securityContext.capabilities.dropCapabilities cannot contain " + c)
		}
	}

	return nil
}

// applySecurity maps the instance and container security contexts onto the runtime spec; container fields win.
func applySecurity(sp *container.Spec, inst, ctr jsontext.Value) {
	a, _ := parseSecurity(inst)
	b, _ := parseSecurity(ctr)

	pick := func(x, y *int) *int {
		if y != nil {
			return y
		}

		return x
	}

	user, group := pick(a.RunAsUser, b.RunAsUser), pick(a.RunAsGroup, b.RunAsGroup)
	if user != nil {
		sp.User = strconv.Itoa(*user)
		if group != nil {
			sp.User += ":" + strconv.Itoa(*group)
		}
	}

	if fs := pick(a.FsGroup, b.FsGroup); fs != nil {
		sp.GroupAdd = append(sp.GroupAdd, strconv.Itoa(*fs))
	}

	for _, sc := range []linuxSecurity{a, b} {
		if sc.ReadonlyRoot != nil {
			sp.ReadOnlyRootfs = *sc.ReadonlyRoot
		}

		if sc.Capabilities != nil {
			sp.CapDrop = append(sp.CapDrop, trimCaps(sc.Capabilities.Drop)...)
			sp.CapAdd = append(sp.CapAdd, trimCaps(sc.Capabilities.Add)...)
		}
	}

	sp.SecurityOpt = append(sp.SecurityOpt, "no-new-privileges")
}

func trimCaps(in []string) []string {
	out := make([]string, 0, len(in))
	for _, c := range in {
		out = append(out, strings.TrimPrefix(c, "CAP_"))
	}

	return out
}

// graceFor is the stop grace period of an instance, capped so a stop cannot hold the engine for long.
func graceFor(inst *Instance) *int {
	if inst.Graceful == nil || *inst.Graceful < 0 {
		return nil
	}

	g := int(min(*inst.Graceful, maxGraceSeconds))

	return &g
}

func teardownFor(inst *Instance) time.Duration {
	if g := graceFor(inst); g != nil {
		return teardownTimeout + time.Duration(*g)*time.Second
	}

	return teardownTimeout
}
