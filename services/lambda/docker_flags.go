package lambda

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	bytesPerKiB     = 1 << 10
	bytesPerMiB     = 1 << 20
	bytesPerGiB     = 1 << 30
	minDockerMemory = 6 * bytesPerMiB
	nanoCPUsPerCPU  = 1_000_000_000
	volumeMaxParts  = 3
	shortFlagLen    = 2
	volumeMinParts  = 2
)

var errInvalidDockerFlags = errors.New("invalid LAMBDA_DOCKER_FLAGS")

var platformPattern = regexp.MustCompile(`^[a-z0-9]+/[a-z0-9_]+(/[a-z0-9]+)?$`)

//nolint:gochecknoglobals // static allow-list of bind-mount options
var allowedVolumeOptions = map[string]struct{}{
	"ro": {}, "rw": {}, "z": {}, "Z": {}, "delegated": {}, "cached": {}, "consistent": {},
}

//nolint:gochecknoglobals // container paths owned by the Lambda sandbox
var reservedMountTargets = map[string]struct{}{
	"/": {}, "/var/task": {}, "/var/runtime": {}, "/var/lang": {}, "/opt": {},
}

//nolint:gochecknoglobals // host sockets that hand over the container engine
var deniedMountSources = map[string]struct{}{
	"/": {}, "/var/run/docker.sock": {}, "/run/docker.sock": {}, "/var/run/podman/podman.sock": {},
	"/run/podman/podman.sock": {},
}

// dockerFlags is the allow-listed subset of LAMBDA_DOCKER_FLAGS applied to function containers.
type dockerFlags struct {
	Network     string
	Platform    string
	Env         []string
	Mounts      []string
	ExtraHosts  []string
	DNS         []string
	MemoryBytes int64
	NanoCPUs    int64
}

// flagSplitter accumulates shell-style words without performing any expansion.
type flagSplitter struct {
	cur     strings.Builder
	tokens  []string
	quote   rune
	inToken bool
	escaped bool
}

func (f *flagSplitter) flush() {
	if f.inToken {
		f.tokens = append(f.tokens, f.cur.String())
		f.cur.Reset()

		f.inToken = false
	}
}

func (f *flagSplitter) feed(r rune) {
	switch {
	case f.escaped:
		f.cur.WriteRune(r)

		f.escaped = false
	case r == '\\' && f.quote != '\'':
		f.escaped, f.inToken = true, true
	case f.quote != 0:
		if r == f.quote {
			f.quote = 0
		} else {
			f.cur.WriteRune(r)
		}
	case r == '\'' || r == '"':
		f.quote, f.inToken = r, true
	case unicode.IsSpace(r):
		f.flush()
	default:
		f.cur.WriteRune(r)

		f.inToken = true
	}
}

// splitDockerFlags tokenises s like a shell would split words (quotes and
// backslash escapes) without performing any expansion or evaluation.
func splitDockerFlags(s string) ([]string, error) {
	var f flagSplitter

	for _, r := range s {
		f.feed(r)
	}

	if f.quote != 0 || f.escaped {
		return nil, fmt.Errorf("%w: unterminated quote or escape", errInvalidDockerFlags)
	}

	f.flush()

	return f.tokens, nil
}

const (
	flagEnv      = "env"
	flagVolume   = "volume"
	flagAddHost  = "add-host"
	flagDNS      = "dns"
	flagNetwork  = "network"
	flagMemory   = "memory"
	flagCPUs     = "cpus"
	flagPlatform = "platform"
)

// flagAliases maps each supported spelling to its canonical long name.
//
//nolint:gochecknoglobals // static alias table
var flagAliases = map[string]string{
	"-e": flagEnv, "--env": flagEnv,
	"-v": flagVolume, "--volume": flagVolume,
	"--add-host": flagAddHost, "--dns": flagDNS,
	"--network": flagNetwork, "--net": flagNetwork,
	"-m": flagMemory, "--memory": flagMemory,
	"--cpus": flagCPUs, "--platform": flagPlatform,
}

// parseDockerFlags parses s into the allow-listed options. Anything outside the
// allow-list, or invalid, is dropped and reported in warnings.
func parseDockerFlags(s string) (dockerFlags, []string) {
	var f dockerFlags

	if strings.TrimSpace(s) == "" {
		return f, nil
	}

	tokens, err := splitDockerFlags(s)
	if err != nil {
		return f, []string{err.Error()}
	}

	var warnings []string

	for i := 0; i < len(tokens); i++ {
		name, value, hasValue, ok := cutFlag(tokens[i])
		if !ok {
			warnings = append(warnings, fmt.Sprintf("ignoring unexpected argument %q", tokens[i]))

			continue
		}

		canon, supported := flagAliases[name]
		if !supported {
			warnings = append(warnings, fmt.Sprintf("flag %s is not permitted", name))

			if !hasValue && i+1 < len(tokens) && !strings.HasPrefix(tokens[i+1], "-") {
				i++ // its value, e.g. "--cap-add SYS_ADMIN"
			}

			continue
		}

		if !hasValue {
			if i+1 >= len(tokens) {
				warnings = append(warnings, fmt.Sprintf("flag %s needs a value", name))

				continue
			}

			i++
			value = tokens[i]
		}

		if msg := f.set(canon, value); msg != "" {
			warnings = append(warnings, fmt.Sprintf("%s %q: %s", name, value, msg))
		}
	}

	return f, warnings
}

// cutFlag splits one token into flag name and an attached value (--flag=v or -eV).
func cutFlag(tok string) (string, string, bool, bool) {
	switch {
	case strings.HasPrefix(tok, "--"):
		name, val, has := strings.Cut(tok, "=")

		return name, val, has, true
	case strings.HasPrefix(tok, "-") && len(tok) > 1:
		if _, known := flagAliases[tok]; known || len(tok) == shortFlagLen {
			return tok, "", false, true
		}

		return tok[:shortFlagLen], strings.TrimPrefix(tok[shortFlagLen:], "="), true, true
	default:
		return "", "", false, false
	}
}

func (f *dockerFlags) set(canon, value string) string {
	switch canon {
	case flagEnv:
		return f.addEnv(value)
	case flagVolume:
		return f.addVolume(value)
	case flagAddHost:
		return f.addHost(value)
	case flagDNS:
		return f.addDNS(value)
	case flagNetwork:
		return f.setNetwork(value)
	case flagMemory:
		return f.setMemory(value)
	case flagCPUs:
		return f.setCPUs(value)
	case flagPlatform:
		return f.setPlatform(value)
	}

	return ""
}

func (f *dockerFlags) addHost(value string) string {
	host, ip, ok := strings.Cut(value, ":")
	if !ok || host == "" || ip == "" {
		return "want host:ip"
	}

	f.ExtraHosts = append(f.ExtraHosts, value)

	return ""
}

func (f *dockerFlags) addDNS(value string) string {
	if _, err := netip.ParseAddr(value); err != nil {
		return "not an IP address"
	}

	f.DNS = append(f.DNS, value)

	return ""
}

func (f *dockerFlags) setNetwork(value string) string {
	if value == "" || value == "host" || strings.HasPrefix(value, "container:") {
		return "network mode not permitted"
	}

	f.Network = value

	return ""
}

func (f *dockerFlags) setMemory(value string) string {
	n, ok := parseMemoryBytes(value)
	if !ok {
		return "invalid memory size"
	}

	f.MemoryBytes = n

	return ""
}

func (f *dockerFlags) setCPUs(value string) string {
	c, err := strconv.ParseFloat(value, 64)
	if err != nil || c <= 0 || math.IsNaN(c) || math.IsInf(c, 0) {
		return "invalid cpu count"
	}

	f.NanoCPUs = int64(c * nanoCPUsPerCPU)

	return ""
}

func (f *dockerFlags) setPlatform(value string) string {
	if !platformPattern.MatchString(value) {
		return "want os/arch"
	}

	f.Platform = value

	return ""
}

func (f *dockerFlags) addEnv(value string) string {
	key, _, hasEq := strings.Cut(value, "=")
	if key == "" || strings.ContainsAny(key, " \t") {
		return "invalid variable name"
	}

	if isReservedEnvKey(key) {
		return "reserved Lambda variable"
	}

	if !hasEq {
		v, set := os.LookupEnv(key)
		if !set {
			return ""
		}

		value = key + "=" + v
	}

	f.Env = append(f.Env, value)

	return ""
}

func (f *dockerFlags) addVolume(value string) string {
	parts := strings.Split(value, ":")
	if len(parts) < volumeMinParts || len(parts) > volumeMaxParts {
		return "want source:target[:options]"
	}

	src, dst := parts[0], parts[1]
	if src == "" || strings.HasPrefix(src, ".") {
		return "invalid source"
	}

	if _, bad := deniedMountSources[src]; bad {
		return "source not permitted"
	}

	if !strings.HasPrefix(dst, "/") {
		return "target must be absolute"
	}

	if _, bad := reservedMountTargets[strings.TrimRight(dst, "/")]; bad || dst == "/" {
		return "target reserved by the Lambda sandbox"
	}

	if len(parts) == volumeMaxParts {
		for opt := range strings.SplitSeq(parts[2], ",") {
			if _, ok := allowedVolumeOptions[opt]; !ok {
				return "mount option not permitted"
			}
		}
	}

	f.Mounts = append(f.Mounts, value)

	return ""
}

// parseMemoryBytes parses docker-style sizes such as 512m, 1g, 65536.
func parseMemoryBytes(s string) (int64, bool) {
	s = strings.ToLower(strings.TrimSuffix(strings.ToLower(s), "b"))
	mult := int64(1)

	switch {
	case strings.HasSuffix(s, "k"):
		mult, s = bytesPerKiB, strings.TrimSuffix(s, "k")
	case strings.HasSuffix(s, "m"):
		mult, s = bytesPerMiB, strings.TrimSuffix(s, "m")
	case strings.HasSuffix(s, "g"):
		mult, s = bytesPerGiB, strings.TrimSuffix(s, "g")
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n > math.MaxInt64/mult || n*mult < minDockerMemory {
		return 0, false
	}

	return n * mult, true
}

// apply overlays the flags on spec; user env is appended last so it wins.
func (f *dockerFlags) apply(spec *container.Spec) {
	spec.Env = append(spec.Env, f.Env...)
	spec.Mounts = append(spec.Mounts, f.Mounts...)
	spec.ExtraHosts = append(spec.ExtraHosts, f.ExtraHosts...)
	spec.DNS = append(spec.DNS, f.DNS...)

	if f.Network != "" {
		spec.Network = f.Network
	}

	if f.MemoryBytes > 0 {
		spec.MemoryBytes = f.MemoryBytes
	}

	if f.NanoCPUs > 0 {
		spec.NanoCPUs = f.NanoCPUs
	}

	if f.Platform != "" {
		spec.Platform = f.Platform
	}
}
