package ec2

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
)

// ErrDockerFlags is returned when GOPHERSTACK_EC2_DOCKER_FLAGS cannot be parsed.
var ErrDockerFlags = errors.New("invalid ec2 docker flags")

const (
	bytesPerKiB      = 1024
	nanoPerCPU       = 1_000_000_000
	flagPartsMin     = 2
	flagPartsMax     = 3
	publishPartsHost = 3
)

var (
	capNameRE = regexp.MustCompile(`^[A-Za-z_]+$`)
	netNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
)

type publishFlag struct {
	hostIP        string
	proto         string
	hostPort      int
	containerPort int
}

// dockerFlags is the validated subset of `docker run` options applied to every launched container.
type dockerFlags struct {
	network    string
	env        []string
	binds      []string
	capAdd     []string
	capDrop    []string
	publish    []publishFlag
	memory     int64
	nanoCPUs   int64
	privileged bool
}

func flagErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrDockerFlags, fmt.Sprintf(format, args...))
}

// parseDockerFlags parses a docker-run style option string. Only --privileged, -e/--env, -v/--volume,
// --cap-add, --cap-drop, -m/--memory, --cpus, --network/--net and -p/--publish are accepted.
// The result is applied to the Docker API directly; no shell ever sees the string.
func parseDockerFlags(s string) (dockerFlags, error) {
	var f dockerFlags

	words, err := splitFlagWords(s)
	if err != nil {
		return f, err
	}

	for i := 0; i < len(words); i++ {
		name, val, hasVal := splitFlagWord(words[i])
		if name == "--privileged" && !hasVal {
			f.privileged = true

			continue
		}

		if !isValueFlag(name) {
			return dockerFlags{}, flagErr("unsupported flag %q (supported: --privileged, -e, -v, --cap-add, "+
				"--cap-drop, --memory, --cpus, --network, -p)", words[i])
		}

		if !hasVal {
			i++
			if i >= len(words) {
				return dockerFlags{}, flagErr("flag %q needs a value", name)
			}

			val = words[i]
		}

		if applyErr := f.set(name, val); applyErr != nil {
			return dockerFlags{}, applyErr
		}
	}

	return f, nil
}

func isValueFlag(name string) bool {
	switch name {
	case "-e", "--env", "-v", "--volume", "--cap-add", "--cap-drop", "-m", "--memory",
		"--cpus", "--network", "--net", "-p", "--publish":
		return true
	}

	return false
}

// splitFlagWord splits "--flag=value" (long flags only) into its parts.
func splitFlagWord(w string) (string, string, bool) {
	if strings.HasPrefix(w, "--") {
		if name, val, ok := strings.Cut(w, "="); ok {
			return name, val, true
		}
	}

	return w, "", false
}

func (f *dockerFlags) set(name, val string) error {
	var err error

	switch name {
	case "-e", "--env":
		err = f.addEnv(val)
	case "-v", "--volume":
		err = f.addBind(val)
	case "--cap-add":
		f.capAdd, err = appendCap(f.capAdd, val)
	case "--cap-drop":
		f.capDrop, err = appendCap(f.capDrop, val)
	case "-m", "--memory":
		f.memory, err = parseMemory(val)
	case "--cpus":
		f.nanoCPUs, err = parseCPUs(val)
	case "--network", "--net":
		if !netNameRE.MatchString(val) {
			err = flagErr("invalid network name %q", val)
		}

		f.network = val
	default:
		err = f.addPublish(val)
	}

	return err
}

func (f *dockerFlags) addEnv(val string) error {
	key, _, ok := strings.Cut(val, "=")
	if !ok || key == "" {
		return flagErr("-e %q must be KEY=VALUE", val)
	}

	f.env = append(f.env, val)

	return nil
}

func (f *dockerFlags) addBind(val string) error {
	parts := strings.Split(val, ":")
	if len(parts) < flagPartsMin || len(parts) > flagPartsMax {
		return flagErr("-v %q must be source:target[:ro|rw]", val)
	}

	if parts[0] == "" || !strings.HasPrefix(parts[1], "/") {
		return flagErr("-v %q needs a non-empty source and an absolute target path", val)
	}

	if len(parts) == flagPartsMax && parts[2] != "ro" && parts[2] != "rw" {
		return flagErr("-v %q: mode must be ro or rw", val)
	}

	f.binds = append(f.binds, val)

	return nil
}

func appendCap(list []string, val string) ([]string, error) {
	if !capNameRE.MatchString(val) {
		return list, flagErr("invalid capability %q", val)
	}

	return append(list, strings.ToUpper(val)), nil
}

func parseMemory(val string) (int64, error) {
	v, mult := strings.ToLower(val), int64(1)

	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"b", 1}, {"k", bytesPerKiB}, {"m", bytesPerKiB * bytesPerKiB}, {"g", bytesPerKiB * bytesPerKiB * bytesPerKiB}} {
		if trimmed, ok := strings.CutSuffix(v, u.suffix); ok {
			v, mult = trimmed, u.mult

			break
		}
	}

	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 || n > math.MaxInt64/mult {
		return 0, flagErr("invalid --memory %q (use a positive integer with optional b, k, m or g)", val)
	}

	return n * mult, nil
}

func parseCPUs(val string) (int64, error) {
	c, err := strconv.ParseFloat(val, 64)
	if err != nil || c <= 0 || c > float64(math.MaxInt32) {
		return 0, flagErr("invalid --cpus %q (use a positive number)", val)
	}

	return int64(c * nanoPerCPU), nil
}

func (f *dockerFlags) addPublish(val string) error {
	spec, proto, _ := strings.Cut(val, "/")
	if proto == "" {
		proto = protoTCP
	}

	if proto != protoTCP && proto != "udp" {
		return flagErr("-p %q: protocol must be tcp or udp", val)
	}

	parts := strings.Split(spec, ":")
	if len(parts) < flagPartsMin || len(parts) > publishPartsHost {
		return flagErr("-p %q must be [hostIP:]hostPort:containerPort[/tcp|udp]", val)
	}

	p := publishFlag{proto: proto}

	if len(parts) == publishPartsHost {
		if _, err := netip.ParseAddr(parts[0]); err != nil {
			return flagErr("-p %q: invalid host IP", val)
		}

		p.hostIP, parts = parts[0], parts[1:]
	}

	var err error
	if p.hostPort, err = parsePortNumber(parts[0]); err != nil {
		return flagErr("-p %q: invalid host port", val)
	}

	if p.containerPort, err = parsePortNumber(parts[1]); err != nil {
		return flagErr("-p %q: invalid container port", val)
	}

	if p.containerPort == sshContainerPort && proto == protoTCP {
		return flagErr("-p %q: container port 22/tcp is reserved for sshd", val)
	}

	f.publish = append(f.publish, p)

	return nil
}

func parsePortNumber(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > maxPort {
		return 0, flagErr("port %q out of range", s)
	}

	return n, nil
}

// sshContainerPort is the in-container sshd port.
const sshContainerPort = 22

// wordSplitter accumulates shell-like words: whitespace separates, quotes group, backslash escapes.
type wordSplitter struct {
	words   []string
	cur     strings.Builder
	quote   rune
	inWord  bool
	escaped bool
}

func (w *wordSplitter) feed(r rune) {
	switch {
	case w.escaped:
		w.cur.WriteRune(r)

		w.escaped = false
	case r == '\\' && w.quote != '\'':
		w.escaped, w.inWord = true, true
	case w.quote != 0:
		if r == w.quote {
			w.quote = 0
		} else {
			w.cur.WriteRune(r)
		}
	case r == '\'' || r == '"':
		w.quote, w.inWord = r, true
	case unicode.IsSpace(r):
		w.flush()
	default:
		w.cur.WriteRune(r)

		w.inWord = true
	}
}

func (w *wordSplitter) flush() {
	if w.inWord {
		w.words = append(w.words, w.cur.String())
		w.cur.Reset()

		w.inWord = false
	}
}

// splitFlagWords tokenises s on whitespace, honouring single and double quotes and backslash escapes.
func splitFlagWords(s string) ([]string, error) {
	var w wordSplitter

	for _, r := range s {
		w.feed(r)
	}

	if w.quote != 0 || w.escaped {
		return nil, flagErr("unterminated quote or escape in %q", s)
	}

	w.flush()

	return w.words, nil
}

// apply layers the flags onto a container's host config, environment and exposed ports.
func (f *dockerFlags) apply(hc *mobycontainer.HostConfig, env []string, exposed mobynetwork.PortSet) []string {
	hc.Privileged = hc.Privileged || f.privileged
	hc.Binds = append(hc.Binds, f.binds...)
	hc.CapAdd = append(hc.CapAdd, f.capAdd...)
	hc.CapDrop = append(hc.CapDrop, f.capDrop...)

	if f.memory > 0 {
		hc.Memory = f.memory
	}

	if f.nanoCPUs > 0 {
		hc.NanoCPUs = f.nanoCPUs
	}

	for _, p := range f.publish {
		port, err := mobynetwork.ParsePort(fmt.Sprintf("%d/%s", p.containerPort, p.proto))
		if err != nil {
			continue
		}

		exposed[port] = struct{}{}
		hc.PortBindings[port] = append(hc.PortBindings[port], mobynetwork.PortBinding{
			HostIP: parseHostIP(p.hostIP), HostPort: strconv.Itoa(p.hostPort),
		})
	}

	return append(env, f.env...)
}
