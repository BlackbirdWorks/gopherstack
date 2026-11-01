package container

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
)

const (
	procStartField = 19
	pidLabelParts  = 2
)

// processStart is the kernel start time of pid ("" where /proc is unavailable).
func processStart(pid int) (string, bool) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return "", false
	}

	s := string(raw)

	rest := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
	if len(rest) <= procStartField {
		return "", false
	}

	return rest[procStartField], true
}

// processAlive reports whether the process in a LabelPID value ("pid:start") still runs. Anything unparseable or
// unverifiable counts as alive: a wrong "dead" would remove a live instance's containers.
func processAlive(label string) bool {
	parts := strings.SplitN(label, ":", pidLabelParts)

	pid, err := strconv.Atoi(parts[0])
	if err != nil || pid <= 0 {
		return true
	}

	if start, ok := processStart(pid); ok {
		return len(parts) < pidLabelParts || parts[1] == "" || start == parts[1]
	}

	if _, statErr := os.Stat("/proc/self"); statErr == nil {
		return false
	}

	p, err := os.FindProcess(pid)
	if err != nil {
		return true
	}

	return !errors.Is(p.Signal(syscall.Signal(0)), os.ErrProcessDone)
}
