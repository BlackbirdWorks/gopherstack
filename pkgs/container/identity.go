package container

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
)

// Labels stamped on every container gopherstack starts.
const labelTrue = "true"

const (
	// LabelManaged marks a container as started by gopherstack.
	LabelManaged = "gopherstack.managed"
	// LabelService names the service that owns the container.
	LabelService = "gopherstack.service"
	// LabelOwner is the instance identity: stable per data dir when persistence is on, fresh per run otherwise.
	LabelOwner = "gopherstack.owner"
	// LabelResource is the owning service's key for the resource; only labelled containers can be adopted.
	LabelResource = "gopherstack.resource"
	// LabelPID is "pid:starttime" of the launching process.
	LabelPID = "gopherstack.pid"
	// LabelEphemeral is "true" when the owner id is per-run (persistence off).
	LabelEphemeral = "gopherstack.ephemeral"
)

// Identity is the per-process instance identity stamped onto launched containers.
type Identity struct {
	Owner     string
	Start     string
	PID       int
	Ephemeral bool
}

var current atomic.Pointer[Identity] //nolint:gochecknoglobals // process-wide identity

// SetPersistentOwner derives a stable owner id from the persistence data dir.
func SetPersistentOwner(dataDir string) Identity {
	if abs, err := filepath.Abs(dataDir); err == nil {
		dataDir = abs
	}

	sum := sha256.Sum256([]byte(dataDir))
	id := newIdentity("dir-"+hex.EncodeToString(sum[:6]), false)
	current.Store(&id)

	return id
}

// SetEphemeralOwner gives the process a fresh owner id, so no earlier run's containers are ever adopted.
func SetEphemeralOwner() Identity {
	id := newIdentity("run-"+randomHex(), true)
	current.Store(&id)

	return id
}

// CurrentIdentity returns the process identity, creating an ephemeral one on first use.
func CurrentIdentity() Identity {
	if id := current.Load(); id != nil {
		return *id
	}

	id := newIdentity("run-"+randomHex(), true)
	if current.CompareAndSwap(nil, &id) {
		return id
	}

	return *current.Load()
}

func newIdentity(owner string, ephemeral bool) Identity {
	pid := os.Getpid()
	start, _ := processStart(pid)

	return Identity{Owner: owner, PID: pid, Start: start, Ephemeral: ephemeral}
}

const randomBytes = 8

func randomHex() string {
	b := make([]byte, randomBytes)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// Labels are the identity labels stamped on every managed container.
func (i Identity) Labels() map[string]string {
	l := map[string]string{
		LabelManaged: labelTrue,
		LabelOwner:   i.Owner,
		LabelPID:     strconv.Itoa(i.PID) + ":" + i.Start,
	}
	if i.Ephemeral {
		l[LabelEphemeral] = labelTrue
	}

	return l
}

// ResourceLabels labels a container so a restarted gopherstack can match it to resource (the service's own key).
func ResourceLabels(service, resource string) map[string]string {
	return map[string]string{LabelService: service, LabelResource: resource}
}

// ServiceLabels names the owning service without a resource key: the container is never adopted, and a restarted
// gopherstack removes it at startup before the service relaunches.
func ServiceLabels(service string) map[string]string {
	return map[string]string{LabelService: service}
}

// ManagedLabels are the labels for a container started outside DockerRuntime: this process's identity plus the
// service labels, with the resource key when resource is non-empty (only then can a restart adopt it).
func ManagedLabels(service, resource string) map[string]string {
	l := CurrentIdentity().Labels()

	if resource == "" {
		maps.Copy(l, ServiceLabels(service))

		return l
	}

	maps.Copy(l, ResourceLabels(service, resource))

	return l
}
