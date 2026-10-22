package gcpredis

import (
	"sort"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/redis/apiv1/redispb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateInstance starts creating an instance; the returned operation completes once its Redis server is up.
func (b *InMemoryBackend) CreateInstance(in *redispb.CreateInstanceRequest) (*longrunningpb.Operation, error) {
	n, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if n.location == allLocs || !regionKnown(n.location) {
		return nil, errNotFound("Location %q is not supported.", n.parent())
	}

	if err = validateID(in.GetInstanceId()); err != nil {
		return nil, err
	}

	if in.GetInstance() == nil {
		return nil, errInvalid("instance is required.")
	}

	n.id = in.GetInstanceId()

	pb, err := buildInstance(n, in.GetInstance(), b.now())
	if err != nil {
		return nil, err
	}

	rec := &instRec{pb: pb}
	if pb.GetAuthEnabled() {
		rec.authString = newAuthString()
	}

	if err = b.insert(n.full(), rec); err != nil {
		return nil, err
	}

	return b.begin(
		n.parent(),
		"create",
		n.full(),
		func() (proto.Message, error) { return b.finishCreate(n.full(), rec) },
	)
}

func (b *InMemoryBackend) insert(name string, rec *instRec) error {
	b.mu.Lock("CreateInstance")
	defer b.mu.Unlock()

	if _, exists := b.instances[name]; exists {
		return errExists("Instance %q already exists.", name)
	}

	if len(b.instances) >= maxInstances {
		return errExhausted("At most %d instances can exist in the emulator.", maxInstances)
	}

	b.instances[name] = rec

	return nil
}

func (b *InMemoryBackend) finishCreate(name string, rec *instRec) (proto.Message, error) {
	b.mu.RLock("finishCreate")
	mode, auth := rec.pb.GetTransitEncryptionMode(), rec.authString
	b.mu.RUnlock()

	eng, certs, err := b.startEngine(mode, auth)
	if err != nil {
		b.mu.Lock("finishCreate")
		if b.instances[name] == rec {
			delete(b.instances, name)
		}
		b.mu.Unlock()

		return nil, err
	}

	b.mu.Lock("finishCreate")
	defer b.mu.Unlock()

	if b.instances[name] != rec {
		eng.Close()

		return nil, errGone
	}

	bindEngine(rec, eng, certs)
	rec.pb.State = redispb.Instance_READY

	return cloneInstance(rec.pb), nil
}

// GetInstance returns an instance.
func (b *InMemoryBackend) GetInstance(name string) (*redispb.Instance, error) {
	n, err := parseName(name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetInstance")
	defer b.mu.RUnlock()

	rec, ok := b.instances[n.full()]
	if !ok {
		return nil, errInstanceNotFound(n.full())
	}

	return cloneInstance(rec.pb), nil
}

// ListInstances lists instances under a location, or every location with "-".
func (b *InMemoryBackend) ListInstances(in *redispb.ListInstancesRequest) ([]*redispb.Instance, string, error) {
	n, err := parseParent(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	if in.GetPageSize() < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	if err = page.ValidateToken(in.GetPageToken()); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	prefix := "projects/" + n.project + "/locations/"
	if n.location != allLocs {
		prefix += n.location + "/"
	}

	b.mu.RLock("ListInstances")
	names := make([]string, 0, len(b.instances))

	for name := range b.instances {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	all := make([]*redispb.Instance, 0, len(names))
	for _, name := range names {
		all = append(all, cloneInstance(b.instances[name].pb))
	}
	b.mu.RUnlock()

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

// GetInstanceAuthString returns the AUTH string of an instance with AUTH enabled.
func (b *InMemoryBackend) GetInstanceAuthString(name string) (*redispb.InstanceAuthString, error) {
	n, err := parseName(name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetInstanceAuthString")
	defer b.mu.RUnlock()

	rec, ok := b.instances[n.full()]
	if !ok {
		return nil, errInstanceNotFound(n.full())
	}

	if rec.authString == "" {
		return nil, errPrecondition("AUTH is not enabled on instance %q.", n.full())
	}

	return &redispb.InstanceAuthString{AuthString: rec.authString}, nil
}

// readyLocked returns the instance when it is idle. Must hold b.mu.
func (b *InMemoryBackend) readyLocked(name string) (*instRec, resName, error) {
	n, err := parseName(name)
	if err != nil {
		return nil, n, err
	}

	rec, ok := b.instances[n.full()]
	if !ok {
		return nil, n, errInstanceNotFound(n.full())
	}

	if rec.pb.GetState() != redispb.Instance_READY || rec.exports > 0 {
		return nil, n, errPrecondition("Instance %q is not ready (state %s); retry once it is READY.",
			n.full(), rec.pb.GetState())
	}

	return rec, n, nil
}

// settle returns a work function that applies mutate and marks the instance READY.
func (b *InMemoryBackend) settle(name string, mutate func(*instRec)) workFn {
	return func() (proto.Message, error) {
		b.mu.Lock("settle")
		defer b.mu.Unlock()

		rec, ok := b.instances[name]
		if !ok {
			return nil, errGone
		}

		if mutate != nil {
			mutate(rec)
		}

		rec.pb.State = redispb.Instance_READY

		return cloneInstance(rec.pb), nil
	}
}

// DeleteInstance deletes an instance and stops its Redis server.
func (b *InMemoryBackend) DeleteInstance(name string) (*longrunningpb.Operation, error) {
	b.mu.Lock("DeleteInstance")
	rec, n, err := b.readyLocked(name)

	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	rec.pb.State = redispb.Instance_DELETING
	b.mu.Unlock()

	return b.begin(n.parent(), "delete", n.full(), func() (proto.Message, error) {
		b.mu.Lock("finishDelete")
		eng := rec.engine
		rec.engine = nil

		if b.instances[n.full()] == rec {
			delete(b.instances, n.full())
		}
		b.mu.Unlock()

		eng.Close()

		return &emptypb.Empty{}, nil
	})
}
