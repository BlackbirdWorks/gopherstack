package glue

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// cloneDevEndpoint returns a deep copy of a DevEndpoint.
func cloneDevEndpoint(dep *DevEndpoint) *DevEndpoint {
	cp := *dep
	cp.Arguments = maps.Clone(dep.Arguments)
	cp.Tags = maps.Clone(dep.Tags)
	cp.SecurityGroupIDs = append([]string(nil), dep.SecurityGroupIDs...)
	cp.PublicKeys = append([]string(nil), dep.PublicKeys...)

	return &cp
}

// devEndpointARN returns the ARN for a Glue dev endpoint.
func (b *InMemoryBackend) devEndpointARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "devEndpoint/"+name)
}

// BatchGetDevEndpoints retrieves multiple dev endpoints by name.
func (b *InMemoryBackend) BatchGetDevEndpoints(names []string) ([]*DevEndpoint, []string) {
	b.mu.RLock("BatchGetDevEndpoints")
	defer b.mu.RUnlock()

	found := make([]*DevEndpoint, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		dep, ok := b.devEndpoints.Get(name)
		if !ok {
			missing = append(missing, name)

			continue
		}

		found = append(found, cloneDevEndpoint(dep))
	}

	return found, missing
}

// AddDevEndpointInternal adds a dev endpoint directly to the backend without validation.
func (b *InMemoryBackend) AddDevEndpointInternal(dep *DevEndpoint) {
	b.mu.Lock("AddDevEndpointInternal")
	defer b.mu.Unlock()

	b.devEndpoints.Put(cloneDevEndpoint(dep))
}

// UpdateDevEndpointOptions carries the optional UpdateDevEndpoint fields
// beyond AddArguments/DeleteArguments.
type UpdateDevEndpointOptions struct {
	PublicKey        string
	AddPublicKeys    []string
	DeletePublicKeys []string
}

// UpdateDevEndpoint updates an existing dev endpoint's arguments and public
// keys, mirroring UpdateDevEndpointInput's AddArguments/DeleteArguments/
// AddPublicKeys/DeletePublicKeys/PublicKey semantics.
func (b *InMemoryBackend) UpdateDevEndpoint(
	name string,
	addArgs map[string]string,
	deleteArgs []string,
	opts UpdateDevEndpointOptions,
) error {
	b.mu.Lock("UpdateDevEndpoint")
	defer b.mu.Unlock()

	dep, ok := b.devEndpoints.Get(name)
	if !ok {
		return fmt.Errorf("dev endpoint %q not found: %w", name, ErrNotFound)
	}

	if dep.Arguments == nil {
		dep.Arguments = make(map[string]string)
	}

	maps.Copy(dep.Arguments, addArgs)

	for _, k := range deleteArgs {
		delete(dep.Arguments, k)
	}

	if opts.PublicKey != "" {
		dep.PublicKey = opts.PublicKey
	}

	if len(opts.DeletePublicKeys) > 0 {
		del := make(map[string]bool, len(opts.DeletePublicKeys))
		for _, k := range opts.DeletePublicKeys {
			del[k] = true
		}

		kept := dep.PublicKeys[:0:0]
		for _, k := range dep.PublicKeys {
			if !del[k] {
				kept = append(kept, k)
			}
		}

		dep.PublicKeys = kept
	}

	dep.PublicKeys = append(dep.PublicKeys, opts.AddPublicKeys...)
	dep.LastModifiedTimestamp = float64(time.Now().Unix())
	dep.LastUpdateStatus = "SUCCESS"

	return nil
}

// CreateDevEndpoint creates a new Glue dev endpoint. RoleArn is required, per
// CreateDevEndpointInput.
func (b *InMemoryBackend) CreateDevEndpoint(
	name string,
	input DevEndpointInput,
	roleArn string,
	tags map[string]string,
) (*DevEndpoint, error) {
	b.mu.Lock("CreateDevEndpoint")
	defer b.mu.Unlock()

	if name == "" || roleArn == "" {
		return nil, fmt.Errorf("%w: EndpointName and RoleArn are required", ErrValidation)
	}

	if b.devEndpoints.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	dep := &DevEndpoint{
		EndpointName:                       name,
		RoleArn:                            roleArn,
		Status:                             stateReady,
		ARN:                                b.devEndpointARN(name),
		Tags:                               maps.Clone(tags),
		Arguments:                          maps.Clone(input.Arguments),
		SecurityGroupIDs:                   append([]string(nil), input.SecurityGroupIDs...),
		SubnetID:                           input.SubnetID,
		PublicKey:                          input.PublicKey,
		PublicKeys:                         append([]string(nil), input.PublicKeys...),
		WorkerType:                         input.WorkerType,
		GlueVersion:                        input.GlueVersion,
		NumberOfNodes:                      input.NumberOfNodes,
		NumberOfWorkers:                    input.NumberOfWorkers,
		ExtraPythonLibsS3Path:              input.ExtraPythonLibsS3Path,
		ExtraJarsS3Path:                    input.ExtraJarsS3Path,
		SecurityConfiguration:              input.SecurityConfiguration,
		AvailabilityZone:                   b.region + "a",
		VpcID:                              "vpc-" + name,
		YarnEndpointAddress:                "internal-" + name + ".yarn." + b.region + ".amazonaws.com",
		PrivateAddress:                     "internal-" + name + "-private." + b.region + ".amazonaws.com",
		ZeppelinRemoteSparkInterpreterPort: 9007, //nolint:mnd // AWS's fixed Zeppelin interpreter port
		CreatedTimestamp:                   now,
		LastModifiedTimestamp:              now,
	}
	b.devEndpoints.Put(dep)

	return cloneDevEndpoint(dep), nil
}

// GetDevEndpoint retrieves a Glue dev endpoint by name.
func (b *InMemoryBackend) GetDevEndpoint(name string) (*DevEndpoint, error) {
	b.mu.RLock("GetDevEndpoint")
	defer b.mu.RUnlock()

	dep, ok := b.devEndpoints.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneDevEndpoint(dep), nil
}

// GetAllDevEndpoints returns all dev endpoints sorted by name.
func (b *InMemoryBackend) GetAllDevEndpoints() []*DevEndpoint {
	b.mu.RLock("GetAllDevEndpoints")
	defer b.mu.RUnlock()

	src := b.devEndpoints.Snapshot()
	out := make([]*DevEndpoint, 0, len(src))
	for _, dep := range src {
		out = append(out, cloneDevEndpoint(dep))
	}

	return out
}

// DeleteDevEndpoint deletes a Glue dev endpoint by name.
func (b *InMemoryBackend) DeleteDevEndpoint(name string) error {
	b.mu.Lock("DeleteDevEndpoint")
	defer b.mu.Unlock()

	if !b.devEndpoints.Has(name) {
		return ErrNotFound
	}

	b.devEndpoints.Delete(name)

	return nil
}
