package ociloadbalancer

import "slices"

// BackendAddr is the address of a backend.
type BackendAddr struct {
	IP   string
	Port int
}

// BackendChange adds and removes backends of one backend set.
type BackendChange struct {
	Set    string
	Add    []BackendAddr
	Remove []BackendAddr
}

// HasBackendSet reports whether the load balancer exists and has the backend set.
func (b *InMemoryBackend) HasBackendSet(id, set string) bool {
	return b.Read(id, func(lb *LoadBalancer) *APIError {
		_, err := lookup(lb.BackendSets, set)

		return err
	}) == nil
}

// ApplyBackends adds and removes backends in one update of the load balancer. Adding an existing backend and
// removing a missing one are no-ops.
func (b *InMemoryBackend) ApplyBackends(id string, changes []BackendChange) error {
	_, err := b.Mutate(id, "", "UpdateBackendSet", "", false, func(lb *LoadBalancer) *APIError {
		for _, c := range changes {
			bs, err := lookup(lb.BackendSets, c.Set)
			if err != nil {
				return err
			}

			for _, a := range c.Add {
				be, berr := buildBackend(backendIn{IPAddress: a.IP, Port: &a.Port})
				if berr != nil {
					return berr
				}

				if findBackend(bs, be.Name) < 0 {
					bs.Backends = append(bs.Backends, be)
				}
			}

			for _, r := range c.Remove {
				bs.Backends = slices.DeleteFunc(bs.Backends, func(be Backend) bool {
					return be.Name == backendName(r.IP, r.Port)
				})
			}
		}

		return nil
	})

	return err
}
