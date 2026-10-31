package ociloadbalancer

import (
	"maps"
	"net/http"
	"slices"
)

func buildOps() map[string]opFunc {
	m := map[string]opFunc{}

	for _, set := range []map[string]opFunc{lbOps(), backendSetOps(), subresourceOps(), healthOps()} {
		maps.Copy(m, set)
	}

	return m
}

func lbOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateLB: (*Handler).createLB, opGetLB: (*Handler).getLB, opListLB: (*Handler).listLBs,
		opUpdateLB: (*Handler).updateLB, opDeleteLB: (*Handler).deleteLB, opChangeComp: (*Handler).changeCompartment,
		opUpdateShape: (*Handler).updateShape, opUpdateNSG: (*Handler).updateNSGs,
		opListShapes: staticList(shapeNames), opListPolicies: staticList(policyNames),
		opListProtocols: staticList(protocolNames), opGetWR: (*Handler).getWorkRequest,
		opListWR: (*Handler).listWorkRequests,
	}
}

func staticList(names []string) opFunc {
	return func(_ *Handler, q *req) error {
		out := make([]map[string]string, 0, len(names))
		for _, n := range names {
			out = append(out, map[string]string{"name": n})
		}

		return q.json(http.StatusOK, out, "")
	}
}

func (h *Handler) createLB(q *req) error {
	var in createLBIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.CreateLoadBalancer(in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getLB(q *req) error {
	lb, etag, err := h.Backend.GetLoadBalancer(q.param("loadBalancerId"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, lb, etag)
}

func (h *Handler) listLBs(q *req) error {
	pg, err := h.Backend.ListLoadBalancers(
		q.query("compartmentId"), q.query("displayName"), q.query("lifecycleState"),
		!equalFold(q.query("detail"), "simple"), q.listOpts(),
	)
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) updateLB(q *req) error {
	return withBody(q, func(in updateLBIn) (string, error) {
		return h.Backend.UpdateLoadBalancer(q.param("loadBalancerId"), q.ifMatch(), in)
	})
}

func (h *Handler) updateShape(q *req) error {
	return withBody(q, func(in updateShapeIn) (string, error) {
		return h.Backend.UpdateShape(q.param("loadBalancerId"), q.ifMatch(), in)
	})
}

func (h *Handler) updateNSGs(q *req) error {
	return withBody(q, func(in nsgIn) (string, error) {
		return h.Backend.UpdateNSGs(q.param("loadBalancerId"), q.ifMatch(), in)
	})
}

func (h *Handler) changeCompartment(q *req) error {
	return withBody(q, func(in changeCompartmentIn) (string, error) {
		return h.Backend.ChangeCompartment(q.param("loadBalancerId"), q.ifMatch(), in)
	})
}

func (h *Handler) deleteLB(q *req) error {
	wr, err := h.Backend.DeleteLoadBalancer(q.param("loadBalancerId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) getWorkRequest(q *req) error {
	w, err := h.Backend.GetWorkRequest(q.param("workRequestId"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, "")
}

func (h *Handler) listWorkRequests(q *req) error {
	pg, err := h.Backend.ListWorkRequests(q.param("loadBalancerId"), q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

// withBody decodes the request body, runs the operation and answers 202 with its work request id.
func withBody[I any](q *req, fn func(in I) (string, error)) error {
	var in I
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := fn(in)

	return acceptedWR(q, wr, err)
}

func acceptedWR(q *req, wr string, err error) error {
	if err != nil {
		return err
	}

	return q.accepted(wr)
}

// mutateBody decodes the body and applies fn to the addressed load balancer.
func mutateBody[I any](
	h *Handler,
	q *req,
	typ string,
	fn func(b *InMemoryBackend, lb *LoadBalancer, in *I) *APIError,
) error {
	return withBody(q, func(in I) (string, error) {
		return h.Backend.Mutate(q.param("loadBalancerId"), q.ifMatch(), typ, false, func(lb *LoadBalancer) *APIError {
			return fn(h.Backend, lb, &in)
		})
	})
}

// mutateDelete applies fn to the addressed load balancer; dangling references answer 409.
func mutateDelete(h *Handler, q *req, typ string, fn func(b *InMemoryBackend, lb *LoadBalancer) *APIError) error {
	wr, err := h.Backend.Mutate(q.param("loadBalancerId"), q.ifMatch(), typ, true, func(lb *LoadBalancer) *APIError {
		return fn(h.Backend, lb)
	})

	return acceptedWR(q, wr, err)
}

// readSub answers with one sub-resource of the addressed load balancer.
func readSub[T any](h *Handler, q *req, pick func(lb *LoadBalancer) (T, *APIError)) error {
	var (
		out  T
		etag string
	)

	err := h.Backend.Read(q.param("loadBalancerId"), func(lb *LoadBalancer) *APIError {
		v, perr := pick(lb)
		out, etag = v, lb.Etag

		return perr
	})
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, out, etag)
}

func lookup[T any](m map[string]*T, name string) (*T, *APIError) {
	v, ok := m[name]
	if !ok {
		return nil, errNotFound(name)
	}

	return v, nil
}

func sortedValues[T any](m map[string]*T) []*T {
	keys := slices.Sorted(maps.Keys(m))
	out := make([]*T, 0, len(keys))

	for _, k := range keys {
		out = append(out, m[k])
	}

	return out
}

// getByName builds a Get operation for a map-held sub-resource addressed by a path parameter.
func getByName[T any](param string, m func(*LoadBalancer) map[string]*T) opFunc {
	return func(h *Handler, q *req) error {
		return readSub(h, q, func(lb *LoadBalancer) (*T, *APIError) { return lookup(m(lb), q.param(param)) })
	}
}

// listAll builds a List operation for a map-held sub-resource.
func listAll[T any](m func(*LoadBalancer) map[string]*T) opFunc {
	return func(h *Handler, q *req) error {
		return readSub(h, q, func(lb *LoadBalancer) ([]*T, *APIError) { return sortedValues(m(lb)), nil })
	}
}

// deleteByName builds a Delete operation for a map-held sub-resource.
func deleteByName[T any](typ, param string, m func(*LoadBalancer) map[string]*T) opFunc {
	return func(h *Handler, q *req) error {
		name := q.param(param)

		return mutateDelete(h, q, typ, func(_ *InMemoryBackend, lb *LoadBalancer) *APIError {
			if _, err := lookup(m(lb), name); err != nil {
				return err
			}

			delete(m(lb), name)

			return nil
		})
	}
}

// Read runs fn against a load balancer under the read lock.
func (b *InMemoryBackend) Read(id string, fn func(lb *LoadBalancer) *APIError) error {
	b.mu.RLock("Read")
	defer b.mu.RUnlock()

	lb, err := b.liveLB(id)
	if err != nil {
		return err
	}

	if lb.State == stateDeleted {
		return errNotFound(id)
	}

	if ferr := fn(lb); ferr != nil {
		return ferr
	}

	return nil
}
