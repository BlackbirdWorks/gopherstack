package acm

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const inventoryPageSize = 1000

// InventoryResources lists every certificate, ACME endpoint, external account binding and domain validation of
// the request Region plus every tagged ARN of the tag store.
func (h *Handler) InventoryResources(ctx context.Context) []TaggedEntry {
	arns := h.resourceARNs(ctx)
	out := h.TaggedResources()

	seen := make(map[string]struct{}, len(out))
	for _, e := range out {
		seen[e.ARN] = struct{}{}
	}

	for _, a := range arns {
		if _, ok := seen[a]; !ok {
			seen[a] = struct{}{}
			out = append(out, TaggedEntry{ARN: a})
		}
	}

	return out
}

func (h *Handler) resourceARNs(ctx context.Context) []string {
	var out []string

	certs := drainPages(func(token string) (page.Page[Certificate], error) {
		return h.Backend.ListCertificates(ctx, ListCertificatesParams{NextToken: token, MaxItems: inventoryPageSize})
	})

	for i := range certs {
		out = append(out, certs[i].ARN)
	}

	endpoints := drainPages(func(token string) (page.Page[AcmeEndpoint], error) {
		return h.Backend.ListAcmeEndpoints(ctx, token, inventoryPageSize)
	})

	for i := range endpoints {
		ep := endpoints[i].ARN
		out = append(out, ep)

		ebs := drainPages(func(token string) (page.Page[AcmeExternalAccountBinding], error) {
			return h.Backend.ListAcmeExternalAccountBindings(ctx, ep, token, inventoryPageSize)
		})

		for j := range ebs {
			out = append(out, ebs[j].ARN)
		}

		dvs := drainPages(func(token string) (page.Page[AcmeDomainValidation], error) {
			return h.Backend.ListAcmeDomainValidations(ctx, ep, token, inventoryPageSize)
		})

		for j := range dvs {
			out = append(out, dvs[j].ARN)
		}
	}

	return out
}

func drainPages[T any](list func(token string) (page.Page[T], error)) []T {
	var out []T

	token := ""

	for {
		p, err := list(token)
		if err != nil {
			return out
		}

		out = append(out, p.Data...)

		if p.Next == "" {
			return out
		}

		token = p.Next
	}
}
