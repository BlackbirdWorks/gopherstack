package app

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	resourceexplorer2backend "github.com/blackbirdworks/gopherstack/services/resourceexplorer2"
)

const (
	inventoryPageSize = 500
	inventoryMaxItems = 10000
)

type inventoryResource = resourceexplorer2backend.SourceResource

// inventorySource lists one service's resources, tagged or not, for the Region carried by ctx. Entries
// carry tags only where the service hands them out for free; the tagging aggregation, which is consulted
// first, supplies tags for the rest.
type inventorySource func(ctx context.Context) []inventoryResource

func (f inventorySource) Resources(ctx context.Context) []inventoryResource { return f(ctx) }

// collectPages follows a marker-paged list to its end, bounded by inventoryMaxItems.
func collectPages[T any](list func(marker string) (page.Page[T], error), add func(T)) {
	marker := ""

	for range inventoryMaxItems / inventoryPageSize {
		p, err := list(marker)
		if err != nil {
			return
		}

		for _, item := range p.Data {
			add(item)
		}

		if p.Next == "" {
			return
		}

		marker = p.Next
	}
}

func iamInventory(bk *iambackend.InMemoryBackend) inventorySource {
	return func(context.Context) []inventoryResource {
		var out []inventoryResource

		collectPages(
			func(m string) (page.Page[iambackend.Role], error) { return bk.ListRoles(m, inventoryPageSize) },
			func(r iambackend.Role) { out = append(out, inventoryResource{ARN: r.Arn, Tags: r.Tags}) })
		collectPages(
			func(m string) (page.Page[iambackend.User], error) { return bk.ListUsers(m, inventoryPageSize) },
			func(u iambackend.User) { out = append(out, inventoryResource{ARN: u.Arn, Tags: u.Tags}) })
		collectPages(
			func(m string) (page.Page[iambackend.Group], error) { return bk.ListGroups(m, inventoryPageSize) },
			func(g iambackend.Group) { out = append(out, inventoryResource{ARN: g.Arn}) })
		collectPages(
			func(m string) (page.Page[iambackend.Policy], error) { return bk.ListPolicies(m, inventoryPageSize) },
			func(p iambackend.Policy) { out = append(out, inventoryResource{ARN: p.Arn, Tags: p.Tags}) })

		return out
	}
}

func cfnInventory(h *cfnbackend.Handler) inventorySource {
	return func(ctx context.Context) []inventoryResource {
		bk, ok := regionpeers.Backend[*cfnbackend.InMemoryBackend](h.BackendFor, awsmeta.Region(ctx))
		if !ok {
			return nil
		}

		var out []inventoryResource

		collectPages(
			func(token string) (page.Page[cfnbackend.StackSummary], error) { return bk.ListStacks(nil, token) },
			func(s cfnbackend.StackSummary) {
				if s.StackStatus != "DELETE_COMPLETE" {
					out = append(out, inventoryResource{ARN: s.StackID})
				}
			})

		return out
	}
}

func elbv2Inventory(h *elbv2backend.Handler, home *elbv2backend.InMemoryBackend) inventorySource {
	backend := regionalELBv2Backend(h, home)

	return func(ctx context.Context) []inventoryResource {
		bk := backend(awsmeta.Region(ctx))

		var out []inventoryResource

		if lbs, err := bk.DescribeLoadBalancers(nil, nil); err == nil {
			for i := range lbs {
				out = append(out, inventoryResource{ARN: lbs[i].LoadBalancerArn})
			}
		}

		if tgs, err := bk.DescribeTargetGroups(nil, nil, ""); err == nil {
			for i := range tgs {
				out = append(out, inventoryResource{ARN: tgs[i].TargetGroupArn})
			}
		}

		return out
	}
}

func elbInventory(bk *elbbackend.InMemoryBackend) inventorySource {
	return func(ctx context.Context) []inventoryResource {
		lbs, err := bk.DescribeLoadBalancers(ctx, nil)
		if err != nil {
			return nil
		}

		out := make([]inventoryResource, 0, len(lbs))
		for i := range lbs {
			out = append(out, inventoryResource{ARN: lbs[i].ARN})
		}

		return out
	}
}

// inventorySources builds the untagged-inclusive sources of the services the tagging aggregation does not
// carry at all; every service it does carry lists its untagged resources through InventoryResources.
func inventorySources(byName map[string]service.Registerable) []resourceexplorer2backend.ResourceSource {
	var out []resourceexplorer2backend.ResourceSource

	if h, ok := byName["IAM"].(*iambackend.Handler); ok {
		if bk, bkOK := h.Backend.(*iambackend.InMemoryBackend); bkOK {
			out = append(out, iamInventory(bk))
		}
	}

	if h, ok := byName["CloudFormation"].(*cfnbackend.Handler); ok {
		out = append(out, cfnInventory(h))
	}

	if h, ok := byName["ELBv2"].(*elbv2backend.Handler); ok {
		if home, homeOK := h.Backend.(*elbv2backend.InMemoryBackend); homeOK {
			out = append(out, elbv2Inventory(h, home))
		}
	}

	if h, ok := byName["ELB"].(*elbbackend.Handler); ok {
		if bk, bkOK := h.Backend.(*elbbackend.InMemoryBackend); bkOK {
			out = append(out, elbInventory(bk))
		}
	}

	return out
}
