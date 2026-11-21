package app

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
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

func ecsInventory(h *ecsbackend.Handler) inventorySource {
	return func(ctx context.Context) []inventoryResource {
		bk, ok := regionpeers.Backend[*ecsbackend.InMemoryBackend](h.BackendFor, awsmeta.Region(ctx))
		if !ok {
			return nil
		}

		clusters, err := bk.ListClusters()
		if err != nil {
			return nil
		}

		out := make([]inventoryResource, 0, len(clusters))

		for _, c := range clusters {
			out = append(out, inventoryResource{ARN: c.ClusterArn})

			services, serr := bk.ListServices(c.ClusterName, "", "")
			if serr != nil {
				continue
			}

			for _, s := range services {
				out = append(out, inventoryResource{ARN: s})
			}
		}

		return out
	}
}

func rdsInventory(h *rdsbackend.Handler) inventorySource {
	return func(ctx context.Context) []inventoryResource {
		bk := h.BackendFor(awsmeta.Region(ctx))

		var out []inventoryResource

		if instances, err := bk.DescribeDBInstances(""); err == nil {
			for i := range instances {
				out = append(out, inventoryResource{ARN: instances[i].DBInstanceArn})
			}
		}

		if clusters, err := bk.DescribeDBClusters(""); err == nil {
			for i := range clusters {
				out = append(out, inventoryResource{ARN: clusters[i].DBClusterArn})
			}
		}

		return out
	}
}

func cwLogsInventory(bk *cwlogsbackend.InMemoryBackend) inventorySource {
	return func(ctx context.Context) []inventoryResource {
		var out []inventoryResource

		token := ""

		for range inventoryMaxItems / inventoryPageSize {
			groups, next, err := bk.DescribeLogGroups(ctx, "", token, "", inventoryPageSize)
			if err != nil {
				return out
			}

			for _, g := range groups {
				out = append(out, inventoryResource{ARN: strings.TrimSuffix(g.Arn, ":*")})
			}

			if next == "" {
				return out
			}

			token = next
		}

		return out
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

// inventorySources builds the untagged-inclusive sources of every wired service that the tagging
// aggregation keeps only tagged entries for or does not carry at all.
func inventorySources(byName map[string]service.Registerable) []resourceexplorer2backend.ResourceSource {
	var out []resourceexplorer2backend.ResourceSource

	if h, ok := byName["ECS"].(*ecsbackend.Handler); ok {
		out = append(out, ecsInventory(h))
	}

	if h, ok := byName["RDS"].(*rdsbackend.Handler); ok {
		out = append(out, rdsInventory(h))
	}

	if h, ok := byName["CloudWatchLogs"].(*cwlogsbackend.Handler); ok {
		if bk, bkOK := h.Backend.(*cwlogsbackend.InMemoryBackend); bkOK {
			out = append(out, cwLogsInventory(bk))
		}
	}

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
