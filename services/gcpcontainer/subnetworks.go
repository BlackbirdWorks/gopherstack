package gcpcontainer

import (
	"slices"
	"strings"

	"cloud.google.com/go/container/apiv1/containerpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultSubnetPage = 500
	filterNetProject  = "networkProjectId"
)

func subnetFilterProject(filter string) (string, error) {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return "", nil
	}

	k, v, ok := strings.Cut(filter, "=")
	if !ok || strings.TrimSpace(k) != filterNetProject || strings.TrimSpace(v) == "" {
		return "", errInvalid(
			"filter",
			"Filtering supports only equality on %s: %s=[PROJECTID].",
			filterNetProject,
			filterNetProject,
		)
	}

	return strings.Trim(strings.TrimSpace(v), `"`), nil
}

func (b *InMemoryBackend) rangeStatusLocked(
	project string,
	s SubnetInfo,
	name string,
) containerpb.UsableSubnetworkSecondaryRange_Status {
	subName := s.Subnetwork[strings.LastIndex(s.Subnetwork, "/")+1:]
	status := containerpb.UsableSubnetworkSecondaryRange_UNUSED

	for _, c := range b.clusters {
		cl := c.Cluster
		if c.Project != project || cl.GetSubnetwork() != subName || regionOf(cl.GetLocation()) != s.Region ||
			c.DeleteOp != "" {
			continue
		}

		switch name {
		case cl.GetIpAllocationPolicy().GetServicesSecondaryRangeName():
			return containerpb.UsableSubnetworkSecondaryRange_IN_USE_SERVICE
		case cl.GetIpAllocationPolicy().GetClusterSecondaryRangeName():
			status = containerpb.UsableSubnetworkSecondaryRange_IN_USE_MANAGED_POD
		}
	}

	return status
}

// ListUsableSubnetworks lists the subnetworks of the Compute Engine emulator that clusters of a project can use.
func (b *InMemoryBackend) ListUsableSubnetworks(
	req *containerpb.ListUsableSubnetworksRequest,
) (*containerpb.ListUsableSubnetworksResponse, error) {
	ids, ok := parseRes(req.GetParent(), kindProjects)
	if !ok {
		return nil, errInvalid("parent", "Invalid parent %q: want projects/{project}.", req.GetParent())
	}

	netProject, err := subnetFilterProject(req.GetFilter())
	if err != nil {
		return nil, err
	}

	if req.GetPageSize() < 0 {
		return nil, errInvalid("page_size", "Page size must not be negative.")
	}

	project := ids[0]

	b.mu.RLock("ListUsableSubnetworks")
	defer b.mu.RUnlock()

	var all []*containerpb.UsableSubnetwork

	if b.compute != nil {
		b.compute.EachSubnetwork(project, func(s SubnetInfo) {
			if netProject != "" && s.NetworkProj != netProject {
				return
			}

			all = append(all, b.usableSubnetLocked(project, s))
		})
	}

	pg := page.New(all, req.GetPageToken(), int(req.GetPageSize()), defaultSubnetPage)

	return &containerpb.ListUsableSubnetworksResponse{Subnetworks: pg.Data, NextPageToken: pg.Next}, nil
}

func (b *InMemoryBackend) usableSubnetLocked(project string, s SubnetInfo) *containerpb.UsableSubnetwork {
	u := &containerpb.UsableSubnetwork{Subnetwork: s.Subnetwork, Network: s.Network, IpCidrRange: s.CIDR}

	names := make([]string, 0, len(s.Secondary))
	for n := range s.Secondary {
		names = append(names, n)
	}

	slices.Sort(names)

	for _, n := range names {
		u.SecondaryIpRanges = append(u.SecondaryIpRanges, &containerpb.UsableSubnetworkSecondaryRange{
			RangeName: n, IpCidrRange: s.Secondary[n], Status: b.rangeStatusLocked(project, s, n),
		})
	}

	return u
}
