package ec2

import (
	"context"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"sync"

	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	vpcNetworkPrefix = "gopherstack-vpc-"
	labelManaged     = dockerLabelPrefix + "managed"
	labelVPC         = dockerLabelPrefix + "vpc"
	labelIPAM        = dockerLabelPrefix + "ipam"
	ipamVPCCIDR      = "vpc-cidr"
	ipamAuto         = "auto"
)

// networkAPI is the Docker network surface; a dockerAPI that lacks it keeps the shared-network behaviour.
type networkAPI interface {
	NetworkCreate(
		ctx context.Context, name string, opts mobyclient.NetworkCreateOptions,
	) (mobyclient.NetworkCreateResult, error)
	NetworkList(ctx context.Context, opts mobyclient.NetworkListOptions) (mobyclient.NetworkListResult, error)
	NetworkRemove(
		ctx context.Context, id string, opts mobyclient.NetworkRemoveOptions,
	) (mobyclient.NetworkRemoveResult, error)
}

// vpcNetwork is the Docker network backing one VPC. static is true when its subnet is the VPC CIDR.
type vpcNetwork struct {
	name   string
	note   string
	static bool
}

type netRegistry struct {
	nets map[string]vpcNetwork
	mu   sync.Mutex
}

func newNetRegistry() *netRegistry { return &netRegistry{nets: make(map[string]vpcNetwork)} }

func vpcNetworkName(vpcID string) string { return vpcNetworkPrefix + vpcID }

func vpcNetworkLabels(vpcID, ipam string) map[string]string {
	return map[string]string{labelManaged: ec2BooleanTrue, labelVPC: vpcID, labelIPAM: ipam}
}

func vpcNetworkFilter(vpcID string) mobyclient.Filters {
	f := mobyclient.Filters{}.Add("label", labelManaged+"="+ec2BooleanTrue)
	if vpcID != "" {
		f = f.Add("label", labelVPC+"="+vpcID)
	}

	return f
}

func mismatchNote(cidr string, cause error) string {
	if cause == nil {
		return fmt.Sprintf("VPC CIDR %q cannot back a Docker network", cidr)
	}

	return fmt.Sprintf(
		"VPC CIDR %s conflicts with a Docker address pool or network (%v); "+
			"instances get Docker-assigned addresses instead of their subnet addresses", cidr, cause)
}

// vpcNetworkFor returns the VPC's Docker network, creating it on first use. ok is false when
// per-VPC networking does not apply (explicit shared network, no VPC, or no network API).
func (d *DockerCompute) vpcNetworkFor(ctx context.Context, vpcID, cidr string) (vpcNetwork, bool, error) {
	na, isNet := d.api.(networkAPI)
	if !isNet || d.cfg.Network != "" || vpcID == "" {
		return vpcNetwork{}, false, nil
	}

	d.nets.mu.Lock()
	defer d.nets.mu.Unlock()

	if n, ok := d.nets.nets[vpcID]; ok {
		return n, true, nil
	}

	n, found, err := d.findVPCNetwork(ctx, na, vpcID, cidr)
	if err != nil {
		return vpcNetwork{}, false, err
	}

	if !found {
		n, err = d.createVPCNetwork(ctx, na, vpcID, cidr)
		if err != nil {
			return vpcNetwork{}, false, err
		}
	}

	d.nets.nets[vpcID] = n

	return n, true, nil
}

func (d *DockerCompute) findVPCNetwork(
	ctx context.Context, na networkAPI, vpcID, cidr string,
) (vpcNetwork, bool, error) {
	res, err := na.NetworkList(ctx, mobyclient.NetworkListOptions{Filters: vpcNetworkFilter(vpcID)})
	if err != nil {
		return vpcNetwork{}, false, fmt.Errorf("network list: %w", err)
	}

	for _, n := range res.Items {
		if n.Labels[labelManaged] != ec2BooleanTrue || n.Labels[labelVPC] != vpcID {
			continue
		}

		vn := vpcNetwork{name: n.Name, static: n.Labels[labelIPAM] == ipamVPCCIDR}
		if !vn.static {
			vn.note = mismatchNote(cidr, nil)
		}

		return vn, true, nil
	}

	return vpcNetwork{}, false, nil
}

func (d *DockerCompute) createVPCNetwork(
	ctx context.Context, na networkAPI, vpcID, cidr string,
) (vpcNetwork, error) {
	name := vpcNetworkName(vpcID)

	var cause error

	if prefix, err := netip.ParsePrefix(cidr); err == nil && prefix.Addr().Is4() {
		_, cause = na.NetworkCreate(ctx, name, mobyclient.NetworkCreateOptions{
			Driver: "bridge",
			IPAM:   &mobynetwork.IPAM{Config: []mobynetwork.IPAMConfig{{Subnet: prefix.Masked()}}},
			Labels: vpcNetworkLabels(vpcID, ipamVPCCIDR),
		})
		if cause == nil {
			return vpcNetwork{name: name, static: true}, nil
		}
	}

	note := mismatchNote(cidr, cause)
	logger.Load(ctx).WarnContext(ctx, "ec2 vpc network falling back to docker-assigned subnet",
		"vpc", vpcID, "cidr", cidr, "error", cause)

	if _, err := na.NetworkCreate(ctx, name, mobyclient.NetworkCreateOptions{
		Driver: "bridge",
		Labels: vpcNetworkLabels(vpcID, ipamAuto),
	}); err != nil {
		return vpcNetwork{}, fmt.Errorf("network create %s: %w", name, err)
	}

	return vpcNetwork{name: name, note: note}, nil
}

// RemoveVPCNetwork deletes the VPC's labelled Docker network; networks still in use are kept.
func (d *DockerCompute) RemoveVPCNetwork(ctx context.Context, vpcID string) error {
	na, ok := d.api.(networkAPI)
	if !ok || vpcID == "" {
		return nil
	}

	d.nets.mu.Lock()
	defer d.nets.mu.Unlock()

	delete(d.nets.nets, vpcID)

	return d.removeLabelled(ctx, na, vpcNetworkFilter(vpcID), vpcID)
}

// PruneIdleNetworks removes every labelled VPC network with no attached containers, returning the names removed.
func (d *DockerCompute) PruneIdleNetworks(ctx context.Context) []string {
	na, ok := d.api.(networkAPI)
	if !ok {
		return nil
	}

	d.nets.mu.Lock()
	defer d.nets.mu.Unlock()

	res, err := na.NetworkList(ctx, mobyclient.NetworkListOptions{Filters: vpcNetworkFilter("")})
	if err != nil {
		return nil
	}

	var removed []string

	for _, n := range res.Items {
		if n.Labels[labelManaged] != ec2BooleanTrue {
			continue
		}

		if _, rmErr := na.NetworkRemove(ctx, n.ID, mobyclient.NetworkRemoveOptions{}); rmErr == nil {
			removed = append(removed, n.Name)
			delete(d.nets.nets, n.Labels[labelVPC])
		}
	}

	sort.Strings(removed)

	return removed
}

func (d *DockerCompute) removeLabelled(
	ctx context.Context, na networkAPI, f mobyclient.Filters, vpcID string,
) error {
	res, err := na.NetworkList(ctx, mobyclient.NetworkListOptions{Filters: f})
	if err != nil {
		return fmt.Errorf("network list: %w", err)
	}

	var errs []string

	for _, n := range res.Items {
		if n.Labels[labelManaged] != ec2BooleanTrue || n.Labels[labelVPC] != vpcID {
			continue
		}

		if _, rmErr := na.NetworkRemove(ctx, n.ID, mobyclient.NetworkRemoveOptions{}); rmErr != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", n.Name, rmErr))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %s", errNetworkInUse, strings.Join(errs, "; "))
	}

	return nil
}
