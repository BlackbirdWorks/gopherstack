package ec2

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
)

const (
	maxPublishedPorts = 32
	worldIPv4         = "0.0.0.0/0"
	worldIPv6         = "::/0"
	publishHostIP     = "127.0.0.1"
	sshPortNumber     = 22
	tagKeyPorts       = "gopherstack:published-ports"
	protoTCP          = "tcp"
	protoUDP          = "udp"
)

// worldOpenPorts lists the TCP/UDP ports open to the world in groups, excluding SSH and ranges wider than the cap.
func worldOpenPorts(groups []*SecurityGroup) []PortSpec {
	return collectPorts(groups, ruleWorldPorts)
}

// declaredTCPPorts lists every TCP port any ingress rule names, whatever its source, for relays that decide
// per connection; UDP cannot be relayed.
func declaredTCPPorts(groups []*SecurityGroup) []PortSpec {
	return collectPorts(groups, func(r SecurityGroupRule) []PortSpec {
		var out []PortSpec

		for _, s := range ruleSpecPorts(r) {
			if s.Protocol == protoTCP {
				out = append(out, s)
			}
		}

		return out
	})
}

func collectPorts(groups []*SecurityGroup, rulePorts func(SecurityGroupRule) []PortSpec) []PortSpec {
	seen := make(map[PortSpec]struct{})

	for _, sg := range groups {
		for _, r := range sg.IngressRules {
			for _, spec := range rulePorts(r) {
				seen[spec] = struct{}{}
			}
		}
	}

	var out []PortSpec
	for s := range seen {
		out = append(out, s)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Port != out[j].Port {
			return out[i].Port < out[j].Port
		}

		return out[i].Protocol < out[j].Protocol
	})

	if len(out) > maxPublishedPorts {
		out = out[:maxPublishedPorts]
	}

	return out
}

func ruleWorldPorts(r SecurityGroupRule) []PortSpec {
	if r.IPRange != worldIPv4 && r.IPv6Range != worldIPv6 {
		return nil
	}

	return ruleSpecPorts(r)
}

func ruleSpecPorts(r SecurityGroupRule) []PortSpec {
	proto := normalizePortProtocol(r.Protocol)
	if proto == "" || r.FromPort < 1 || r.ToPort < r.FromPort || r.ToPort-r.FromPort >= maxPublishedPorts {
		return nil
	}

	var out []PortSpec

	for p := r.FromPort; p <= min(r.ToPort, maxPortNumber); p++ {
		if p != sshPortNumber {
			out = append(out, PortSpec{Protocol: proto, Port: p})
		}
	}

	return out
}

const maxPortNumber = 65535

func normalizePortProtocol(p string) string {
	switch strings.ToLower(p) {
	case protoTCP, "6":
		return protoTCP
	case protoUDP, "17":
		return protoUDP
	default:
		return ""
	}
}

// ComputeNetworkContext returns the VPC CIDR and world-open ports for an instance about to launch.
func (b *InMemoryBackend) ComputeNetworkContext(inst *Instance) (string, []PortSpec) {
	b.mu.RLock("ComputeNetworkContext")
	defer b.mu.RUnlock()

	cidr := ""
	if vpc, ok := b.vpcs.Get(inst.VPCID); ok {
		cidr = vpc.CIDRBlock
	}

	var groups []*SecurityGroup

	for _, id := range inst.SecurityGroups {
		if sg, ok := b.securityGroups.Get(id); ok {
			groups = append(groups, sg)
		}
	}

	if b.sgEnforcer != nil {
		return cidr, declaredTCPPorts(groups)
	}

	return cidr, worldOpenPorts(groups)
}

func (p PortSpec) key() (mobynetwork.Port, error) {
	return mobynetwork.ParsePort(fmt.Sprintf("%d/%s", p.Port, p.Protocol))
}

// publishedFrom lists the host mappings in a container's port map, excluding sshd's.
func publishedFrom(pm mobynetwork.PortMap) []PublishedPort {
	var out []PublishedPort

	for port, bindings := range pm {
		if port == containerSSHPort {
			continue
		}

		for _, b := range bindings {
			hp, err := strconv.Atoi(b.HostPort)
			if err != nil || hp == 0 {
				continue
			}

			out = append(out, PublishedPort{
				Protocol:      string(port.Proto()),
				ContainerPort: int(port.Num()),
				HostIP:        hostIPString(b),
				HostPort:      hp,
			})

			break
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ContainerPort != out[j].ContainerPort {
			return out[i].ContainerPort < out[j].ContainerPort
		}

		return out[i].Protocol < out[j].Protocol
	})

	return out
}

func hostIPString(b mobynetwork.PortBinding) string {
	if b.HostIP.IsValid() {
		return b.HostIP.String()
	}

	return publishHostIP
}

func formatPublishedPorts(pp []PublishedPort) string {
	parts := make([]string, 0, len(pp))
	for _, p := range pp {
		parts = append(parts, fmt.Sprintf("%d/%s=%s:%d", p.ContainerPort, p.Protocol, p.HostIP, p.HostPort))
	}

	return strings.Join(parts, ",")
}

// PublishedPorts reports the host mappings of a running container's security-group ports.
func (d *DockerCompute) PublishedPorts(ctx context.Context, providerID string) ([]PublishedPort, error) {
	if f, ok := d.fronts.get(providerID); ok {
		return append([]PublishedPort(nil), f.ports...), nil
	}

	res, err := d.api.ContainerInspect(ctx, providerID, mobyclient.ContainerInspectOptions{})
	if err != nil {
		return nil, fmt.Errorf("container inspect: %w", err)
	}

	if res.Container.NetworkSettings == nil {
		return nil, nil
	}

	return publishedFrom(res.Container.NetworkSettings.Ports), nil
}
