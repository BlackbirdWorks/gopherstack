package network

import mobynetwork "github.com/moby/moby/api/types/network"

// NetworkingConfig aliases the Moby networking config type.
type NetworkingConfig = mobynetwork.NetworkingConfig

// Port aliases the Moby network port type.
type Port = mobynetwork.Port

// PortBinding aliases the Moby network port binding type.
type PortBinding = mobynetwork.PortBinding

// PortMap aliases the Moby network port map type.
type PortMap = mobynetwork.PortMap

// PortSet aliases the Moby network port set type.
type PortSet = mobynetwork.PortSet

// ParsePort parses a port string into a Moby network port value.
func ParsePort(value string) (Port, error) {
	return mobynetwork.ParsePort(value)
}
