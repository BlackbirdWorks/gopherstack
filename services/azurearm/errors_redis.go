package azurearm

import "errors"

// Sentinel errors of the Microsoft.Cache provider.
var (
	ErrRedisNotFound         = errors.New("azurearm: redis cache not found")
	ErrRedisFirewallNotFound = errors.New("azurearm: redis firewall rule not found")
	ErrRedisPatchNotFound    = errors.New("azurearm: redis patch schedule not found")
	ErrRedisLinkedNotFound   = errors.New("azurearm: redis linked server not found")
	ErrRedisNameTaken        = errors.New("azurearm: redis cache name is already in use")
	ErrRedisInvalid          = errors.New("azurearm: invalid redis request")
	ErrRedisUnavailable      = errors.New("azurearm: redis data plane is not available")
)
