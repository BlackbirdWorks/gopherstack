package ec2

import "fmt"

// ExportKeyPair returns the public-key material for a stored key pair.
// For keys created via CreateKeyPair a deterministic SSH-authorized-keys line
// is generated from the fingerprint. AWS similarly stores only the public half.
func (b *InMemoryBackend) ExportKeyPair(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	b.mu.RLock("ExportKeyPair")
	defer b.mu.RUnlock()

	kp, ok := b.keyPairs.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: %s", errR3KeyPairNotFound, name)
	}

	return "ssh-rsa AAAAB3NzaC1yc2EAAAADAQ... " + kp.Fingerprint + " exported@gopherstack", nil
}

// ---- Instance type offerings ----

// InstanceTypeOffering pairs an instance type with an AZ offering.
type InstanceTypeOffering struct {
	InstanceType     string `json:"instanceType,omitempty"`
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	Location         string `json:"location,omitempty"`
	LocationType     string `json:"locationType,omitempty"`
}
