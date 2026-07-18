package opsworks

// RegisterElasticIP registers an elastic IP address.
func (b *InMemoryBackend) RegisterElasticIP(elasticIP, region string) (*ElasticIP, error) {
	if elasticIP == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("RegisterElasticIP")
	defer b.mu.Unlock()

	r := region
	if r == "" {
		r = b.region
	}

	e := &storedElasticIP{
		IP:     elasticIP,
		Region: r,
		Domain: "vpc",
	}
	b.elasticIPs.Put(e)

	return e.toElasticIP(), nil
}

// DeregisterElasticIP removes a registered elastic IP.
func (b *InMemoryBackend) DeregisterElasticIP(elasticIP string) error {
	b.mu.Lock("DeregisterElasticIP")
	defer b.mu.Unlock()

	if !b.elasticIPs.Delete(elasticIP) {
		return ErrElasticIPNotFound
	}

	return nil
}

// AssociateElasticIP associates an elastic IP with an instance.
func (b *InMemoryBackend) AssociateElasticIP(elasticIP, instanceID string) error {
	b.mu.Lock("AssociateElasticIP")
	defer b.mu.Unlock()

	e, ok := b.elasticIPs.Get(elasticIP)
	if !ok {
		return ErrElasticIPNotFound
	}

	if !b.instances.Has(instanceID) {
		return ErrInstanceNotFound
	}

	e.InstanceID = instanceID

	return nil
}

// DisassociateElasticIP removes an elastic IP's instance association.
func (b *InMemoryBackend) DisassociateElasticIP(elasticIP string) error {
	b.mu.Lock("DisassociateElasticIP")
	defer b.mu.Unlock()

	e, ok := b.elasticIPs.Get(elasticIP)
	if !ok {
		return ErrElasticIPNotFound
	}

	e.InstanceID = ""

	return nil
}

// DescribeElasticIps returns elastic IPs optionally filtered by instance or IP list.
func (b *InMemoryBackend) DescribeElasticIps(instanceID string, ips []string) ([]*ElasticIP, error) {
	b.mu.RLock("DescribeElasticIps")
	defer b.mu.RUnlock()

	if len(ips) > 0 {
		result := make([]*ElasticIP, 0, len(ips))
		for _, ip := range ips {
			e, ok := b.elasticIPs.Get(ip)
			if !ok {
				return nil, ErrElasticIPNotFound
			}
			result = append(result, e.toElasticIP())
		}

		return result, nil
	}

	result := make([]*ElasticIP, 0)
	for _, e := range b.elasticIPs.All() {
		if instanceID != "" && e.InstanceID != instanceID {
			continue
		}
		result = append(result, e.toElasticIP())
	}

	return result, nil
}

// UpdateElasticIP updates the name of a registered elastic IP.
func (b *InMemoryBackend) UpdateElasticIP(elasticIP, name string) error {
	b.mu.Lock("UpdateElasticIP")
	defer b.mu.Unlock()

	e, ok := b.elasticIPs.Get(elasticIP)
	if !ok {
		return ErrElasticIPNotFound
	}

	e.Name = name

	return nil
}
