package opsworks

// RegisterElasticIP registers an elastic IP address with a stack. ElasticIp
// and StackId are both "This member is required" on the real
// RegisterElasticIpInput; there is no Region member on the real input at all
// (confirmed against aws-sdk-go-v2/service/opsworks@v1.31.0's
// api_op_RegisterElasticIp.go).
func (b *InMemoryBackend) RegisterElasticIP(elasticIP, stackID string) (*ElasticIP, error) {
	if elasticIP == "" || stackID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("RegisterElasticIP")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return nil, ErrStackNotFound
	}

	e := &storedElasticIP{
		IP:      elasticIP,
		Region:  b.region,
		Domain:  "vpc",
		StackID: stackID,
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

// DescribeElasticIps returns elastic IPs optionally filtered by stack,
// instance, or IP list. StackId is a real DescribeElasticIpsInput filter
// member (confirmed against aws-sdk-go-v2/service/opsworks@v1.31.0's
// api_op_DescribeElasticIps.go), alongside InstanceId and Ips.
func (b *InMemoryBackend) DescribeElasticIps(stackID, instanceID string, ips []string) ([]*ElasticIP, error) {
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
		if stackID != "" && e.StackID != stackID {
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
