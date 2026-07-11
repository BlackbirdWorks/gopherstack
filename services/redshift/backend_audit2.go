package redshift

import (
	"fmt"
	"time"
)

// ---- Custom Domain Association ----

// CreateCustomDomainAssociation creates a custom domain name association for a cluster.
func (b *InMemoryBackend) CreateCustomDomainAssociation(
	clusterID, customDomainName, customDomainCertificateArn string,
) (*CustomDomainAssociation, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if customDomainName == "" {
		return nil, fmt.Errorf("%w: CustomDomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCustomDomainAssociation")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	key := clusterID + ":" + customDomainName
	if _, exists := b.customDomains.Get(key); exists {
		return nil, fmt.Errorf("%w: custom domain %s already associated with cluster %s",
			ErrCustomDomainAlreadyExists, customDomainName, clusterID)
	}

	assoc := &CustomDomainAssociation{
		ClusterIdentifier:          clusterID,
		CustomDomainName:           customDomainName,
		CustomDomainCertificateArn: customDomainCertificateArn,
	}
	b.customDomains.Put(assoc)

	cp := *assoc

	return &cp, nil
}

// DeleteCustomDomainAssociation removes the custom domain association for a cluster.
func (b *InMemoryBackend) DeleteCustomDomainAssociation(clusterID, customDomainName string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if customDomainName == "" {
		return fmt.Errorf("%w: CustomDomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteCustomDomainAssociation")
	defer b.mu.Unlock()

	key := clusterID + ":" + customDomainName
	if _, exists := b.customDomains.Get(key); !exists {
		return fmt.Errorf("%w: custom domain %s not associated with cluster %s",
			ErrCustomDomainNotFound, customDomainName, clusterID)
	}

	b.customDomains.Delete(key)

	return nil
}

// DescribeCustomDomainAssociations returns custom domain associations, optionally filtered.
func (b *InMemoryBackend) DescribeCustomDomainAssociations(
	clusterID, customDomainName string,
) ([]CustomDomainAssociation, error) {
	b.mu.RLock("DescribeCustomDomainAssociations")
	defer b.mu.RUnlock()

	if clusterID != "" && customDomainName != "" {
		key := clusterID + ":" + customDomainName
		a, exists := b.customDomains.Get(key)
		if !exists {
			return nil, fmt.Errorf("%w: custom domain %s not associated with cluster %s",
				ErrCustomDomainNotFound, customDomainName, clusterID)
		}

		cp := *a

		return []CustomDomainAssociation{cp}, nil
	}

	result := make([]CustomDomainAssociation, 0, b.customDomains.Len())

	for _, a := range b.customDomains.All() {
		if clusterID != "" && a.ClusterIdentifier != clusterID {
			continue
		}

		result = append(result, *a)
	}

	return result, nil
}

// ModifyCustomDomainAssociation updates the certificate ARN for an existing custom domain.
func (b *InMemoryBackend) ModifyCustomDomainAssociation(
	clusterID, customDomainName, customDomainCertificateArn string,
) (*CustomDomainAssociation, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if customDomainName == "" {
		return nil, fmt.Errorf("%w: CustomDomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyCustomDomainAssociation")
	defer b.mu.Unlock()

	key := clusterID + ":" + customDomainName
	a, exists := b.customDomains.Get(key)
	if !exists {
		return nil, fmt.Errorf("%w: custom domain %s not associated with cluster %s",
			ErrCustomDomainNotFound, customDomainName, clusterID)
	}

	a.CustomDomainCertificateArn = customDomainCertificateArn
	cp := *a

	return &cp, nil
}

// ---- Endpoint Access ----

// CreateEndpointAccess creates a new Redshift-managed VPC endpoint.
func (b *InMemoryBackend) CreateEndpointAccess(
	clusterID, endpointName, vpcID string,
) (*EndpointAccess, error) {
	if endpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", ErrInvalidParameter)
	}

	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateEndpointAccess")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	if _, exists := b.endpointAccesses.Get(endpointName); exists {
		return nil, fmt.Errorf("%w: endpoint %s already exists", ErrEndpointAccessAlreadyExists, endpointName)
	}

	ep := &EndpointAccess{
		ClusterIdentifier:  clusterID,
		EndpointName:       endpointName,
		EndpointStatus:     endpointStatusActive,
		EndpointCreateTime: time.Now().UTC().Format(time.RFC3339),
		Port:               redshiftDefaultPort,
		VpcID:              vpcID,
	}
	b.endpointAccesses.Put(ep)

	cp := *ep

	return &cp, nil
}

// DeleteEndpointAccess deletes the named endpoint access entry.
func (b *InMemoryBackend) DeleteEndpointAccess(endpointName string) (*EndpointAccess, error) {
	if endpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteEndpointAccess")
	defer b.mu.Unlock()

	ep, exists := b.endpointAccesses.Get(endpointName)
	if !exists {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrEndpointAccessNotFound, endpointName)
	}

	b.endpointAccesses.Delete(endpointName)

	cp := *ep
	cp.EndpointStatus = "deleting"

	return &cp, nil
}

// DescribeEndpointAccess returns endpoint accesses, optionally filtered by cluster and endpoint name.
func (b *InMemoryBackend) DescribeEndpointAccess(
	clusterID, endpointName string,
) ([]EndpointAccess, error) {
	b.mu.RLock("DescribeEndpointAccess")
	defer b.mu.RUnlock()

	if endpointName != "" {
		ep, exists := b.endpointAccesses.Get(endpointName)
		if !exists {
			return nil, fmt.Errorf("%w: endpoint %s not found", ErrEndpointAccessNotFound, endpointName)
		}

		cp := *ep

		return []EndpointAccess{cp}, nil
	}

	result := make([]EndpointAccess, 0, b.endpointAccesses.Len())

	for _, ep := range b.endpointAccesses.All() {
		if clusterID != "" && ep.ClusterIdentifier != clusterID {
			continue
		}

		result = append(result, *ep)
	}

	return result, nil
}

// ModifyEndpointAccess updates the VPC security groups for an endpoint.
func (b *InMemoryBackend) ModifyEndpointAccess(endpointName, vpcID string) (*EndpointAccess, error) {
	if endpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyEndpointAccess")
	defer b.mu.Unlock()

	ep, exists := b.endpointAccesses.Get(endpointName)
	if !exists {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrEndpointAccessNotFound, endpointName)
	}

	if vpcID != "" {
		ep.VpcID = vpcID
	}

	cp := *ep

	return &cp, nil
}

// ---- Integration ----

// CreateIntegration creates a new zero-ETL integration.
func (b *InMemoryBackend) CreateIntegration(
	integrationName, sourceArn, targetArn, kmsKeyID, description string,
) (*Integration, error) {
	if integrationName == "" {
		return nil, fmt.Errorf("%w: IntegrationName is required", ErrInvalidParameter)
	}

	if sourceArn == "" {
		return nil, fmt.Errorf("%w: SourceArn is required", ErrInvalidParameter)
	}

	if targetArn == "" {
		return nil, fmt.Errorf("%w: TargetArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIntegration")
	defer b.mu.Unlock()

	if _, exists := b.integrations.Get(integrationName); exists {
		return nil, fmt.Errorf("%w: integration %s already exists", ErrIntegrationAlreadyExists, integrationName)
	}

	arn := "arn:aws:redshift:" + b.region + ":" + b.accountID + ":integration/" + integrationName
	ig := &Integration{
		IntegrationArn:  arn,
		IntegrationName: integrationName,
		SourceArn:       sourceArn,
		TargetArn:       targetArn,
		KmsKeyID:        kmsKeyID,
		Description:     description,
		Status:          endpointStatusActive,
	}
	b.integrations.Put(ig)

	cp := *ig

	return &cp, nil
}

// DeleteIntegration deletes the named integration.
func (b *InMemoryBackend) DeleteIntegration(integrationArn string) (*Integration, error) {
	if integrationArn == "" {
		return nil, fmt.Errorf("%w: IntegrationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	for _, ig := range b.integrations.All() {
		if ig.IntegrationArn == integrationArn || ig.IntegrationName == integrationArn {
			cp := *ig
			b.integrations.Delete(ig.IntegrationName)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: integration %s not found", ErrIntegrationNotFound, integrationArn)
}

// DescribeIntegrations returns integrations, optionally filtered by ARN.
func (b *InMemoryBackend) DescribeIntegrations(integrationArn string) ([]Integration, error) {
	b.mu.RLock("DescribeIntegrations")
	defer b.mu.RUnlock()

	if integrationArn != "" {
		for _, ig := range b.integrations.All() {
			if ig.IntegrationArn == integrationArn {
				cp := *ig

				return []Integration{cp}, nil
			}
		}

		return nil, fmt.Errorf("%w: integration %s not found", ErrIntegrationNotFound, integrationArn)
	}

	result := make([]Integration, 0, b.integrations.Len())

	for _, ig := range b.integrations.All() {
		result = append(result, *ig)
	}

	return result, nil
}

// ModifyIntegration updates the description of an integration.
func (b *InMemoryBackend) ModifyIntegration(integrationArn, description string) (*Integration, error) {
	if integrationArn == "" {
		return nil, fmt.Errorf("%w: IntegrationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIntegration")
	defer b.mu.Unlock()

	for _, ig := range b.integrations.All() {
		if ig.IntegrationArn == integrationArn {
			ig.Description = description
			cp := *ig

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: integration %s not found", ErrIntegrationNotFound, integrationArn)
}

// ---- Redshift IDC Application ----

// CreateIdcApplication creates a new Redshift IDC application.
func (b *InMemoryBackend) CreateIdcApplication(
	appName, idcInstanceArn, idcDisplayName, iamRoleArn string,
) (*IdcApplication, error) {
	if appName == "" {
		return nil, fmt.Errorf("%w: IdcApplicationName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIdcApplication")
	defer b.mu.Unlock()

	if _, exists := b.idcApplications.Get(appName); exists {
		return nil, fmt.Errorf("%w: application %s already exists", ErrIdcApplicationAlreadyExists, appName)
	}

	arn := "arn:aws:redshift:" + b.region + ":" + b.accountID + ":redshiftidcapplication/" + appName
	app := &IdcApplication{
		IdcApplicationArn:  arn,
		IdcApplicationName: appName,
		IdcInstanceArn:     idcInstanceArn,
		IdcDisplayName:     idcDisplayName,
		IamRoleArn:         iamRoleArn,
	}
	b.idcApplications.Put(app)

	cp := *app

	return &cp, nil
}

// DeleteIdcApplication deletes the named IDC application.
func (b *InMemoryBackend) DeleteIdcApplication(appArn string) error {
	if appArn == "" {
		return fmt.Errorf("%w: IdcApplicationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIdcApplication")
	defer b.mu.Unlock()

	for _, app := range b.idcApplications.All() {
		if app.IdcApplicationArn == appArn || app.IdcApplicationName == appArn {
			b.idcApplications.Delete(app.IdcApplicationName)

			return nil
		}
	}

	return fmt.Errorf("%w: application %s not found", ErrIdcApplicationNotFound, appArn)
}

// DescribeIdcApplications returns IDC applications, optionally filtered by ARN.
func (b *InMemoryBackend) DescribeIdcApplications(appArn string) ([]IdcApplication, error) {
	b.mu.RLock("DescribeIdcApplications")
	defer b.mu.RUnlock()

	if appArn != "" {
		for _, app := range b.idcApplications.All() {
			if app.IdcApplicationArn == appArn {
				cp := *app

				return []IdcApplication{cp}, nil
			}
		}

		return nil, fmt.Errorf("%w: application %s not found", ErrIdcApplicationNotFound, appArn)
	}

	result := make([]IdcApplication, 0, b.idcApplications.Len())

	for _, app := range b.idcApplications.All() {
		result = append(result, *app)
	}

	return result, nil
}

// ModifyIdcApplication updates the display name and IAM role of an IDC application.
func (b *InMemoryBackend) ModifyIdcApplication(
	appArn, idcDisplayName, iamRoleArn string,
) (*IdcApplication, error) {
	if appArn == "" {
		return nil, fmt.Errorf("%w: IdcApplicationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIdcApplication")
	defer b.mu.Unlock()

	for _, app := range b.idcApplications.All() {
		if app.IdcApplicationArn == appArn {
			if idcDisplayName != "" {
				app.IdcDisplayName = idcDisplayName
			}

			if iamRoleArn != "" {
				app.IamRoleArn = iamRoleArn
			}

			cp := *app

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: application %s not found", ErrIdcApplicationNotFound, appArn)
}
