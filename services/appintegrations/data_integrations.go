package appintegrations

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"
)

const (
	execOnDemand  = "ON_DEMAND"
	execScheduled = "SCHEDULED"
)

// CreateDataIntegrationInput is the CreateDataIntegration request body.
type CreateDataIntegrationInput struct {
	Tags                map[string]string
	ObjectConfiguration map[string]map[string][]string
	FileConfiguration   *FileConfiguration
	ScheduleConfig      *ScheduleConfiguration
	Name                string
	Description         string
	KmsKey              string
	SourceURI           string
	ClientToken         string
}

func cloneData(d *DataIntegration) *DataIntegration {
	cp := *d
	cp.Tags = maps.Clone(d.Tags)

	return &cp
}

func checkSchedule(s *ScheduleConfiguration) error {
	if s == nil {
		return nil
	}

	if !strings.HasPrefix(s.ScheduleExpression, "rate(") && !strings.HasPrefix(s.ScheduleExpression, "cron(") {
		return invalidErr("ScheduleExpression must be a rate(...) or cron(...) expression")
	}

	return nil
}

// CreateDataIntegration registers a data integration.
func (b *InMemoryBackend) CreateDataIntegration(
	ctx context.Context, in *CreateDataIntegrationInput,
) (*DataIntegration, error) {
	if err := checkName("Name", in.Name); err != nil {
		return nil, err
	}

	if in.KmsKey == "" || len(in.KmsKey) > maxNameLen {
		return nil, invalidErr("KmsKey must be 1-%d characters", maxNameLen)
	}

	if in.SourceURI != "" && !strings.Contains(in.SourceURI, "://") {
		return nil, invalidErr("SourceURI must be a URI such as salesforce://my-source")
	}

	if err := checkSchedule(in.ScheduleConfig); err != nil {
		return nil, err
	}

	if err := checkDescription(in.Description); err != nil {
		return nil, err
	}

	if err := checkTags(in.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDataIntegration")
	defer b.mu.Unlock()

	for _, d := range b.dataIntegs.All() {
		if d.Name == in.Name {
			return nil, duplicateErr("DataIntegration", in.Name)
		}
	}

	id := uuid.NewString()
	d := &DataIntegration{
		ObjectConfiguration:   in.ObjectConfiguration,
		Tags:                  maps.Clone(in.Tags),
		FileConfiguration:     in.FileConfiguration,
		ScheduleConfiguration: in.ScheduleConfig,
		Arn:                   b.buildARN(ctx, "data-integration/"+id),
		ID:                    id,
		Name:                  in.Name,
		Description:           in.Description,
		KmsKey:                in.KmsKey,
		SourceURI:             in.SourceURI,
	}
	b.dataIntegs.Put(d)

	return cloneData(d), nil
}

func (b *InMemoryBackend) dataLocked(identifier string) (*DataIntegration, error) {
	if d, ok := b.dataIntegs.Get(identifier); ok {
		return d, nil
	}

	for _, d := range b.dataIntegs.All() {
		if d.Arn == identifier || d.Name == identifier {
			return d, nil
		}
	}

	return nil, notFoundErr("DataIntegration", identifier)
}

// GetDataIntegration resolves an ID, ARN or name.
func (b *InMemoryBackend) GetDataIntegration(identifier string) (*DataIntegration, error) {
	b.mu.RLock("GetDataIntegration")
	defer b.mu.RUnlock()

	d, err := b.dataLocked(identifier)
	if err != nil {
		return nil, err
	}

	return cloneData(d), nil
}

// ListDataIntegrations pages through data integrations ordered by name.
func (b *InMemoryBackend) ListDataIntegrations(
	token string,
	maxResults int32,
) ([]DataIntegrationSummary, string, error) {
	b.mu.RLock("ListDataIntegrations")
	all := make([]DataIntegrationSummary, 0, b.dataIntegs.Len())
	b.dataIntegs.Range(func(d *DataIntegration) bool {
		all = append(all, DataIntegrationSummary{Arn: d.Arn, Name: d.Name, SourceURI: d.SourceURI})

		return true
	})
	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c DataIntegrationSummary) int { return strings.Compare(a.Name, c.Name) })

	return page(all, token, maxResults)
}

// UpdateDataIntegration changes the name and description of a data integration.
func (b *InMemoryBackend) UpdateDataIntegration(identifier string, name, description *string) error {
	if name != nil {
		if err := checkName("Name", *name); err != nil {
			return err
		}
	}

	if description != nil {
		if err := checkDescription(*description); err != nil {
			return err
		}
	}

	b.mu.Lock("UpdateDataIntegration")
	defer b.mu.Unlock()

	d, err := b.dataLocked(identifier)
	if err != nil {
		return err
	}

	if name != nil && *name != d.Name {
		for _, o := range b.dataIntegs.All() {
			if o.Name == *name {
				return duplicateErr("DataIntegration", *name)
			}
		}

		d.Name = *name
	}

	if description != nil {
		d.Description = *description
	}

	return nil
}

// DeleteDataIntegration removes a data integration that has no associations.
func (b *InMemoryBackend) DeleteDataIntegration(identifier string) error {
	b.mu.Lock("DeleteDataIntegration")
	defer b.mu.Unlock()

	d, err := b.dataLocked(identifier)
	if err != nil {
		return err
	}

	for _, a := range b.dataAssocs.All() {
		if a.DataIntegrationID == d.ID {
			return invalidErr("DataIntegration %q still has associations", identifier)
		}
	}

	b.dataIntegs.Delete(d.ID)

	return nil
}

// CreateDataIntegrationAssociationInput is the CreateDataIntegrationAssociation request.
type CreateDataIntegrationAssociationInput struct {
	ClientAssociationMetadata map[string]string
	ObjectConfiguration       map[string]map[string][]string
	ExecutionConfiguration    *ExecutionConfiguration
	Identifier                string `json:"-"        path:"DataIntegrationIdentifier"`
	ClientID                  string `json:"ClientId"`
	DestinationURI            string
	ClientToken               string
}

func checkExecution(c *ExecutionConfiguration) error {
	if c == nil {
		return nil
	}

	switch c.ExecutionMode {
	case execOnDemand:
		if c.OnDemandConfiguration == nil || c.OnDemandConfiguration.StartTime == "" {
			return invalidErr("OnDemandConfiguration.StartTime is required for ON_DEMAND execution")
		}
	case execScheduled:
		if c.ScheduleConfiguration == nil {
			return invalidErr("ScheduleConfiguration is required for SCHEDULED execution")
		}

		return checkSchedule(c.ScheduleConfiguration)
	default:
		return invalidErr("ExecutionMode must be ON_DEMAND or SCHEDULED")
	}

	return nil
}

// CreateDataIntegrationAssociation binds a data integration to a client; a data integration takes one.
func (b *InMemoryBackend) CreateDataIntegrationAssociation(
	ctx context.Context, in *CreateDataIntegrationAssociationInput,
) (*DataIntegrationAssociation, error) {
	if err := checkExecution(in.ExecutionConfiguration); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDataIntegrationAssociation")
	defer b.mu.Unlock()

	d, err := b.dataLocked(in.Identifier)
	if err != nil {
		return nil, err
	}

	for _, a := range b.dataAssocs.All() {
		if a.DataIntegrationID == d.ID {
			return nil, invalidErr("DataIntegration %q is already associated", in.Identifier)
		}
	}

	id := uuid.NewString()
	a := &DataIntegrationAssociation{
		ClientAssociationMetadata:     maps.Clone(in.ClientAssociationMetadata),
		ObjectConfiguration:           in.ObjectConfiguration,
		ExecutionConfiguration:        in.ExecutionConfiguration,
		ClientID:                      in.ClientID,
		DataIntegrationArn:            d.Arn,
		DataIntegrationAssociationArn: b.buildARN(ctx, "data-integration-association/"+d.ID+"/"+id),
		DataIntegrationAssociationID:  id,
		DataIntegrationID:             d.ID,
		DestinationURI:                in.DestinationURI,
	}
	b.dataAssocs.Put(a)

	cp := *a

	return &cp, nil
}

// UpdateDataIntegrationAssociation replaces the execution configuration of an association.
func (b *InMemoryBackend) UpdateDataIntegrationAssociation(
	identifier, associationIdentifier string, cfg *ExecutionConfiguration,
) error {
	if cfg == nil {
		return invalidErr("ExecutionConfiguration is required")
	}

	if err := checkExecution(cfg); err != nil {
		return err
	}

	b.mu.Lock("UpdateDataIntegrationAssociation")
	defer b.mu.Unlock()

	a, err := b.dataAssocLocked(identifier, associationIdentifier)
	if err != nil {
		return err
	}

	a.ExecutionConfiguration = cfg

	return nil
}

func (b *InMemoryBackend) dataAssocLocked(identifier, assoc string) (*DataIntegrationAssociation, error) {
	d, err := b.dataLocked(identifier)
	if err != nil {
		return nil, err
	}

	for _, a := range b.dataAssocs.All() {
		if a.DataIntegrationID == d.ID &&
			(a.DataIntegrationAssociationID == assoc || a.DataIntegrationAssociationArn == assoc) {
			return a, nil
		}
	}

	return nil, notFoundErr("DataIntegrationAssociation", assoc)
}

// ListDataIntegrationAssociations pages through the associations of one data integration.
func (b *InMemoryBackend) ListDataIntegrationAssociations(
	identifier, token string, maxResults int32,
) ([]DataIntegrationAssociationSummary, string, error) {
	b.mu.RLock("ListDataIntegrationAssociations")

	d, err := b.dataLocked(identifier)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []DataIntegrationAssociationSummary

	for _, a := range b.dataAssocs.All() {
		if a.DataIntegrationID == d.ID {
			all = append(all, DataIntegrationAssociationSummary{
				ExecutionConfiguration:        a.ExecutionConfiguration,
				ClientID:                      a.ClientID,
				DataIntegrationArn:            a.DataIntegrationArn,
				DataIntegrationAssociationArn: a.DataIntegrationAssociationArn,
				DestinationURI:                a.DestinationURI,
			})
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c DataIntegrationAssociationSummary) int {
		return strings.Compare(a.DataIntegrationAssociationArn, c.DataIntegrationAssociationArn)
	})

	return page(all, token, maxResults)
}

func (b *InMemoryBackend) getDataAssociation(identifier, id string) (*DataIntegrationAssociation, error) {
	b.mu.RLock("getDataAssociation")
	defer b.mu.RUnlock()

	a, err := b.dataAssocLocked(identifier, id)
	if err != nil {
		return nil, err
	}

	cp := *a

	return &cp, nil
}
