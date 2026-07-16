package redshift

import (
	"fmt"
	"time"
)

// AssociateDataShareConsumer associates a consumer with a data share.
func (b *InMemoryBackend) AssociateDataShareConsumer(
	dataShareArn, consumerArn, consumerRegion string,
	_ bool,
) (*DataShare, error) {
	if dataShareArn == "" {
		return nil, fmt.Errorf("%w: DataShareArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateDataShareConsumer")
	defer b.mu.Unlock()

	ds, exists := b.dataShares.Get(dataShareArn)
	if !exists {
		return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
	}

	assoc := DataShareAssociation{
		ConsumerIdentifier: consumerArn,
		ConsumerRegion:     consumerRegion,
		CreatedDate:        time.Now(),
		StatusChangeDate:   time.Now(),
		Status:             dataShareStatusActive,
		Type:               "CONSUMER",
	}
	ds.DataShareAssociations = append(ds.DataShareAssociations, assoc)

	return cloneDataShare(ds), nil
}

// AuthorizeDataShare authorizes a data share to a consumer.
func (b *InMemoryBackend) AuthorizeDataShare(dataShareArn, consumerIdentifier string) (*DataShare, error) {
	if dataShareArn == "" {
		return nil, fmt.Errorf("%w: DataShareArn is required", ErrInvalidParameter)
	}
	if consumerIdentifier == "" {
		return nil, fmt.Errorf("%w: ConsumerIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("AuthorizeDataShare")
	defer b.mu.Unlock()

	ds, exists := b.dataShares.Get(dataShareArn)
	if !exists {
		return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
	}

	assoc := DataShareAssociation{
		ConsumerIdentifier: consumerIdentifier,
		CreatedDate:        time.Now(),
		StatusChangeDate:   time.Now(),
		Status:             dataShareStatusAuthorized,
		Type:               "CONSUMER",
	}
	ds.DataShareAssociations = append(ds.DataShareAssociations, assoc)

	return cloneDataShare(ds), nil
}

// DescribeDataShares returns data shares, optionally filtered by ARN.
func (b *InMemoryBackend) DescribeDataShares(dataShareArn string) ([]DataShare, error) {
	b.mu.RLock("DescribeDataShares")
	defer b.mu.RUnlock()

	if dataShareArn != "" {
		ds, exists := b.dataShares.Get(dataShareArn)
		if !exists {
			return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
		}

		return []DataShare{*cloneDataShare(ds)}, nil
	}

	result := make([]DataShare, 0, b.dataShares.Len())

	for _, ds := range b.dataShares.All() {
		result = append(result, *cloneDataShare(ds))
	}

	return result, nil
}

// DescribeDataSharesForConsumer returns data shares with a matching consumer association.
func (b *InMemoryBackend) DescribeDataSharesForConsumer(consumerArn, status string) ([]DataShare, error) {
	b.mu.RLock("DescribeDataSharesForConsumer")
	defer b.mu.RUnlock()

	var result []DataShare

	for _, ds := range b.dataShares.All() {
		for _, a := range ds.DataShareAssociations {
			if consumerArn != "" && a.ConsumerIdentifier != consumerArn {
				continue
			}

			if status != "" && a.Status != status {
				continue
			}

			result = append(result, *cloneDataShare(ds))

			break
		}
	}

	return result, nil
}

// DescribeDataSharesForProducer returns data shares matching a producer ARN.
func (b *InMemoryBackend) DescribeDataSharesForProducer(producerArn, status string) ([]DataShare, error) {
	b.mu.RLock("DescribeDataSharesForProducer")
	defer b.mu.RUnlock()

	var result []DataShare

	for _, ds := range b.dataShares.All() {
		if producerArn != "" && ds.ProducerArn != producerArn {
			continue
		}

		if status != "" {
			match := false

			for _, a := range ds.DataShareAssociations {
				if a.Status == status {
					match = true

					break
				}
			}

			if !match {
				continue
			}
		}

		result = append(result, *cloneDataShare(ds))
	}

	return result, nil
}

// DeauthorizeDataShare sets the association status to DEAUTHORIZED for the given consumer.
func (b *InMemoryBackend) DeauthorizeDataShare(dataShareArn, consumerIdentifier string) (*DataShare, error) {
	if dataShareArn == "" {
		return nil, fmt.Errorf("%w: DataShareArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeauthorizeDataShare")
	defer b.mu.Unlock()

	ds, exists := b.dataShares.Get(dataShareArn)
	if !exists {
		return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
	}

	for i, a := range ds.DataShareAssociations {
		if a.ConsumerIdentifier == consumerIdentifier {
			ds.DataShareAssociations[i].Status = "DEAUTHORIZED"
		}
	}

	return cloneDataShare(ds), nil
}

// DisassociateDataShareConsumer removes the consumer association from a data share.
func (b *InMemoryBackend) DisassociateDataShareConsumer(
	dataShareArn, consumerArn, _ string,
	_ bool,
) (*DataShare, error) {
	if dataShareArn == "" {
		return nil, fmt.Errorf("%w: DataShareArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateDataShareConsumer")
	defer b.mu.Unlock()

	ds, exists := b.dataShares.Get(dataShareArn)
	if !exists {
		return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
	}

	filtered := make([]DataShareAssociation, 0, len(ds.DataShareAssociations))

	for _, a := range ds.DataShareAssociations {
		if a.ConsumerIdentifier != consumerArn {
			filtered = append(filtered, a)
		}
	}

	ds.DataShareAssociations = filtered

	return cloneDataShare(ds), nil
}

// RejectDataShare marks all associations as REJECTED and disables public access.
func (b *InMemoryBackend) RejectDataShare(dataShareArn string) (*DataShare, error) {
	if dataShareArn == "" {
		return nil, fmt.Errorf("%w: DataShareArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("RejectDataShare")
	defer b.mu.Unlock()

	ds, exists := b.dataShares.Get(dataShareArn)
	if !exists {
		return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
	}

	ds.AllowPubliclyAccessibleConsumers = false

	for i := range ds.DataShareAssociations {
		ds.DataShareAssociations[i].Status = "REJECTED"
	}

	return cloneDataShare(ds), nil
}

// cloneDataShare returns a deep copy of a DataShare.
func cloneDataShare(ds *DataShare) *DataShare {
	cp := *ds
	cp.DataShareAssociations = make([]DataShareAssociation, len(ds.DataShareAssociations))
	copy(cp.DataShareAssociations, ds.DataShareAssociations)

	return &cp
}

// AddDataShareInternal seeds a data share directly into the backend.
func (b *InMemoryBackend) AddDataShareInternal(ds *DataShare) {
	b.mu.Lock("AddDataShareInternal")
	defer b.mu.Unlock()
	b.dataShares.Put(ds)
}
