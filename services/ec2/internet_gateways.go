package ec2

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ErrInternetGatewayNotFound is returned when an Internet Gateway is not found.
var ErrInternetGatewayNotFound = errors.New("InvalidInternetGatewayID.NotFound")

// IGWAttachment represents the attachment of an Internet Gateway to a VPC.
type IGWAttachment struct {
	VPCID string `json:"vpcID,omitempty"`
	State string `json:"state,omitempty"`
}

// InternetGateway represents an EC2 Internet Gateway.
type InternetGateway struct {
	ID          string          `json:"id,omitempty"`
	Attachments []IGWAttachment `json:"attachments,omitempty"`
}

// CreateInternetGateway creates a new Internet Gateway.
func (b *InMemoryBackend) CreateInternetGateway() (*InternetGateway, error) {
	b.mu.Lock("CreateInternetGateway")
	defer b.mu.Unlock()

	id := "igw-" + uuid.New().String()[:17]
	igw := &InternetGateway{
		ID:          id,
		Attachments: []IGWAttachment{},
	}
	b.internetGateways.Put(igw)

	return igw, nil
}

// DeleteInternetGateway removes an Internet Gateway.
func (b *InMemoryBackend) DeleteInternetGateway(id string) error {
	b.mu.Lock("DeleteInternetGateway")
	defer b.mu.Unlock()

	if _, ok := b.internetGateways.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrInternetGatewayNotFound, id)
	}
	b.internetGateways.Delete(id)
	delete(b.tags, id)

	return nil
}

// DescribeInternetGateways returns IGWs, optionally filtered by IDs.
// When ids are provided, lookups are O(len(ids)) via the IGW map rather than
// scanning every IGW in the backend.
func (b *InMemoryBackend) DescribeInternetGateways(ids []string) []*InternetGateway {
	b.mu.RLock("DescribeInternetGateways")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		out := make([]*InternetGateway, 0, len(ids))

		for _, id := range ids {
			igw, ok := b.internetGateways.Get(id)
			if !ok {
				continue
			}

			cp := *igw
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*InternetGateway, 0, b.internetGateways.Len())

	for _, igw := range b.internetGateways.All() {
		cp := *igw
		out = append(out, &cp)
	}

	return out
}

// AttachInternetGateway attaches an IGW to a VPC.
func (b *InMemoryBackend) AttachInternetGateway(igwID, vpcID string) error {
	b.mu.Lock("AttachInternetGateway")
	defer b.mu.Unlock()

	igw, ok := b.internetGateways.Get(igwID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInternetGatewayNotFound, igwID)
	}

	if _, vpcOK := b.vpcs.Get(vpcID); !vpcOK {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	igw.Attachments = append(igw.Attachments, IGWAttachment{VPCID: vpcID, State: stateAvailable})

	return nil
}

// DetachInternetGateway detaches an IGW from a VPC.
func (b *InMemoryBackend) DetachInternetGateway(igwID, vpcID string) error {
	b.mu.Lock("DetachInternetGateway")
	defer b.mu.Unlock()

	igw, ok := b.internetGateways.Get(igwID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInternetGatewayNotFound, igwID)
	}

	for i, att := range igw.Attachments {
		if att.VPCID == vpcID {
			igw.Attachments = append(igw.Attachments[:i], igw.Attachments[i+1:]...)

			return nil
		}
	}

	return fmt.Errorf("%w: IGW %s is not attached to VPC %s", ErrInvalidParameter, igwID, vpcID)
}
