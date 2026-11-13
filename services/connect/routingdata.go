package connect

import (
	"encoding/json/jsontext"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	stepActive   = "ACTIVE"
	stepInactive = "INACTIVE"
)

// RoutingCriteria is the routing criteria of a contact (types.RoutingCriteria).
type RoutingCriteria struct {
	ActivationTimestamp *awstime.EpochTime `json:"ActivationTimestamp,omitempty"`
	Steps               []RoutingStep      `json:"Steps"`
	Index               int32              `json:"Index"`
}

// RoutingStep is one step of a contact's routing criteria.
type RoutingStep struct {
	Expiry     *RoutingExpiry `json:"Expiry,omitempty"`
	Status     string         `json:"Status"`
	Expression jsontext.Value `json:"Expression,omitempty"`
}

// RoutingExpiry is the expiry of a routing step.
type RoutingExpiry struct {
	ExpiryTimestamp   *awstime.EpochTime `json:"ExpiryTimestamp,omitempty"`
	DurationInSeconds int32              `json:"DurationInSeconds"`
}

// UpdateContactRoutingDataInput is the UpdateContactRoutingData request.
type UpdateContactRoutingDataInput struct {
	QueuePriority              *int64
	QueueTimeAdjustmentSeconds *int32
	RoutingCriteria            *struct {
		Steps []struct {
			Expiry *struct {
				DurationInSeconds *int32
			}
			Expression jsontext.Value
		}
	}
	ContactID  string `json:"-" path:"ContactId"`
	InstanceID string `json:"-" path:"InstanceId"`
}

func validateRoutingData(in *UpdateContactRoutingDataInput) error {
	if in.QueuePriority != nil && in.QueueTimeAdjustmentSeconds != nil {
		return invalidParamErr("QueuePriority and QueueTimeAdjustmentSeconds cannot both be provided")
	}

	if in.QueuePriority == nil && in.QueueTimeAdjustmentSeconds == nil && in.RoutingCriteria == nil {
		return invalidParamErr("one of QueuePriority, QueueTimeAdjustmentSeconds or RoutingCriteria is required")
	}

	if in.QueuePriority != nil && *in.QueuePriority < 1 {
		return invalidParamErr("QueuePriority must be at least 1")
	}

	if in.RoutingCriteria == nil {
		return nil
	}

	for i, s := range in.RoutingCriteria.Steps {
		if len(s.Expression) > 0 && !s.Expression.IsValid(jsonx.V1()) {
			return invalidParamErr("Steps[%d].Expression is not valid", i)
		}

		if s.Expiry != nil && s.Expiry.DurationInSeconds != nil && *s.Expiry.DurationInSeconds < 0 {
			return invalidParamErr("Steps[%d].Expiry.DurationInSeconds must not be negative", i)
		}
	}

	return nil
}

func buildRoutingCriteria(in *UpdateContactRoutingDataInput, now awstime.EpochTime) *RoutingCriteria {
	rc := &RoutingCriteria{ActivationTimestamp: &now, Steps: make([]RoutingStep, 0, len(in.RoutingCriteria.Steps))}

	for i, s := range in.RoutingCriteria.Steps {
		step := RoutingStep{Expression: s.Expression, Status: stepInactive}
		if i == 0 {
			step.Status = stepActive
		}

		if s.Expiry != nil && s.Expiry.DurationInSeconds != nil {
			step.Expiry = &RoutingExpiry{DurationInSeconds: *s.Expiry.DurationInSeconds}
			if i == 0 {
				t := awstime.NewEpochTime(now.Add(time.Duration(*s.Expiry.DurationInSeconds) * time.Second))
				step.Expiry.ExpiryTimestamp = &t
			}
		}

		rc.Steps = append(rc.Steps, step)
	}

	return rc
}

// UpdateContactRoutingData sets the queue priority, queue time adjustment or routing criteria of a contact.
func (b *Backend) UpdateContactRoutingData(in *UpdateContactRoutingDataInput) (*empty, error) {
	if err := validateRoutingData(in); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateContactRoutingData")
	defer b.mu.Unlock()

	c, err := b.resourceContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if in.QueuePriority != nil {
		c.Contact.QueuePriority = in.QueuePriority
	}

	if in.QueueTimeAdjustmentSeconds != nil {
		c.Contact.QueueTimeAdjustmentSeconds = in.QueueTimeAdjustmentSeconds
	}

	if in.RoutingCriteria != nil {
		c.Contact.RoutingCriteria = buildRoutingCriteria(in, awstime.NewEpochTime(b.now()))
	}

	b.touch(c)

	return &empty{}, nil
}
