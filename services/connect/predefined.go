package connect

import (
	"context"
	"slices"
)

const (
	maxPredefinedValues   = 128
	maxPredefinedValueLen = 64
	maxPredefinedPurposes = 10
)

// PredefinedAttributeValues is the union of value lists; only StringList exists.
type PredefinedAttributeValues struct {
	StringList []string `json:"StringList"`
}

// PredefinedAttributeConfiguration is the stored configuration.
type PredefinedAttributeConfiguration struct {
	EnableValueValidationOnAssociation bool `json:"EnableValueValidationOnAssociation"`
	IsReadOnly                         bool `json:"IsReadOnly"`
}

// PredefinedAttribute is a Connect predefined attribute.
type PredefinedAttribute struct {
	Stamp
	AttributeConfiguration *PredefinedAttributeConfiguration `json:"AttributeConfiguration,omitempty"`
	Values                 *PredefinedAttributeValues        `json:"Values,omitempty"`
	Name                   string                            `json:"Name"`
	Purposes               []string                          `json:"Purposes,omitempty"`
}

// PredefinedAttributeSummary is the list shape.
type PredefinedAttributeSummary struct {
	Stamp
	Name string `json:"Name"`
}

type predefinedRec struct {
	InstanceID string              `json:"instanceId"`
	Attribute  PredefinedAttribute `json:"attribute"`
}

type predefinedBody struct {
	AttributeConfiguration *struct {
		EnableValueValidationOnAssociation bool `json:"EnableValueValidationOnAssociation"`
	}
	Values   *PredefinedAttributeValues
	Purposes []string
}

// CreatePredefinedAttributeInput is the CreatePredefinedAttribute request.
type CreatePredefinedAttributeInput struct {
	Name string
	instPath
	predefinedBody
}

// UpdatePredefinedAttributeInput is the UpdatePredefinedAttribute request.
type UpdatePredefinedAttributeInput struct {
	Name string `json:"-" path:"Name"`
	instPath
	predefinedBody
}

func checkPredefinedBody(in *predefinedBody) error {
	if in.Values != nil {
		if n := len(in.Values.StringList); n == 0 || n > maxPredefinedValues {
			return invalidParamErr("Values.StringList must contain 1-%d values", maxPredefinedValues)
		}

		for _, v := range in.Values.StringList {
			if err := checkName("Values.StringList", v, maxPredefinedValueLen); err != nil {
				return err
			}
		}
	}

	if len(in.Purposes) > maxPredefinedPurposes {
		return invalidParamErr("Purposes must contain at most %d entries", maxPredefinedPurposes)
	}

	return nil
}

func applyPredefined(a *PredefinedAttribute, in *predefinedBody) {
	if in.Values != nil {
		a.Values = &PredefinedAttributeValues{StringList: slices.Clone(in.Values.StringList)}
	}

	if in.Purposes != nil {
		a.Purposes = slices.Clone(in.Purposes)
	}

	if in.AttributeConfiguration != nil {
		if a.AttributeConfiguration == nil {
			a.AttributeConfiguration = &PredefinedAttributeConfiguration{}
		}

		a.AttributeConfiguration.EnableValueValidationOnAssociation =
			in.AttributeConfiguration.EnableValueValidationOnAssociation
	}
}

// CreatePredefinedAttribute creates a predefined attribute.
func (b *Backend) CreatePredefinedAttribute(ctx context.Context, in *CreatePredefinedAttributeInput) (*empty, error) {
	if err := firstError(checkName("Name", in.Name, maxNameLen), checkPredefinedBody(&in.predefinedBody)); err != nil {
		return nil, err
	}

	if in.Values == nil {
		return nil, invalidParamErr("Values is required")
	}

	return writeInstance(b, "CreatePredefinedAttribute", in.InstanceID, func(inst string) (*empty, error) {
		if b.predefined.has(inst, in.Name) {
			return nil, duplicateErr("PredefinedAttribute", in.Name)
		}

		a := PredefinedAttribute{Stamp: b.stamp(ctx), Name: in.Name}
		applyPredefined(&a, &in.predefinedBody)
		b.predefined.put(&predefinedRec{InstanceID: inst, Attribute: a})

		return &empty{}, nil
	})
}

// DescribePredefinedAttribute returns a predefined attribute.
func (b *Backend) DescribePredefinedAttribute(inst, name string) (*PredefinedAttribute, error) {
	return readInstance(b, "DescribePredefinedAttribute", inst, func(inst string) (*PredefinedAttribute, error) {
		r, err := b.predefined.get(inst, name)
		if err != nil {
			return nil, err
		}

		out := r.Attribute

		return &out, nil
	})
}

// ListPredefinedAttributes pages through predefined attributes.
func (b *Backend) ListPredefinedAttributes(
	inst, token string,
	maxResults int32,
) ([]PredefinedAttributeSummary, string, error) {
	return listIn(b, "ListPredefinedAttributes", inst, b.predefined,
		func(r *predefinedRec) (PredefinedAttributeSummary, bool) {
			return PredefinedAttributeSummary{Stamp: r.Attribute.Stamp, Name: r.Attribute.Name}, true
		}, token, maxResults)
}

// UpdatePredefinedAttribute changes the supplied settings of a predefined attribute.
func (b *Backend) UpdatePredefinedAttribute(ctx context.Context, in *UpdatePredefinedAttributeInput) (*empty, error) {
	if err := checkPredefinedBody(&in.predefinedBody); err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdatePredefinedAttribute", in.InstanceID, func(inst string) (*empty, error) {
		r, err := b.predefined.get(inst, in.Name)
		if err != nil {
			return nil, err
		}

		applyPredefined(&r.Attribute, &in.predefinedBody)
		r.Attribute.Stamp = b.stamp(ctx)

		return &empty{}, nil
	})
}

// DeletePredefinedAttribute removes a predefined attribute.
func (b *Backend) DeletePredefinedAttribute(inst, name string) (*empty, error) {
	return writeInstance(b, "DeletePredefinedAttribute", inst, func(inst string) (*empty, error) {
		return nil, b.predefined.del(inst, name)
	})
}
