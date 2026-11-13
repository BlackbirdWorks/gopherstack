package connect

import (
	"context"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	labelExtraction     = "ExtractionDefinition"
	notFoundUseDefault  = "USE_DEFAULT_VALUE"
	notFoundOmit        = "OMIT"
	maxExtractionPrompt = 4096
)

// ExtractionNotFoundBehavior is what an extraction does when the value is absent.
type ExtractionNotFoundBehavior struct {
	Behavior     string `json:"Behavior"`
	DefaultValue string `json:"DefaultValue,omitempty"`
}

// ExtractionConfiguration is the prompt hint and not-found behavior of a definition.
type ExtractionConfiguration struct {
	NotFoundBehavior *ExtractionNotFoundBehavior `json:"NotFoundBehavior,omitempty"`
	PromptHint       string                      `json:"PromptHint"`
}

// ExtractionDisplay holds the agent workspace label of a definition.
type ExtractionDisplay struct {
	Label string `json:"Label,omitempty"`
}

// ExtractionDefinition is a contact data extraction definition.
type ExtractionDefinition struct {
	CreatedTime             *awstime.EpochTime       `json:"CreatedTime"`
	LastUpdatedTime         *awstime.EpochTime       `json:"LastUpdatedTime"`
	ExtractionConfiguration *ExtractionConfiguration `json:"ExtractionConfiguration"`
	Display                 *ExtractionDisplay       `json:"Display,omitempty"`
	Tags                    map[string]string        `json:"Tags,omitempty"`
	ExtractionDefinitionArn string                   `json:"ExtractionDefinitionArn"`
	ExtractionDefinitionID  string                   `json:"ExtractionDefinitionId"`
	LastUpdatedBy           string                   `json:"LastUpdatedBy"`
	Name                    string                   `json:"Name"`
}

// ExtractionDefinitionSummary is the list shape of a definition.
type ExtractionDefinitionSummary struct {
	CreatedTime             *awstime.EpochTime `json:"CreatedTime"`
	LastUpdatedTime         *awstime.EpochTime `json:"LastUpdatedTime"`
	ExtractionDefinitionArn string             `json:"ExtractionDefinitionArn"`
	ExtractionDefinitionID  string             `json:"ExtractionDefinitionId"`
	LastUpdatedBy           string             `json:"LastUpdatedBy"`
	Name                    string             `json:"Name"`
}

type extractionRec struct {
	InstanceID string               `json:"instanceId"`
	Definition ExtractionDefinition `json:"definition"`
}

// CreateExtractionDefinitionInput is the CreateExtractionDefinition request.
type CreateExtractionDefinitionInput struct {
	ExtractionConfiguration *ExtractionConfiguration
	Display                 *ExtractionDisplay
	Tags                    map[string]string
	ClientToken             string
	Name                    string
	instPath
}

// UpdateExtractionDefinitionInput is the UpdateExtractionDefinition request.
type UpdateExtractionDefinitionInput struct {
	ExtractionConfiguration *ExtractionConfiguration
	Display                 *ExtractionDisplay
	ExtractionDefinitionID  string `json:"-" path:"ExtractionDefinitionId"`
	ClientToken             string
	Name                    string
	instPath
}

type extractionIDsOut struct {
	ExtractionDefinitionArn string `json:"ExtractionDefinitionArn"`
	ExtractionDefinitionID  string `json:"ExtractionDefinitionId"`
}

func checkExtractionConfig(c *ExtractionConfiguration) error {
	if c == nil || strings.TrimSpace(c.PromptHint) == "" {
		return invalidParamErr("ExtractionConfiguration.PromptHint is required")
	}

	if len(c.PromptHint) > maxExtractionPrompt {
		return invalidParamErr("PromptHint must be at most %d characters", maxExtractionPrompt)
	}

	if nf := c.NotFoundBehavior; nf != nil {
		return oneOf("NotFoundBehavior.Behavior", nf.Behavior, notFoundUseDefault, notFoundOmit)
	}

	return nil
}

func (b *Backend) touchExtraction(ctx context.Context, r *extractionRec) {
	t := awstime.NewEpochTime(b.now())
	r.Definition.LastUpdatedTime = &t
	r.Definition.LastUpdatedBy = b.callerARN(ctx)
}

func (r *extractionRec) view() ExtractionDefinition {
	out := r.Definition
	out.Tags = cloneTags(out.Tags)

	return out
}

// CreateExtractionDefinition creates a contact data extraction definition.
func (b *Backend) CreateExtractionDefinition(
	ctx context.Context, in *CreateExtractionDefinitionInput,
) (*extractionIDsOut, error) {
	if err := firstError(
		checkName("Name", in.Name, maxNameLen), checkExtractionConfig(in.ExtractionConfiguration), checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateExtractionDefinition", in.InstanceID, func(inst string) (*extractionIDsOut, error) {
		if nameExists(b.extractions, inst, func(r *extractionRec) string { return r.Definition.Name }, in.Name, "") {
			return nil, duplicateErr(labelExtraction, in.Name)
		}

		id := newID()
		created := awstime.NewEpochTime(b.now())
		rec := &extractionRec{InstanceID: inst, Definition: ExtractionDefinition{
			ExtractionDefinitionArn: b.buildARN(ctx, "instance/"+inst+"/extraction-definition/"+id),
			ExtractionDefinitionID:  id, Name: in.Name, CreatedTime: &created,
			ExtractionConfiguration: in.ExtractionConfiguration, Display: in.Display, Tags: cloneTags(in.Tags),
		}}
		b.touchExtraction(ctx, rec)
		b.extractions.put(rec)

		return &extractionIDsOut{
			ExtractionDefinitionArn: rec.Definition.ExtractionDefinitionArn, ExtractionDefinitionID: id,
		}, nil
	})
}

// DescribeExtractionDefinition returns an extraction definition.
func (b *Backend) DescribeExtractionDefinition(inst, id string) (*ExtractionDefinition, error) {
	return readInstance(b, "DescribeExtractionDefinition", inst, func(inst string) (*ExtractionDefinition, error) {
		r, err := b.extractions.get(inst, id)
		if err != nil {
			return nil, err
		}

		out := r.view()

		return &out, nil
	})
}

// ListExtractionDefinitions pages through the extraction definitions, sorted by name.
func (b *Backend) ListExtractionDefinitions(
	inst, token string, maxResults int32,
) ([]ExtractionDefinitionSummary, string, error) {
	all, err := readInstance(b, "ListExtractionDefinitions", inst,
		func(inst string) ([]ExtractionDefinitionSummary, error) {
			var out []ExtractionDefinitionSummary

			for _, r := range b.extractions.in(inst) {
				d := r.Definition
				out = append(out, ExtractionDefinitionSummary{
					CreatedTime:             d.CreatedTime,
					LastUpdatedTime:         d.LastUpdatedTime,
					ExtractionDefinitionArn: d.ExtractionDefinitionArn,
					ExtractionDefinitionID:  d.ExtractionDefinitionID,
					LastUpdatedBy:           d.LastUpdatedBy,
					Name:                    d.Name,
				})
			}

			return out, nil
		})
	if err != nil {
		return nil, "", err
	}

	slices.SortFunc(all, func(a, c ExtractionDefinitionSummary) int {
		return strings.Compare(strings.ToLower(a.Name), strings.ToLower(c.Name))
	})

	return paged(all, token, maxResults)
}

// UpdateExtractionDefinition replaces the name, configuration and display of a definition.
func (b *Backend) UpdateExtractionDefinition(
	ctx context.Context, in *UpdateExtractionDefinitionInput,
) (*empty, error) {
	if err := firstError(
		checkName("Name", in.Name, maxNameLen), checkExtractionConfig(in.ExtractionConfiguration),
	); err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdateExtractionDefinition", in.InstanceID, func(inst string) (*empty, error) {
		r, err := b.extractions.get(inst, in.ExtractionDefinitionID)
		if err != nil {
			return nil, err
		}

		if nameExists(b.extractions, inst, func(x *extractionRec) string { return x.Definition.Name },
			in.Name, r.Definition.ExtractionDefinitionID) {
			return nil, duplicateErr(labelExtraction, in.Name)
		}

		r.Definition.Name = in.Name
		r.Definition.ExtractionConfiguration = in.ExtractionConfiguration

		if in.Display != nil {
			r.Definition.Display = in.Display
		}

		b.touchExtraction(ctx, r)

		return &empty{}, nil
	})
}

// DeleteExtractionDefinition removes an extraction definition.
func (b *Backend) DeleteExtractionDefinition(inst, id string) (*empty, error) {
	return writeInstance(b, "DeleteExtractionDefinition", inst, func(inst string) (*empty, error) {
		return nil, b.extractions.del(inst, id)
	})
}
