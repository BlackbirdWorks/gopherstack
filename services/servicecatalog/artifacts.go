package servicecatalog

import (
	"context"
	"slices"
	"strings"
)

const (
	infoTemplateURL  = "LoadTemplateFromURL"
	infoPhysicalID   = "ImportFromPhysicalId"
	infoTemplateKeyR = "TemplateUrl"
)

func registerArtifactOps(m map[string]opFunc, b *InMemoryBackend) {
	m["CreateProvisioningArtifact"] = op(b.CreateProvisioningArtifact)
	m["UpdateProvisioningArtifact"] = op(b.UpdateProvisioningArtifact)
	m["DeleteProvisioningArtifact"] = op(b.DeleteProvisioningArtifact)
	m["DescribeProvisioningArtifact"] = op(b.DescribeProvisioningArtifact)
	m["ListProvisioningArtifacts"] = op(b.ListProvisioningArtifacts)
	m["DescribeProvisioningParameters"] = op(b.DescribeProvisioningParameters)
}

// artifactTypeFor is the one provisioning artifact type a product type accepts.
func artifactTypeFor(productType string) string {
	switch productType {
	case productTypeTFOpen:
		return productTypeTFOpen
	case productTypeTFCloud:
		return productTypeTFCloud
	case productTypeExternal:
		return productTypeExternal
	default:
		return artifactTypeCFN
	}
}

func (b *InMemoryBackend) engineOrNil() StackEngine {
	b.mu.RLock("engine")
	defer b.mu.RUnlock()

	return b.engine
}

// newArtifact validates version properties for a product type and loads its template when it must be checked.
func (b *InMemoryBackend) newArtifact(
	ctx context.Context,
	productType string,
	props *ProvisioningArtifactProperties,
) (*Artifact, error) {
	typ := props.Type
	if typ == "" {
		typ = artifactTypeFor(productType)
	}

	if typ != artifactTypeFor(productType) {
		return nil, invalid("a %s product cannot have a %s provisioning artifact", productType, typ)
	}

	if len(props.Name) > 100 || len(props.Description) > 8192 {
		return nil, invalid("Name must be at most 100 and Description at most 8192 characters")
	}

	art := &Artifact{
		Name: props.Name, Description: props.Description, Type: typ, Guidance: "DEFAULT", Info: props.Info,
		DisableTemplateValidation: props.DisableTemplateValidation,
	}

	if typ != artifactTypeCFN {
		return art, nil
	}

	url, phys := props.Info[infoTemplateURL], props.Info[infoPhysicalID]

	if (url == "") == (phys == "") {
		return nil, invalid("Info must contain exactly one of %s and %s", infoTemplateURL, infoPhysicalID)
	}

	if !props.DisableTemplateValidation {
		if err := b.validateTemplateOf(ctx, art); err != nil {
			return nil, err
		}
	}

	return art, nil
}

func (b *InMemoryBackend) validateTemplateOf(ctx context.Context, art *Artifact) error {
	if b.engineOrNil() == nil {
		return nil
	}

	_, _, err := b.templateInfoOf(ctx, art)

	return err
}

// templateInfoOf loads an artifact's template and inspects it. It must be called without the lock held.
func (b *InMemoryBackend) templateInfoOf(ctx context.Context, art *Artifact) (string, TemplateInfo, error) {
	eng := b.engineOrNil()
	if eng == nil {
		return "", TemplateInfo{}, fail(ErrOperationNotSupported, "%v", errStackEngineUnavailable)
	}

	var (
		body string
		err  error
	)

	if url := art.Info[infoTemplateURL]; url != "" {
		body, err = eng.FetchTemplate(ctx, url)
	} else {
		body, err = eng.StackTemplate(ctx, art.Info[infoPhysicalID])
	}

	if err != nil {
		return "", TemplateInfo{}, invalid("the template could not be loaded: %v", err)
	}

	info, err := eng.InspectTemplate(ctx, body)
	if err != nil {
		return "", TemplateInfo{}, invalid("the template is not valid: %v", err)
	}

	return body, info, nil
}

func artifactInfoOut(a *Artifact) map[string]string {
	url := a.Info[infoTemplateURL]
	if url == "" {
		return map[string]string{}
	}

	return map[string]string{infoTemplateKeyR: url}
}

// CreateProvisioningArtifactInput is the CreateProvisioningArtifact request.
type CreateProvisioningArtifactInput struct {
	Parameters       *ProvisioningArtifactProperties `json:"Parameters"`
	ProductID        string                          `json:"ProductId"`
	IdempotencyToken string                          `json:"IdempotencyToken,omitempty"`
}

// ProvisioningArtifactOutput is the response of artifact create and update operations.
type ProvisioningArtifactOutput struct {
	ProvisioningArtifactDetail *ProvisioningArtifactDetail `json:"ProvisioningArtifactDetail"`
	Info                       map[string]string           `json:"Info"`
	Status                     string                      `json:"Status"`
}

// CreateProvisioningArtifact adds a version to a product.
func (b *InMemoryBackend) CreateProvisioningArtifact(
	ctx context.Context,
	in *CreateProvisioningArtifactInput,
) (*ProvisioningArtifactOutput, error) {
	if in.Parameters == nil {
		return nil, invalid("Parameters is required")
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("CreateProvisioningArtifact")
	prod, err := b.ownedProductLocked(account, region, in.ProductID)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	art, err := b.newArtifact(ctx, prod.Type, in.Parameters)
	if err != nil {
		return nil, err
	}

	got, err := idemCreate(b, "CreateProvisioningArtifact", in.IdempotencyToken, in,
		func(a *Artifact) string { return a.ID },
		func(id string) (*Artifact, error) {
			b.mu.RLock("CreateProvisioningArtifact")
			defer b.mu.RUnlock()

			a, ok := b.artifacts.Get(id)
			if !ok {
				return nil, notFound("provisioning artifact", id)
			}

			return a, nil
		},
		func() (*Artifact, error) {
			b.mu.Lock("CreateProvisioningArtifact")
			defer b.mu.Unlock()

			if _, perr := b.ownedProductLocked(account, region, in.ProductID); perr != nil {
				return nil, perr
			}

			art.ID, art.ProductID, art.CreatedTime = newID("pa-"), in.ProductID, b.now()
			b.artifacts.Put(art)

			return art, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &ProvisioningArtifactOutput{
		ProvisioningArtifactDetail: artifactDetail(got), Info: artifactInfoOut(got), Status: statusAvailable,
	}, nil
}

// UpdateProvisioningArtifactInput is the UpdateProvisioningArtifact request.
type UpdateProvisioningArtifactInput struct {
	Active                 *bool   `json:"Active,omitempty"`
	Description            *string `json:"Description,omitempty"`
	ProductID              string  `json:"ProductId"`
	ProvisioningArtifactID string  `json:"ProvisioningArtifactId"`
	Name                   string  `json:"Name,omitempty"`
	Guidance               string  `json:"Guidance,omitempty"`
}

func (b *InMemoryBackend) ownedArtifactLocked(account, region, productID, artifactID string) (*Artifact, error) {
	if _, err := b.ownedProductLocked(account, region, productID); err != nil {
		return nil, err
	}

	a, ok := b.artifacts.Get(artifactID)
	if !ok || a.ProductID != productID {
		return nil, notFound("provisioning artifact", artifactID)
	}

	return a, nil
}

// UpdateProvisioningArtifact edits a version.
func (b *InMemoryBackend) UpdateProvisioningArtifact(
	ctx context.Context,
	in *UpdateProvisioningArtifactInput,
) (*ProvisioningArtifactOutput, error) {
	if in.Guidance != "" && in.Guidance != "DEFAULT" && in.Guidance != "DEPRECATED" {
		return nil, invalid("Guidance must be DEFAULT or DEPRECATED")
	}

	b.mu.Lock("UpdateProvisioningArtifact")
	defer b.mu.Unlock()

	a, err := b.ownedArtifactLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ProductID, in.ProvisioningArtifactID)
	if err != nil {
		return nil, err
	}

	if in.Name != "" {
		a.Name = in.Name
	}

	setIf(&a.Description, in.Description)

	if in.Guidance != "" {
		a.Guidance = in.Guidance
	}

	if in.Active != nil {
		a.Inactive = !*in.Active
	}

	return &ProvisioningArtifactOutput{
		ProvisioningArtifactDetail: artifactDetail(a), Info: artifactInfoOut(a), Status: statusAvailable,
	}, nil
}

// DeleteProvisioningArtifactInput is the DeleteProvisioningArtifact request.
type DeleteProvisioningArtifactInput struct {
	ProductID              string `json:"ProductId"`
	ProvisioningArtifactID string `json:"ProvisioningArtifactId"`
}

// DeleteProvisioningArtifact removes a version that no provisioned product uses.
func (b *InMemoryBackend) DeleteProvisioningArtifact(
	ctx context.Context,
	in *DeleteProvisioningArtifactInput,
) (*EmptyOutput, error) {
	b.mu.Lock("DeleteProvisioningArtifact")
	defer b.mu.Unlock()

	a, err := b.ownedArtifactLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ProductID, in.ProvisioningArtifactID)
	if err != nil {
		return nil, err
	}

	for _, pp := range b.provisioned.All() {
		if pp.ArtifactID == a.ID {
			return nil, fail(
				ErrResourceInUse,
				"provisioning artifact %s is used by provisioned product %s",
				a.ID,
				pp.ID,
			)
		}
	}

	for _, l := range b.actionLinks.All() {
		if l.ArtifactID == a.ID {
			b.actionLinks.Delete(l.Key)
		}
	}

	b.artifacts.Delete(a.ID)

	return &EmptyOutput{}, nil
}

// ListProvisioningArtifactsInput is the ListProvisioningArtifacts request.
type ListProvisioningArtifactsInput struct {
	ProductID string `json:"ProductId"`
}

// ListProvisioningArtifactsOutput is the ListProvisioningArtifacts response.
type ListProvisioningArtifactsOutput struct {
	NextPageToken               string                       `json:"NextPageToken,omitempty"`
	ProvisioningArtifactDetails []ProvisioningArtifactDetail `json:"ProvisioningArtifactDetails"`
}

// ListProvisioningArtifacts lists the versions of a product.
func (b *InMemoryBackend) ListProvisioningArtifacts(
	ctx context.Context,
	in *ListProvisioningArtifactsInput,
) (*ListProvisioningArtifactsOutput, error) {
	b.mu.RLock("ListProvisioningArtifacts")
	defer b.mu.RUnlock()

	if _, err := b.ownedProductLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ProductID); err != nil {
		return nil, err
	}

	out := []ProvisioningArtifactDetail{}
	for _, a := range b.artifactsOfLocked(in.ProductID) {
		out = append(out, *artifactDetail(a))
	}

	return &ListProvisioningArtifactsOutput{ProvisioningArtifactDetails: out}, nil
}

// ParameterConstraints is the ParameterConstraints shape.
type ParameterConstraints struct {
	AllowedPattern        string   `json:"AllowedPattern,omitempty"`
	ConstraintDescription string   `json:"ConstraintDescription,omitempty"`
	AllowedValues         []string `json:"AllowedValues,omitempty"`
}

// ProvisioningArtifactParameter is the ProvisioningArtifactParameter shape.
type ProvisioningArtifactParameter struct {
	ParameterConstraints *ParameterConstraints `json:"ParameterConstraints,omitempty"`
	DefaultValue         string                `json:"DefaultValue,omitempty"`
	Description          string                `json:"Description,omitempty"`
	ParameterKey         string                `json:"ParameterKey"`
	ParameterType        string                `json:"ParameterType,omitempty"`
	IsNoEcho             bool                  `json:"IsNoEcho"`
}

func artifactParameters(info TemplateInfo) []ProvisioningArtifactParameter {
	out := make([]ProvisioningArtifactParameter, 0, len(info.Parameters))

	for _, p := range info.Parameters {
		out = append(out, ProvisioningArtifactParameter{
			ParameterKey:  p.Key,
			ParameterType: p.Type,
			DefaultValue:  p.Default,
			Description:   p.Description,
			IsNoEcho:      p.NoEcho,
			ParameterConstraints: &ParameterConstraints{
				AllowedPattern:        p.AllowedPattern,
				ConstraintDescription: p.ConstraintDescription,
				AllowedValues:         p.AllowedValues,
			},
		})
	}

	return out
}

// DescribeProvisioningArtifactInput is the DescribeProvisioningArtifact request.
type DescribeProvisioningArtifactInput struct {
	ProductID                             string `json:"ProductId,omitempty"`
	ProductName                           string `json:"ProductName,omitempty"`
	ProvisioningArtifactID                string `json:"ProvisioningArtifactId,omitempty"`
	ProvisioningArtifactName              string `json:"ProvisioningArtifactName,omitempty"`
	IncludeProvisioningArtifactParameters bool   `json:"IncludeProvisioningArtifactParameters,omitempty"`
	Verbose                               bool   `json:"Verbose,omitempty"`
}

// DescribeProvisioningArtifactOutput is the DescribeProvisioningArtifact response.
type DescribeProvisioningArtifactOutput struct {
	ProvisioningArtifactDetail     *ProvisioningArtifactDetail     `json:"ProvisioningArtifactDetail"`
	Info                           map[string]string               `json:"Info"`
	Status                         string                          `json:"Status"`
	ProvisioningArtifactParameters []ProvisioningArtifactParameter `json:"ProvisioningArtifactParameters,omitempty"`
}

func (b *InMemoryBackend) visibleArtifactLocked(
	ctx context.Context, account, region, productID, productName, artID, artName string,
) (*Product, *Artifact, error) {
	p, err := b.visibleProductLocked(ctx, account, region, productID, productName)
	if err != nil {
		return nil, nil, err
	}

	arts := b.artifactsOfLocked(p.ID)

	for _, a := range arts {
		if (artID != "" && a.ID == artID) || (artID == "" && artName != "" && a.Name == artName) {
			return p, a, nil
		}
	}

	if artID == "" && artName == "" {
		for _, art := range slices.Backward(arts) {
			if !art.Inactive {
				return p, art, nil
			}
		}
	}

	return nil, nil, notFound("provisioning artifact", strOr(artID, artName))
}

// DescribeProvisioningArtifact returns a version of a product the caller can launch.
func (b *InMemoryBackend) DescribeProvisioningArtifact(
	ctx context.Context,
	in *DescribeProvisioningArtifactInput,
) (*DescribeProvisioningArtifactOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("DescribeProvisioningArtifact")
	_, a, err := b.visibleArtifactLocked(ctx, account, region, in.ProductID, in.ProductName,
		in.ProvisioningArtifactID, in.ProvisioningArtifactName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	out := &DescribeProvisioningArtifactOutput{
		ProvisioningArtifactDetail: artifactDetail(a), Info: artifactInfoOut(a), Status: statusAvailable,
	}

	if in.IncludeProvisioningArtifactParameters {
		_, info, terr := b.templateInfoOf(ctx, a)
		if terr != nil {
			return nil, terr
		}

		out.ProvisioningArtifactParameters = artifactParameters(info)
	}

	return out, nil
}

// DescribeProvisioningParametersInput is the DescribeProvisioningParameters request.
type DescribeProvisioningParametersInput struct {
	ProductID                string `json:"ProductId,omitempty"`
	ProductName              string `json:"ProductName,omitempty"`
	ProvisioningArtifactID   string `json:"ProvisioningArtifactId,omitempty"`
	ProvisioningArtifactName string `json:"ProvisioningArtifactName,omitempty"`
	PathID                   string `json:"PathId,omitempty"`
	PathName                 string `json:"PathName,omitempty"`
}

// UsageInstruction is the UsageInstruction shape.
type UsageInstruction struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// ProvisioningArtifactOutputWire is the ProvisioningArtifactOutput shape.
type ProvisioningArtifactOutputWire struct {
	Description string `json:"Description,omitempty"`
	Key         string `json:"Key"`
}

// TagOptionSummary is the TagOptionSummary shape.
type TagOptionSummary struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// DescribeProvisioningParametersOutput is the DescribeProvisioningParameters response.
type DescribeProvisioningParametersOutput struct {
	ProvisioningArtifactParameters []ProvisioningArtifactParameter  `json:"ProvisioningArtifactParameters"`
	ConstraintSummaries            []ConstraintSummary              `json:"ConstraintSummaries"`
	UsageInstructions              []UsageInstruction               `json:"UsageInstructions"`
	TagOptions                     []TagOptionSummary               `json:"TagOptions"`
	ProvisioningArtifactOutputKeys []ProvisioningArtifactOutputWire `json:"ProvisioningArtifactOutputKeys"`
	ProvisioningArtifactOutputs    []ProvisioningArtifactOutputWire `json:"ProvisioningArtifactOutputs"`
}

// DescribeProvisioningParameters returns what a launch of a version needs and which constraints apply.
func (b *InMemoryBackend) DescribeProvisioningParameters(
	ctx context.Context,
	in *DescribeProvisioningParametersInput,
) (*DescribeProvisioningParametersOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("DescribeProvisioningParameters")
	p, a, err := b.visibleArtifactLocked(ctx, account, region, in.ProductID, in.ProductName,
		in.ProvisioningArtifactID, in.ProvisioningArtifactName)

	var path *Portfolio
	if err == nil {
		path, err = b.resolvePathLocked(ctx, account, region, p.ID, in.PathID, in.PathName)
	}

	var (
		summaries []ConstraintSummary
		usage     []UsageInstruction
		options   []TagOptionSummary
	)

	if err == nil {
		summaries = b.constraintSummariesLocked(path.ID, p.ID)
		usage = b.usageInstructionsLocked(path.ID, p.ID)
		options = b.tagOptionSummariesLocked(account, path, p.ID)
	}

	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	_, info, err := b.templateInfoOf(ctx, a)
	if err != nil {
		return nil, err
	}

	outs := make([]ProvisioningArtifactOutputWire, 0, len(info.Outputs))
	for _, o := range info.Outputs {
		outs = append(outs, ProvisioningArtifactOutputWire{Key: o.Key, Description: o.Description})
	}

	return &DescribeProvisioningParametersOutput{
		ProvisioningArtifactParameters: artifactParameters(info), ConstraintSummaries: orEmpty(summaries),
		UsageInstructions: orEmpty(usage), TagOptions: orEmpty(options), ProvisioningArtifactOutputKeys: outs,
		ProvisioningArtifactOutputs: outs,
	}, nil
}

func (b *InMemoryBackend) usageInstructionsLocked(portfolioID, productID string) []UsageInstruction {
	var out []UsageInstruction

	for _, c := range b.constraintsForLocked(portfolioID, productID) {
		if c.Type == constraintTemplate {
			out = append(out, UsageInstruction{Type: "rules", Value: c.Parameters})
		}
	}

	return out
}

func (b *InMemoryBackend) tagOptionSummariesLocked(account string, path *Portfolio, productID string) []TagOptionSummary {
	vals := map[string][]string{}

	if !b.sharesTagOptionsLocked(account, path) {
		return []TagOptionSummary{}
	}

	for _, id := range []string{path.ID, productID} {
		for _, to := range b.tagOptionsForLocked(id) {
			if to.Active != nil && *to.Active && !slices.Contains(vals[to.Key], to.Value) {
				vals[to.Key] = append(vals[to.Key], to.Value)
			}
		}
	}

	out := make([]TagOptionSummary, 0, len(vals))

	for _, k := range sortedKeys(vals) {
		slices.Sort(vals[k])
		out = append(out, TagOptionSummary{Key: k, Values: vals[k]})
	}

	return out
}

func isExternalProduct(t string) bool { return t != productTypeCFN && strings.TrimSpace(t) != "" }
