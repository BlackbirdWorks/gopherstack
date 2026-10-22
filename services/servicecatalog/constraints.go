package servicecatalog

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"
	"strings"
)

const maxNotificationARNs = 5

func registerConstraintOps(m map[string]opFunc, b *InMemoryBackend) {
	m["CreateConstraint"] = op(b.CreateConstraint)
	m["UpdateConstraint"] = op(b.UpdateConstraint)
	m["DeleteConstraint"] = op(b.DeleteConstraint)
	m["DescribeConstraint"] = op(b.DescribeConstraint)
	m["ListConstraintsForPortfolio"] = op(b.ListConstraintsForPortfolio)
}

func constraintDetail(c *Constraint) *ConstraintDetail {
	return &ConstraintDetail{
		ConstraintID: c.ID, Description: c.Description, Owner: c.AccountID, PortfolioID: c.PortfolioID,
		ProductID: c.ProductID, Type: c.Type,
	}
}

// validateConstraintParameters checks the JSON a constraint type requires.
func validateConstraintParameters(typ, params string) error {
	var doc map[string]any
	if err := json.Unmarshal([]byte(params), &doc); err != nil {
		return invalid("Parameters is not a valid JSON object")
	}

	switch typ {
	case constraintLaunch:
		return validateLaunchConstraint(doc)
	case constraintNotify:
		return validateNotificationConstraint(doc)
	case constraintTemplate:
		rules, ok := doc["Rules"].(map[string]any)
		if !ok {
			return invalid("a TEMPLATE constraint needs a Rules object")
		}

		return validateTemplateRules(rules)
	case constraintStackSet:
		return validateStackSetConstraint(doc)
	case constraintResource:
		v := stringField(doc, "TagUpdateOnProvisionedProduct")
		if v != "ALLOWED" && v != "NOT_ALLOWED" {
			return invalid("TagUpdateOnProvisionedProduct must be ALLOWED or NOT_ALLOWED")
		}
	default:
		return invalid("Type must be LAUNCH, NOTIFICATION, RESOURCE_UPDATE, STACKSET or TEMPLATE")
	}

	return nil
}

func validateLaunchConstraint(doc map[string]any) error {
	role, local := stringField(doc, "RoleArn"), stringField(doc, "LocalRoleName")
	if (role == "") == (local == "") {
		return invalid("a LAUNCH constraint needs exactly one of RoleArn and LocalRoleName")
	}

	if role != "" && !strings.HasPrefix(role, "arn:") {
		return invalid("RoleArn %q is not an ARN", role)
	}

	return nil
}

func validateNotificationConstraint(doc map[string]any) error {
	arns, ok := doc["NotificationArns"].([]any)
	if !ok || len(arns) == 0 || len(arns) > maxNotificationARNs {
		return invalid("a NOTIFICATION constraint needs between 1 and %d NotificationArns", maxNotificationARNs)
	}

	for _, a := range arns {
		if s, isStr := a.(string); !isStr || !strings.HasPrefix(s, "arn:") {
			return invalid("NotificationArns must be SNS topic ARNs")
		}
	}

	return nil
}

func validateStackSetConstraint(doc map[string]any) error {
	if _, ok := doc["Version"]; !ok {
		return invalid("a STACKSET constraint needs a Version")
	}

	props, ok := doc["Properties"].(map[string]any)
	if !ok {
		return invalid("a STACKSET constraint needs a Properties object")
	}

	for _, k := range []string{"AccountList", "RegionList"} {
		if len(stringList(props[k])) == 0 {
			return invalid("a STACKSET constraint needs a non-empty %s", k)
		}
	}

	for _, k := range []string{"AdminRole", "ExecutionRole"} {
		if stringField(props, k) == "" {
			return invalid("a STACKSET constraint needs %s", k)
		}
	}

	return nil
}

func stringField(doc map[string]any, key string) string {
	s, _ := doc[key].(string)

	return s
}

// CreateConstraintInput is the CreateConstraint request.
type CreateConstraintInput struct {
	PortfolioID      string `json:"PortfolioId"`
	ProductID        string `json:"ProductId"`
	Parameters       string `json:"Parameters"`
	Type             string `json:"Type"`
	Description      string `json:"Description,omitempty"`
	IdempotencyToken string `json:"IdempotencyToken,omitempty"`
}

// ConstraintOutput is the response of constraint create, update and describe operations.
type ConstraintOutput struct {
	ConstraintDetail     *ConstraintDetail `json:"ConstraintDetail"`
	ConstraintParameters string            `json:"ConstraintParameters"`
	Status               string            `json:"Status"`
}

// CreateConstraint adds a constraint on a product within a portfolio.
func (b *InMemoryBackend) CreateConstraint(ctx context.Context, in *CreateConstraintInput) (*ConstraintOutput, error) {
	if err := validateConstraintParameters(in.Type, in.Parameters); err != nil {
		return nil, err
	}

	if len(in.Description) > maxDescription {
		return nil, invalid("Description must be at most %d characters", maxDescription)
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	c, err := idemCreate(b, "CreateConstraint", in.IdempotencyToken, in,
		func(c *Constraint) string { return c.ID },
		func(id string) (*Constraint, error) {
			b.mu.RLock("CreateConstraint")
			defer b.mu.RUnlock()

			return b.constraintLocked(account, region, id)
		},
		func() (*Constraint, error) {
			b.mu.Lock("CreateConstraint")
			defer b.mu.Unlock()

			return b.createConstraintLocked(account, region, in)
		},
	)
	if err != nil {
		return nil, err
	}

	return &ConstraintOutput{
		ConstraintDetail:     constraintDetail(c),
		ConstraintParameters: c.Parameters,
		Status:               statusAvailable,
	}, nil
}

func (b *InMemoryBackend) createConstraintLocked(
	account, region string,
	in *CreateConstraintInput,
) (*Constraint, error) {
	if _, err := b.ownedPortfolioLocked(account, region, in.PortfolioID); err != nil {
		return nil, err
	}

	if _, err := b.ownedProductLocked(account, region, in.ProductID); err != nil {
		return nil, err
	}

	if _, ok := b.associations.Get(in.PortfolioID + "|" + in.ProductID); !ok {
		return nil, invalid("product %s is not associated with portfolio %s", in.ProductID, in.PortfolioID)
	}

	if in.Type == constraintLaunch || in.Type == constraintStackSet {
		for _, c := range b.constraintsForLocked(in.PortfolioID, in.ProductID) {
			if c.Type == constraintLaunch || c.Type == constraintStackSet {
				return nil, fail(
					ErrDuplicateResource,
					"a %s constraint already exists for this product in the portfolio",
					c.Type,
				)
			}
		}
	} else {
		for _, c := range b.constraintsForLocked(in.PortfolioID, in.ProductID) {
			if c.Type == in.Type {
				return nil, fail(
					ErrDuplicateResource,
					"a %s constraint already exists for this product in the portfolio",
					c.Type,
				)
			}
		}
	}

	c := &Constraint{
		ID: newID("cons-"), AccountID: account, Region: region, PortfolioID: in.PortfolioID, ProductID: in.ProductID,
		Type: in.Type, Description: in.Description, Parameters: in.Parameters,
	}
	b.constraints.Put(c)

	return c, nil
}

func (b *InMemoryBackend) constraintLocked(account, region, id string) (*Constraint, error) {
	c, ok := b.constraints.Get(id)
	if !ok || c.AccountID != account || c.Region != region {
		return nil, notFound("constraint", id)
	}

	return c, nil
}

func (b *InMemoryBackend) constraintsForLocked(portfolioID, productID string) []*Constraint {
	var out []*Constraint

	for _, c := range b.constraints.All() {
		if c.PortfolioID == portfolioID && c.ProductID == productID {
			out = append(out, c)
		}
	}

	slices.SortFunc(out, func(x, y *Constraint) int { return cmp.Compare(x.ID, y.ID) })

	return out
}

func (b *InMemoryBackend) constraintSummariesLocked(portfolioID, productID string) []ConstraintSummary {
	cons := b.constraintsForLocked(portfolioID, productID)

	out := make([]ConstraintSummary, 0, len(cons))
	for _, c := range cons {
		out = append(out, ConstraintSummary{Type: c.Type, Description: c.Description})
	}

	return out
}

// UpdateConstraintInput is the UpdateConstraint request.
type UpdateConstraintInput struct {
	Description *string `json:"Description,omitempty"`
	ID          string  `json:"Id"`
	Parameters  string  `json:"Parameters,omitempty"`
}

// UpdateConstraint changes a constraint's description or parameters.
func (b *InMemoryBackend) UpdateConstraint(ctx context.Context, in *UpdateConstraintInput) (*ConstraintOutput, error) {
	b.mu.Lock("UpdateConstraint")
	defer b.mu.Unlock()

	c, err := b.constraintLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	if in.Parameters != "" {
		if err = validateConstraintParameters(c.Type, in.Parameters); err != nil {
			return nil, err
		}
	}

	if in.Description != nil && len(*in.Description) > maxDescription {
		return nil, invalid("Description must be at most %d characters", maxDescription)
	}

	if in.Parameters != "" {
		c.Parameters = in.Parameters
	}

	setIf(&c.Description, in.Description)

	return &ConstraintOutput{
		ConstraintDetail:     constraintDetail(c),
		ConstraintParameters: c.Parameters,
		Status:               statusAvailable,
	}, nil
}

// DescribeConstraintInput is the DescribeConstraint and DeleteConstraint request.
type DescribeConstraintInput struct {
	ID string `json:"Id"`
}

// DeleteConstraint removes a constraint.
func (b *InMemoryBackend) DeleteConstraint(ctx context.Context, in *DescribeConstraintInput) (*EmptyOutput, error) {
	b.mu.Lock("DeleteConstraint")
	defer b.mu.Unlock()

	c, err := b.constraintLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	b.constraints.Delete(c.ID)

	return &EmptyOutput{}, nil
}

// DescribeConstraint returns a constraint.
func (b *InMemoryBackend) DescribeConstraint(
	ctx context.Context,
	in *DescribeConstraintInput,
) (*ConstraintOutput, error) {
	b.mu.RLock("DescribeConstraint")
	defer b.mu.RUnlock()

	c, err := b.constraintLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	return &ConstraintOutput{
		ConstraintDetail:     constraintDetail(c),
		ConstraintParameters: c.Parameters,
		Status:               statusAvailable,
	}, nil
}

// ListConstraintsForPortfolioInput is the ListConstraintsForPortfolio request.
type ListConstraintsForPortfolioInput struct {
	PortfolioID string `json:"PortfolioId"`
	ProductID   string `json:"ProductId,omitempty"`
	Paging
}

// ListConstraintsForPortfolioOutput is the ListConstraintsForPortfolio response.
type ListConstraintsForPortfolioOutput struct {
	NextPageToken     string             `json:"NextPageToken,omitempty"`
	ConstraintDetails []ConstraintDetail `json:"ConstraintDetails"`
}

// ListConstraintsForPortfolio lists the constraints of a portfolio, optionally for one product.
func (b *InMemoryBackend) ListConstraintsForPortfolio(
	ctx context.Context,
	in *ListConstraintsForPortfolioInput,
) (*ListConstraintsForPortfolioOutput, error) {
	b.mu.RLock("ListConstraintsForPortfolio")
	defer b.mu.RUnlock()

	if _, err := b.ownedPortfolioLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.PortfolioID); err != nil {
		return nil, err
	}

	var all []ConstraintDetail

	for _, c := range b.constraints.All() {
		if c.PortfolioID == in.PortfolioID && (in.ProductID == "" || c.ProductID == in.ProductID) {
			all = append(all, *constraintDetail(c))
		}
	}

	slices.SortFunc(all, func(x, y ConstraintDetail) int { return cmp.Compare(x.ConstraintID, y.ConstraintID) })

	pg, err := pageOf(all, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListConstraintsForPortfolioOutput{ConstraintDetails: orEmpty(pg.Data), NextPageToken: pg.Next}, nil
}
