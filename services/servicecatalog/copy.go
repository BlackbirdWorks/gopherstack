package servicecatalog

import (
	"context"
	"maps"
	"strings"
)

const arnPartCount = 6

// CopyProductInput is the CopyProduct request.
type CopyProductInput struct {
	SourceProductArn                      string              `json:"SourceProductArn"`
	TargetProductID                       string              `json:"TargetProductId,omitempty"`
	TargetProductName                     string              `json:"TargetProductName,omitempty"`
	IdempotencyToken                      string              `json:"IdempotencyToken,omitempty"`
	CopyOptions                           []string            `json:"CopyOptions,omitempty"`
	SourceProvisioningArtifactIdentifiers []map[string]string `json:"SourceProvisioningArtifactIdentifiers,omitempty"`
}

// CopyProductOutput is the CopyProduct response.
type CopyProductOutput struct {
	CopyProductToken string `json:"CopyProductToken"`
}

// CopyProduct copies a product, or some of its versions, into this Region.
func (b *InMemoryBackend) CopyProduct(ctx context.Context, in *CopyProductInput) (*CopyProductOutput, error) {
	srcID, ok := productIDFromARN(in.SourceProductArn)
	if !ok {
		return nil, invalid("SourceProductArn %q is not a product ARN", in.SourceProductArn)
	}

	if (in.TargetProductID != "") == (in.TargetProductName != "") && in.TargetProductID != "" {
		return nil, invalid("specify at most one of TargetProductId and TargetProductName")
	}

	for _, o := range in.CopyOptions {
		if o != "CopyTags" {
			return nil, invalid("CopyOptions entry %q is not supported", o)
		}
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	out, err := idemReplay(b, "CopyProduct", in.IdempotencyToken, in, func() (*CopyProductOutput, error) {
		b.mu.Lock("CopyProduct")
		defer b.mu.Unlock()

		return b.copyProductLocked(account, region, srcID, in)
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

func productIDFromARN(a string) (string, bool) {
	parts := strings.SplitN(a, ":", arnPartCount)
	if len(parts) != arnPartCount || parts[2] != "catalog" || !strings.HasPrefix(parts[5], "product/") {
		return "", false
	}

	return strings.TrimPrefix(parts[5], "product/"), true
}

func (b *InMemoryBackend) copyProductLocked(
	account, region, srcID string,
	in *CopyProductInput,
) (*CopyProductOutput, error) {
	src, ok := b.products.Get(srcID)
	if !ok || src.AccountID != account || src.ARN != in.SourceProductArn {
		return nil, notFound("product", srcID)
	}

	wanted := map[string]bool{}
	for _, m := range in.SourceProvisioningArtifactIdentifiers {
		wanted[m["Id"]] = true
	}

	var arts []*Artifact

	for _, a := range b.artifactsOfLocked(src.ID) {
		if len(wanted) == 0 || wanted[a.ID] {
			arts = append(arts, a)
		}
	}

	if len(wanted) > 0 && len(arts) != len(wanted) {
		return nil, notFound("provisioning artifact", "in the source product")
	}

	now := b.now()

	var target *Product

	if in.TargetProductID != "" {
		var err error
		if target, err = b.ownedProductLocked(account, region, in.TargetProductID); err != nil {
			return nil, err
		}
	} else {
		id := newID("prod-")
		cp := *src
		cp.ID, cp.ViewID, cp.ARN, cp.Region, cp.CreatedTime = id, newID(
			"prodview-",
		), b.productARN(
			account,
			region,
			id,
		), region, now
		cp.SourceProductID, cp.Tags = src.ID, nil
		cp.Name = strOr(in.TargetProductName, src.Name)

		for _, o := range in.CopyOptions {
			if o == "CopyTags" {
				cp.Tags = maps.Clone(src.Tags)
			}
		}

		target = &cp
		b.products.Put(target)
	}

	for _, a := range arts {
		c := *a
		c.ID, c.ProductID, c.CreatedTime, c.Info = newID("pa-"), target.ID, now, maps.Clone(a.Info)
		b.artifacts.Put(&c)
	}

	token := newID("copy-")
	status := recSucceeded

	b.copies.Put(&CopyOperation{
		Token: token, AccountID: account, TargetProductID: target.ID, FinalStatus: status, SettleAt: now.Add(b.dwell),
	})

	return &CopyProductOutput{CopyProductToken: token}, nil
}

// DescribeCopyProductStatusInput is the DescribeCopyProductStatus request.
type DescribeCopyProductStatusInput struct {
	CopyProductToken string `json:"CopyProductToken"`
}

// DescribeCopyProductStatusOutput is the DescribeCopyProductStatus response.
type DescribeCopyProductStatusOutput struct {
	CopyProductStatus string `json:"CopyProductStatus"`
	StatusDetail      string `json:"StatusDetail,omitempty"`
	TargetProductID   string `json:"TargetProductId,omitempty"`
}

// DescribeCopyProductStatus reports the progress of a CopyProduct request.
func (b *InMemoryBackend) DescribeCopyProductStatus(
	ctx context.Context,
	in *DescribeCopyProductStatusInput,
) (*DescribeCopyProductStatusOutput, error) {
	b.mu.RLock("DescribeCopyProductStatus")
	defer b.mu.RUnlock()

	c, ok := b.copies.Get(in.CopyProductToken)
	if !ok || c.AccountID != b.callerAccount(ctx) {
		return nil, notFound("copy product token", in.CopyProductToken)
	}

	if b.now().Before(c.SettleAt) {
		return &DescribeCopyProductStatusOutput{
			CopyProductStatus: recInProgress,
			TargetProductID:   c.TargetProductID,
		}, nil
	}

	return &DescribeCopyProductStatusOutput{
		CopyProductStatus: c.FinalStatus, StatusDetail: c.StatusDetail, TargetProductID: c.TargetProductID,
	}, nil
}
