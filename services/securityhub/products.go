package securityhub

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var knownProducts = []Product{ //nolint:gochecknoglobals // read-only lookup data
	{
		ProductArn:       "arn:aws:securityhub:us-east-1::product/aws/guardduty",
		ProductName:      "GuardDuty",
		CompanyName:      companyAWS,
		Description:      "Amazon GuardDuty is a threat detection service.",
		Categories:       []string{"Software and Configuration Checks"},
		IntegrationTypes: []string{intTypeSendFindings},
		MarketplaceURL:   "https://aws.amazon.com/guardduty/",
		ActivationURL:    "https://console.aws.amazon.com/guardduty/",
	},
	{
		ProductArn:       "arn:aws:securityhub:us-east-1::product/aws/inspector",
		ProductName:      "Inspector",
		CompanyName:      companyAWS,
		Description:      "Amazon Inspector is an automated security assessment service.",
		Categories:       []string{"Software and Configuration Checks"},
		IntegrationTypes: []string{intTypeSendFindings},
		MarketplaceURL:   "https://aws.amazon.com/inspector/",
		ActivationURL:    "https://console.aws.amazon.com/inspector/",
	},
	{
		ProductArn:       "arn:aws:securityhub:us-east-1::product/aws/macie",
		ProductName:      "Macie",
		CompanyName:      companyAWS,
		Description:      "Amazon Macie is a data security service.",
		Categories:       []string{"Sensitive Data Identifications"},
		IntegrationTypes: []string{intTypeSendFindings},
		MarketplaceURL:   "https://aws.amazon.com/macie/",
		ActivationURL:    "https://console.aws.amazon.com/macie/",
	},
}

func (b *InMemoryBackend) DescribeProducts(productArn, nextToken string, maxResults int) ([]*Product, string) {
	b.mu.RLock("DescribeProducts")
	defer b.mu.RUnlock()

	var results []*Product

	for i := range knownProducts {
		p := knownProducts[i]
		if productArn == "" || p.ProductArn == productArn {
			results = append(results, &p)
		}
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []*Product{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

func (b *InMemoryBackend) EnableImportFindingsForProduct(productArn string) (string, error) {
	b.mu.Lock("EnableImportFindingsForProduct")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return "", ErrHubNotEnabled
	}

	// Check if already enabled
	for subArn, pArn := range b.productSubscriptions {
		if pArn == productArn {
			return subArn, ErrAlreadyExists
		}
	}

	subArn := arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("product-subscription/%s", productArn))
	b.productSubscriptions[subArn] = productArn

	return subArn, nil
}

func (b *InMemoryBackend) DisableImportFindingsForProduct(productSubscriptionArn string) error {
	b.mu.Lock("DisableImportFindingsForProduct")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return ErrHubNotEnabled
	}

	if _, ok := b.productSubscriptions[productSubscriptionArn]; !ok {
		return fmt.Errorf("%w: product subscription %s", ErrNotFound, productSubscriptionArn)
	}

	delete(b.productSubscriptions, productSubscriptionArn)

	return nil
}

func (b *InMemoryBackend) ListEnabledProductsForImport(nextToken string, maxResults int) ([]string, string) {
	b.mu.RLock("ListEnabledProductsForImport")
	defer b.mu.RUnlock()

	results := make([]string, 0, len(b.productSubscriptions))
	for subArn := range b.productSubscriptions {
		results = append(results, subArn)
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []string{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

func (b *InMemoryBackend) DescribeProductsV2(nextToken string, maxResults int) ([]*Product, string) {
	// V2 reuses same product catalog as V1
	return b.DescribeProducts("", nextToken, maxResults)
}

// recommendationStatusSucceeded mirrors types.RecommendationStatusSucceeded
// (securityhub@v1.75.4 types/enums.go); this backend generates synchronously
// so a recommendation is always immediately SUCCEEDED, never IN_PROGRESS/FAILED.
const recommendationStatusSucceeded = "SUCCEEDED"

// recommendedActionCreatePolicy is one of the (undocumented-as-Go-enum,
// plain-string) RecommendedAction values UnusedPermissionsRecommendationStep's
// doc comments describe -- "create or detach a policy for an unused
// permissions finding" -- used for the CREATE_POLICY case this backend
// always synthesizes (no ExistingPolicyId is tracked, so there is nothing to
// detach).
const recommendedActionCreatePolicy = "CREATE_POLICY"

func (b *InMemoryBackend) GenerateRecommendedPolicyV2(metadataUID string) (*RecommendedPolicyV2, error) {
	b.mu.Lock("GenerateRecommendedPolicyV2")
	defer b.mu.Unlock()

	recommendedPolicy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"securityhub:*","Resource":"*"}]}`

	rec := &RecommendedPolicyV2{
		MetadataUid:       metadataUID,
		Status:            recommendationStatusSucceeded,
		RecommendedAction: recommendedActionCreatePolicy,
		RecommendedPolicy: recommendedPolicy,
		PolicyUpdatedAt:   time.Now().UTC().Format(time.RFC3339),
	}
	b.recommendedPoliciesV2.Put(rec)

	return rec, nil
}

func (b *InMemoryBackend) GetRecommendedPolicyV2(metadataUID string) (*RecommendedPolicyV2, error) {
	b.mu.RLock("GetRecommendedPolicyV2")
	defer b.mu.RUnlock()

	rec, ok := b.recommendedPoliciesV2.Get(metadataUID)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *rec

	return &cp, nil
}
