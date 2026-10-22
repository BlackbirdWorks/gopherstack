package servicecatalog

import (
	"cmp"
	"context"
	"math"
	"slices"
	"strings"
)

// ProvisionedProductDetail is the ProvisionedProductDetail shape.
type ProvisionedProductDetail struct {
	CreatedTime                        *float64 `json:"CreatedTime,omitempty"`
	Arn                                string   `json:"Arn"`
	ID                                 string   `json:"Id"`
	IdempotencyToken                   string   `json:"IdempotencyToken,omitempty"`
	LastProvisioningRecordID           string   `json:"LastProvisioningRecordId,omitempty"`
	LastRecordID                       string   `json:"LastRecordId,omitempty"`
	LastSuccessfulProvisioningRecordID string   `json:"LastSuccessfulProvisioningRecordId,omitempty"`
	LaunchRoleArn                      string   `json:"LaunchRoleArn,omitempty"`
	Name                               string   `json:"Name"`
	ProductID                          string   `json:"ProductId"`
	ProvisioningArtifactID             string   `json:"ProvisioningArtifactId"`
	Status                             string   `json:"Status"`
	StatusMessage                      string   `json:"StatusMessage,omitempty"`
	Type                               string   `json:"Type"`
}

// ProvisionedProductAttribute is the ProvisionedProductAttribute shape.
type ProvisionedProductAttribute struct {
	CreatedTime                        *float64 `json:"CreatedTime,omitempty"`
	PhysicalID                         string   `json:"PhysicalId,omitempty"`
	ProvisioningArtifactID             string   `json:"ProvisioningArtifactId"`
	IdempotencyToken                   string   `json:"IdempotencyToken,omitempty"`
	LastProvisioningRecordID           string   `json:"LastProvisioningRecordId,omitempty"`
	LastRecordID                       string   `json:"LastRecordId,omitempty"`
	LastSuccessfulProvisioningRecordID string   `json:"LastSuccessfulProvisioningRecordId,omitempty"`
	Name                               string   `json:"Name"`
	Arn                                string   `json:"Arn"`
	ID                                 string   `json:"Id"`
	ProductID                          string   `json:"ProductId"`
	ProductName                        string   `json:"ProductName,omitempty"`
	ProvisioningArtifactName           string   `json:"ProvisioningArtifactName,omitempty"`
	Status                             string   `json:"Status"`
	StatusMessage                      string   `json:"StatusMessage,omitempty"`
	UserArnSession                     string   `json:"UserArnSession,omitempty"`
	Type                               string   `json:"Type"`
	UserArn                            string   `json:"UserArn,omitempty"`
	Tags                               []Tag    `json:"Tags,omitempty"`
}

func (b *InMemoryBackend) ppDetail(pp *ProvisionedProduct) ProvisionedProductDetail {
	return ProvisionedProductDetail{
		CreatedTime: ts(pp.CreatedTime), Arn: pp.ARN, ID: pp.ID, IdempotencyToken: pp.IdempotencyToken,
		LastProvisioningRecordID: pp.LastProvisioningRecordID, LastRecordID: pp.LastRecordID,
		LastSuccessfulProvisioningRecordID: pp.LastSuccessfulProvisioningRecordID, LaunchRoleArn: pp.LaunchRoleArn,
		Name: pp.Name, ProductID: pp.ProductID, ProvisioningArtifactID: pp.ArtifactID, Status: b.ppStatus(pp),
		StatusMessage: pp.StatusMessage, Type: pp.Type,
	}
}

func (b *InMemoryBackend) ppAttribute(pp *ProvisionedProduct) ProvisionedProductAttribute {
	a := ProvisionedProductAttribute{
		CreatedTime:                        ts(pp.CreatedTime),
		Arn:                                pp.ARN,
		ID:                                 pp.ID,
		IdempotencyToken:                   pp.IdempotencyToken,
		LastProvisioningRecordID:           pp.LastProvisioningRecordID,
		LastRecordID:                       pp.LastRecordID,
		LastSuccessfulProvisioningRecordID: pp.LastSuccessfulProvisioningRecordID,
		Name:                               pp.Name,
		PhysicalID:                         pp.StackID,
		ProductID:                          pp.ProductID,
		ProvisioningArtifactID:             pp.ArtifactID,
		Status:                             b.ppStatus(pp),
		StatusMessage:                      pp.StatusMessage,
		Tags:                               mapToTags(pp.Tags),
		Type:                               pp.Type,
		UserArn:                            pp.UserArn,
	}

	if prod, ok := b.products.Get(pp.ProductID); ok {
		a.ProductName = prod.Name
	}

	if art, ok := b.artifacts.Get(pp.ArtifactID); ok {
		a.ProvisioningArtifactName = art.Name
	}

	return a
}

// DescribeProvisionedProductInput is the DescribeProvisionedProduct request.
type DescribeProvisionedProductInput struct {
	ID   string `json:"Id,omitempty"`
	Name string `json:"Name,omitempty"`
}

// DescribeProvisionedProductOutput is the DescribeProvisionedProduct response.
type DescribeProvisionedProductOutput struct {
	ProvisionedProductDetail *ProvisionedProductDetail `json:"ProvisionedProductDetail"`
	CloudWatchDashboards     []CloudWatchDashboard     `json:"CloudWatchDashboards"`
}

// DescribeProvisionedProduct returns a provisioned product by ID or name.
func (b *InMemoryBackend) DescribeProvisionedProduct(
	ctx context.Context,
	in *DescribeProvisionedProductInput,
) (*DescribeProvisionedProductOutput, error) {
	b.mu.RLock("DescribeProvisionedProduct")
	pp, err := b.provisionedLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID, in.Name)

	var (
		d     ProvisionedProductDetail
		stack string
	)

	if err == nil {
		d, stack = b.ppDetail(pp), pp.StackName
		if pp.Type == ppTypeCFNStackSet {
			stack = ""
		}
	}

	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return &DescribeProvisionedProductOutput{
		ProvisionedProductDetail: &d,
		CloudWatchDashboards:     b.dashboardsOf(ctx, stack),
	}, nil
}

func (b *InMemoryBackend) ownedProvisionedLocked(
	ctx context.Context,
	f *AccessLevelFilter,
) ([]*ProvisionedProduct, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	if f != nil {
		switch f.Key {
		case "", "Account", "Role", "User":
		default:
			return nil, invalid("AccessLevelFilter Key must be Account, Role or User")
		}
	}

	var out []*ProvisionedProduct

	for _, pp := range b.provisioned.All() {
		if pp.AccountID != account || pp.Region != region {
			continue
		}

		if f != nil && (f.Key == "User" || f.Key == "Role") {
			want := f.Value
			if want == "self" || want == "" {
				want = awsmetaCaller(ctx)
			}

			if want != "" && pp.UserArn != want {
				continue
			}
		}

		out = append(out, pp)
	}

	slices.SortFunc(out, func(x, y *ProvisionedProduct) int { return cmp.Compare(x.ID, y.ID) })

	return out, nil
}

// ScanProvisionedProductsInput is the ScanProvisionedProducts request.
type ScanProvisionedProductsInput struct {
	AccessLevelFilter *AccessLevelFilter `json:"AccessLevelFilter,omitempty"`
	Paging
}

// ScanProvisionedProductsOutput is the ScanProvisionedProducts response.
type ScanProvisionedProductsOutput struct {
	NextPageToken       string                     `json:"NextPageToken,omitempty"`
	ProvisionedProducts []ProvisionedProductDetail `json:"ProvisionedProducts"`
}

// ScanProvisionedProducts lists provisioned products visible at an access level.
func (b *InMemoryBackend) ScanProvisionedProducts(
	ctx context.Context,
	in *ScanProvisionedProductsInput,
) (*ScanProvisionedProductsOutput, error) {
	b.mu.RLock("ScanProvisionedProducts")
	defer b.mu.RUnlock()

	owned, err := b.ownedProvisionedLocked(ctx, in.AccessLevelFilter)
	if err != nil {
		return nil, err
	}

	pg, err := pageOf(owned, in.Paging)
	if err != nil {
		return nil, err
	}

	out := make([]ProvisionedProductDetail, 0, len(pg.Data))
	for _, pp := range pg.Data {
		out = append(out, b.ppDetail(pp))
	}

	return &ScanProvisionedProductsOutput{ProvisionedProducts: out, NextPageToken: pg.Next}, nil
}

// SearchProvisionedProductsInput is the SearchProvisionedProducts request.
type SearchProvisionedProductsInput struct {
	AccessLevelFilter *AccessLevelFilter  `json:"AccessLevelFilter,omitempty"`
	Filters           map[string][]string `json:"Filters,omitempty"`
	SortBy            string              `json:"SortBy,omitempty"`
	SortOrder         string              `json:"SortOrder,omitempty"`
	Paging
}

// SearchProvisionedProductsOutput is the SearchProvisionedProducts response.
type SearchProvisionedProductsOutput struct {
	NextPageToken       string                        `json:"NextPageToken,omitempty"`
	ProvisionedProducts []ProvisionedProductAttribute `json:"ProvisionedProducts"`
	TotalResultsCount   int32                         `json:"TotalResultsCount"`
}

func ppField(a ProvisionedProductAttribute, field string) (string, bool) {
	switch strings.ToLower(field) {
	case "arn":
		return a.Arn, true
	case "id":
		return a.ID, true
	case "name":
		return a.Name, true
	case queryStatus:
		return a.Status, true
	case "type":
		return a.Type, true
	case "productid":
		return a.ProductID, true
	case "productname":
		return a.ProductName, true
	case "provisioningartifactid":
		return a.ProvisioningArtifactID, true
	case "provisioningartifactname":
		return a.ProvisioningArtifactName, true
	case "physicalid":
		return a.PhysicalID, true
	case "lastrecordid":
		return a.LastRecordID, true
	case "idempotencytoken":
		return a.IdempotencyToken, true
	case "userarn":
		return a.UserArn, true
	case "userarnsession":
		return a.UserArnSession, true
	}

	return "", false
}

func attributeMatches(a ProvisionedProductAttribute, query string) bool {
	for tok := range strings.FieldsSeq(query) {
		if !tokenMatches(a, tok) {
			return false
		}
	}

	return true
}

// tokenMatches applies one query token: a "field:value" term when the field is known, else free text.
func tokenMatches(a ProvisionedProductAttribute, tok string) bool {
	if field, val, hasField := strings.Cut(tok, ":"); hasField {
		if strings.EqualFold(field, "tags") {
			return tagQueryMatches(a.Tags, val)
		}

		if got, known := ppField(a, field); known {
			return containsFold(got, val)
		}
	}

	return containsFold(a.Name, tok) || containsFold(a.ProductName, tok) || containsFold(a.ID, tok) ||
		containsFold(a.ProvisioningArtifactName, tok) || containsFold(a.Status, tok)
}

func tagQueryMatches(tags []Tag, spec string) bool {
	k, v, hasValue := strings.Cut(spec, "=")

	for _, t := range tags {
		if t.Key == k && (!hasValue || t.Value == v) {
			return true
		}
	}

	return false
}

// SearchProvisionedProducts searches provisioned products by a free-text and field query.
func (b *InMemoryBackend) SearchProvisionedProducts(
	ctx context.Context,
	in *SearchProvisionedProductsInput,
) (*SearchProvisionedProductsOutput, error) {
	for k := range in.Filters {
		if k != "SearchQuery" {
			return nil, invalid("unsupported filter %q", k)
		}
	}

	if in.SortOrder != "" && in.SortOrder != orderAscending && in.SortOrder != orderDescending {
		return nil, invalid("SortOrder must be ASCENDING or DESCENDING")
	}

	b.mu.RLock("SearchProvisionedProducts")
	defer b.mu.RUnlock()

	owned, err := b.ownedProvisionedLocked(ctx, in.AccessLevelFilter)
	if err != nil {
		return nil, err
	}

	var hits []ProvisionedProductAttribute

	for _, pp := range owned {
		a := b.ppAttribute(pp)
		if matchesAll(a, in.Filters["SearchQuery"]) {
			hits = append(hits, a)
		}
	}

	sortAttributes(hits, in.SortBy, in.SortOrder)

	pg, err := pageOf(hits, in.Paging)
	if err != nil {
		return nil, err
	}

	return &SearchProvisionedProductsOutput{
		ProvisionedProducts: orEmpty(
			pg.Data,
		), TotalResultsCount: int32(min(len(hits), math.MaxInt32)), NextPageToken: pg.Next,
	}, nil
}

func matchesAll(a ProvisionedProductAttribute, queries []string) bool {
	for _, q := range queries {
		if !attributeMatches(a, q) {
			return false
		}
	}

	return true
}

func sortAttributes(as []ProvisionedProductAttribute, by, order string) {
	slices.SortFunc(as, func(x, y ProvisionedProductAttribute) int {
		var c int

		switch strings.ToLower(by) {
		case "createdtime":
			c = cmp.Compare(derefF(x.CreatedTime), derefF(y.CreatedTime))
		case "name":
			c = cmp.Compare(x.Name, y.Name)
		case "status":
			c = cmp.Compare(x.Status, y.Status)
		case "productid":
			c = cmp.Compare(x.ProductID, y.ProductID)
		case "arn":
			c = cmp.Compare(x.Arn, y.Arn)
		}

		if c == 0 {
			c = cmp.Compare(x.ID, y.ID)
		}

		if order == orderDescending {
			return -c
		}

		return c
	})
}

func derefF(f *float64) float64 {
	if f == nil {
		return 0
	}

	return *f
}

// GetProvisionedProductOutputsInput is the GetProvisionedProductOutputs request.
type GetProvisionedProductOutputsInput struct {
	ProvisionedProductID   string   `json:"ProvisionedProductId,omitempty"`
	ProvisionedProductName string   `json:"ProvisionedProductName,omitempty"`
	OutputKeys             []string `json:"OutputKeys,omitempty"`
	Paging
}

// GetProvisionedProductOutputsOutput is the GetProvisionedProductOutputs response.
type GetProvisionedProductOutputsOutput struct {
	NextPageToken string         `json:"NextPageToken,omitempty"`
	Outputs       []RecordOutput `json:"Outputs"`
}

func recordOutputs(outs []Output) []RecordOutput {
	res := make([]RecordOutput, 0, len(outs))
	for _, o := range outs {
		res = append(res, RecordOutput{Description: o.Description, OutputKey: o.Key, OutputValue: o.Value})
	}

	return res
}

// GetProvisionedProductOutputs returns the stack outputs of a provisioned product.
func (b *InMemoryBackend) GetProvisionedProductOutputs(
	ctx context.Context,
	in *GetProvisionedProductOutputsInput,
) (*GetProvisionedProductOutputsOutput, error) {
	b.mu.RLock("GetProvisionedProductOutputs")
	defer b.mu.RUnlock()

	pp, err := b.provisionedLocked(
		b.callerAccount(ctx),
		b.callerRegion(ctx),
		in.ProvisionedProductID,
		in.ProvisionedProductName,
	)
	if err != nil {
		return nil, err
	}

	var outs []Output

	for _, o := range pp.Outputs {
		if len(in.OutputKeys) == 0 || slices.Contains(in.OutputKeys, o.Key) {
			outs = append(outs, o)
		}
	}

	pg, err := pageOf(recordOutputs(outs), in.Paging)
	if err != nil {
		return nil, err
	}

	return &GetProvisionedProductOutputsOutput{Outputs: pg.Data, NextPageToken: pg.Next}, nil
}

// DescribeRecordInput is the DescribeRecord request.
type DescribeRecordInput struct {
	ID string `json:"Id"`
	Paging
}

// DescribeRecordOutput is the DescribeRecord response.
type DescribeRecordOutput struct {
	RecordDetail  *RecordDetail  `json:"RecordDetail"`
	NextPageToken string         `json:"NextPageToken,omitempty"`
	RecordOutputs []RecordOutput `json:"RecordOutputs"`
}

func (b *InMemoryBackend) recordLocked(account, region, id string) (*Record, error) {
	r, ok := b.records.Get(id)
	if !ok || r.AccountID != account || r.Region != region {
		return nil, notFound("record", id)
	}

	return r, nil
}

// DescribeRecord returns a provisioning record and its outputs.
func (b *InMemoryBackend) DescribeRecord(ctx context.Context, in *DescribeRecordInput) (*DescribeRecordOutput, error) {
	b.mu.RLock("DescribeRecord")
	defer b.mu.RUnlock()

	r, err := b.recordLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	var outs []RecordOutput
	if b.recStatus(r) == recSucceeded {
		outs = recordOutputs(r.Outputs)
	}

	pg, err := pageOf(outs, in.Paging)
	if err != nil {
		return nil, err
	}

	return &DescribeRecordOutput{
		RecordDetail: b.recordDetailLocked(r, false), RecordOutputs: orEmpty(pg.Data), NextPageToken: pg.Next,
	}, nil
}

// ListRecordHistorySearchFilter is the ListRecordHistorySearchFilter shape.
type ListRecordHistorySearchFilter struct {
	Key   string `json:"Key,omitempty"`
	Value string `json:"Value,omitempty"`
}

// ListRecordHistoryInput is the ListRecordHistory request.
type ListRecordHistoryInput struct {
	AccessLevelFilter *AccessLevelFilter             `json:"AccessLevelFilter,omitempty"`
	SearchFilter      *ListRecordHistorySearchFilter `json:"SearchFilter,omitempty"`
	Paging
}

// ListRecordHistoryOutput is the ListRecordHistory response.
type ListRecordHistoryOutput struct {
	NextPageToken string         `json:"NextPageToken,omitempty"`
	RecordDetails []RecordDetail `json:"RecordDetails"`
}

func recordMatches(r *Record, status string, f *ListRecordHistorySearchFilter) (bool, error) {
	if f == nil || f.Key == "" {
		return true, nil
	}

	switch strings.ToLower(f.Key) {
	case "identifier":
		return r.ID == f.Value, nil
	case "provisionedproduct":
		return r.ProvisionedProductID == f.Value || r.ProvisionedProductName == f.Value, nil
	case "product":
		return r.ProductID == f.Value, nil
	case "provisioningartifact":
		return r.ArtifactID == f.Value, nil
	case "operation", "recordtype":
		return r.Type == f.Value, nil
	case queryStatus:
		return status == f.Value, nil
	}

	return false, invalid("SearchFilter Key %q is not supported", f.Key)
}

// ListRecordHistory lists provisioning records, newest first.
func (b *InMemoryBackend) ListRecordHistory(
	ctx context.Context,
	in *ListRecordHistoryInput,
) (*ListRecordHistoryOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("ListRecordHistory")
	defer b.mu.RUnlock()

	var recs []*Record

	for _, r := range b.records.All() {
		if r.AccountID != account || r.Region != region {
			continue
		}

		ok, err := recordMatches(r, b.recStatus(r), in.SearchFilter)
		if err != nil {
			return nil, err
		}

		if ok {
			recs = append(recs, r)
		}
	}

	slices.SortFunc(recs, func(x, y *Record) int {
		if c := y.CreatedTime.Compare(x.CreatedTime); c != 0 {
			return c
		}

		return cmp.Compare(y.ID, x.ID)
	})

	pg, err := pageOf(recs, in.Paging)
	if err != nil {
		return nil, err
	}

	out := make([]RecordDetail, 0, len(pg.Data))
	for _, r := range pg.Data {
		out = append(out, *b.recordDetailLocked(r, false))
	}

	return &ListRecordHistoryOutput{RecordDetails: out, NextPageToken: pg.Next}, nil
}

// EngineWorkflowResult carries the shared members of the Notify*EngineWorkflowResult requests.
type EngineWorkflowResult struct {
	IdempotencyToken string         `json:"IdempotencyToken"`
	RecordID         string         `json:"RecordId"`
	Status           string         `json:"Status"`
	WorkflowToken    string         `json:"WorkflowToken"`
	FailureReason    string         `json:"FailureReason,omitempty"`
	Outputs          []RecordOutput `json:"Outputs,omitempty"`
}

// NotifyProvisionProductEngineWorkflowResultInput is the NotifyProvisionProductEngineWorkflowResult request.
type NotifyProvisionProductEngineWorkflowResultInput struct {
	ResourceIdentifier map[string]any `json:"ResourceIdentifier,omitempty"`
	EngineWorkflowResult
}

// NotifyProvisionProductEngineWorkflowResult completes an externally executed provision workflow.
func (b *InMemoryBackend) NotifyProvisionProductEngineWorkflowResult(
	ctx context.Context,
	in *NotifyProvisionProductEngineWorkflowResultInput,
) (*EmptyOutput, error) {
	return b.notifyEngine(ctx, &in.EngineWorkflowResult, recProvision)
}

// NotifyUpdateProvisionedProductEngineWorkflowResultInput is the Notify...UpdateProvisionedProduct... request.
type NotifyUpdateProvisionedProductEngineWorkflowResultInput struct {
	EngineWorkflowResult
}

// NotifyUpdateProvisionedProductEngineWorkflowResult completes an externally executed update workflow.
func (b *InMemoryBackend) NotifyUpdateProvisionedProductEngineWorkflowResult(
	ctx context.Context,
	in *NotifyUpdateProvisionedProductEngineWorkflowResultInput,
) (*EmptyOutput, error) {
	return b.notifyEngine(ctx, &in.EngineWorkflowResult, recUpdate)
}

// NotifyTerminateProvisionedProductEngineWorkflowResultInput is the Notify...TerminateProvisionedProduct... request.
type NotifyTerminateProvisionedProductEngineWorkflowResultInput struct {
	EngineWorkflowResult
}

// NotifyTerminateProvisionedProductEngineWorkflowResult completes an externally executed terminate workflow.
func (b *InMemoryBackend) NotifyTerminateProvisionedProductEngineWorkflowResult(
	ctx context.Context,
	in *NotifyTerminateProvisionedProductEngineWorkflowResultInput,
) (*EmptyOutput, error) {
	return b.notifyEngine(ctx, &in.EngineWorkflowResult, recTerminate)
}

func (b *InMemoryBackend) notifyEngine(
	ctx context.Context,
	in *EngineWorkflowResult,
	recType string,
) (*EmptyOutput, error) {
	if in.Status != recSucceeded && in.Status != recFailed {
		return nil, invalid("Status must be SUCCEEDED or FAILED")
	}

	b.mu.Lock("notifyEngine")
	defer b.mu.Unlock()

	r, err := b.recordLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.RecordID)
	if err != nil {
		return nil, err
	}

	if r.Type != recType || !r.AwaitingEngine || r.WorkflowToken != in.WorkflowToken {
		return nil, invalid("record %s is not waiting for this engine workflow", in.RecordID)
	}

	pp, ok := b.provisioned.Get(r.ProvisionedProductID)
	if !ok {
		return nil, notFound("provisioned product", r.ProvisionedProductID)
	}

	b.settleLocked(r)

	if in.Status == recFailed {
		r.FinalStatus = recFailed
		r.Errors = []RecordError{
			{Code: "EngineWorkflowFailed", Description: strOr(in.FailureReason, "the engine workflow failed")},
		}
		pp.StatusMessage = r.Errors[0].Description
		pp.Status = ppError

		if recType == recUpdate {
			pp.Status = ppTainted
		}

		return &EmptyOutput{}, nil
	}

	r.FinalStatus = recSucceeded

	if recType == recTerminate {
		b.provisioned.Delete(pp.ID)

		return &EmptyOutput{}, nil
	}

	if len(in.Outputs) > 0 {
		outs := make([]Output, 0, len(in.Outputs))
		for _, o := range in.Outputs {
			outs = append(outs, Output{Key: o.OutputKey, Value: o.OutputValue, Description: o.Description})
		}

		r.Outputs, pp.Outputs = outs, outs
	}

	pp.Status, pp.StatusMessage, pp.LastSuccessfulProvisioningRecordID = statusAvailable, "", r.ID

	return &EmptyOutput{}, nil
}
