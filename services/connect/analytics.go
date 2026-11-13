package connect

import (
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	labelAnalyticsAssoc = "AnalyticsDataAssociation"
	maxDataSetIDLen     = 255
	maxAnalyticsPage    = 1000
	shareStatusActive   = "ACTIVE"
)

var accountIDPattern = regexp.MustCompile(`^\d{12}$`)

type analyticsAssocRec struct {
	InstanceID       string `json:"instanceId"`
	DataSetID        string `json:"dataSetId"`
	TargetAccountID  string `json:"targetAccountId"`
	ResourceShareID  string `json:"resourceShareId"`
	ResourceShareArn string `json:"resourceShareArn"`
}

// AnalyticsDataAssociationResult is types.AnalyticsDataAssociationResult.
type AnalyticsDataAssociationResult struct {
	DataSetID           string `json:"DataSetId"`
	ResourceShareArn    string `json:"ResourceShareArn"`
	ResourceShareID     string `json:"ResourceShareId"`
	ResourceShareStatus string `json:"ResourceShareStatus"`
	TargetAccountID     string `json:"TargetAccountId"`
}

// ErrorResult is types.ErrorResult.
type ErrorResult struct {
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

// AssociateAnalyticsDataSetInput is the AssociateAnalyticsDataSet and DisassociateAnalyticsDataSet request.
type AssociateAnalyticsDataSetInput struct {
	DataSetID       string `json:"DataSetId"`
	TargetAccountID string `json:"TargetAccountId"`
	instPath
}

// BatchAnalyticsDataSetInput is the BatchAssociateAnalyticsDataSet and BatchDisassociateAnalyticsDataSet request.
type BatchAnalyticsDataSetInput struct {
	TargetAccountID string `json:"TargetAccountId"`
	instPath
	DataSetIDs []string `json:"DataSetIds"`
}

// ListAnalyticsDataAssociationsInput is the ListAnalyticsDataAssociations request.
type ListAnalyticsDataAssociationsInput struct {
	DataSetID string `json:"-" query:"DataSetId"`
	NextToken string `json:"-" query:"nextToken"`
	instPath
	MaxResults int32 `json:"-" query:"maxResults"`
}

type batchAssociateOut struct {
	Created []AnalyticsDataAssociationResult `json:"Created"`
	Errors  []ErrorResult                    `json:"Errors"`
}

type batchDisassociateOut struct {
	Deleted []string      `json:"Deleted"`
	Errors  []ErrorResult `json:"Errors"`
}

func analyticsKey(dataSet, target string) string { return target + "/" + dataSet }

func (r *analyticsAssocRec) view() AnalyticsDataAssociationResult {
	return AnalyticsDataAssociationResult{
		DataSetID: r.DataSetID, ResourceShareArn: r.ResourceShareArn, ResourceShareID: r.ResourceShareID,
		ResourceShareStatus: shareStatusActive, TargetAccountID: r.TargetAccountID,
	}
}

func checkDataSetID(id string) error {
	if id == "" || len(id) > maxDataSetIDLen {
		return invalidParamErr("DataSetId must be 1-%d characters", maxDataSetIDLen)
	}

	return nil
}

func (b *Backend) analyticsTarget(target string) (string, error) {
	if target == "" {
		return b.accountID, nil
	}

	if !accountIDPattern.MatchString(target) {
		return "", invalidParamErr("TargetAccountId must be a 12-digit account ID")
	}

	return target, nil
}

func (b *Backend) associateLocked(inst, dataSet, target string) AnalyticsDataAssociationResult {
	if r, err := b.analyticsAssocs.get(inst, analyticsKey(dataSet, target)); err == nil {
		return r.view()
	}

	id := newID()
	rec := &analyticsAssocRec{
		InstanceID: inst, DataSetID: dataSet, TargetAccountID: target, ResourceShareID: id,
		ResourceShareArn: arn.Build("ram", b.region, b.accountID, "resource-share/"+id),
	}
	b.analyticsAssocs.put(rec)

	return rec.view()
}

// AssociateAnalyticsDataSet shares one analytics dataset with the target account.
func (b *Backend) AssociateAnalyticsDataSet(
	in *AssociateAnalyticsDataSetInput,
) (*AnalyticsDataAssociationResult, error) {
	if err := checkDataSetID(in.DataSetID); err != nil {
		return nil, err
	}

	target, err := b.analyticsTarget(in.TargetAccountID)
	if err != nil {
		return nil, err
	}

	return writeInstance(
		b,
		"AssociateAnalyticsDataSet",
		in.InstanceID,
		func(inst string) (*AnalyticsDataAssociationResult, error) {
			out := b.associateLocked(inst, in.DataSetID, target)

			return &out, nil
		},
	)
}

// BatchAssociateAnalyticsDataSet shares several analytics datasets; an invalid dataset ID fails alone.
func (b *Backend) BatchAssociateAnalyticsDataSet(in *BatchAnalyticsDataSetInput) (*batchAssociateOut, error) {
	if len(in.DataSetIDs) == 0 {
		return nil, invalidParamErr("DataSetIds must contain at least one dataset ID")
	}

	target, err := b.analyticsTarget(in.TargetAccountID)
	if err != nil {
		return nil, err
	}

	return writeInstance(
		b,
		"BatchAssociateAnalyticsDataSet",
		in.InstanceID,
		func(inst string) (*batchAssociateOut, error) {
			out := &batchAssociateOut{Created: []AnalyticsDataAssociationResult{}, Errors: []ErrorResult{}}

			for _, id := range in.DataSetIDs {
				if cerr := checkDataSetID(id); cerr != nil {
					out.Errors = append(
						out.Errors,
						ErrorResult{ErrorCode: "InvalidParameterException", ErrorMessage: cerr.Error()},
					)

					continue
				}

				out.Created = append(out.Created, b.associateLocked(inst, id, target))
			}

			return out, nil
		},
	)
}

// DisassociateAnalyticsDataSet removes the sharing of one dataset.
func (b *Backend) DisassociateAnalyticsDataSet(in *AssociateAnalyticsDataSetInput) (*empty, error) {
	if err := checkDataSetID(in.DataSetID); err != nil {
		return nil, err
	}

	target, err := b.analyticsTarget(in.TargetAccountID)
	if err != nil {
		return nil, err
	}

	return writeInstance(b, "DisassociateAnalyticsDataSet", in.InstanceID, func(inst string) (*empty, error) {
		return &empty{}, b.analyticsAssocs.del(inst, analyticsKey(in.DataSetID, target))
	})
}

// BatchDisassociateAnalyticsDataSet removes the sharing of several datasets; unknown ones fail alone.
func (b *Backend) BatchDisassociateAnalyticsDataSet(in *BatchAnalyticsDataSetInput) (*batchDisassociateOut, error) {
	if len(in.DataSetIDs) == 0 {
		return nil, invalidParamErr("DataSetIds must contain at least one dataset ID")
	}

	target, err := b.analyticsTarget(in.TargetAccountID)
	if err != nil {
		return nil, err
	}

	return writeInstance(
		b,
		"BatchDisassociateAnalyticsDataSet",
		in.InstanceID,
		func(inst string) (*batchDisassociateOut, error) {
			out := &batchDisassociateOut{Deleted: []string{}, Errors: []ErrorResult{}}

			for _, id := range in.DataSetIDs {
				if derr := b.analyticsAssocs.del(inst, analyticsKey(id, target)); derr != nil {
					out.Errors = append(
						out.Errors,
						ErrorResult{ErrorCode: "ResourceNotFoundException", ErrorMessage: derr.Error()},
					)

					continue
				}

				out.Deleted = append(out.Deleted, id)
			}

			return out, nil
		},
	)
}

// ListAnalyticsDataAssociations lists the dataset associations of an instance, optionally for one dataset.
func (b *Backend) ListAnalyticsDataAssociations(in *ListAnalyticsDataAssociationsInput) (*map[string]any, error) {
	if in.MaxResults > maxAnalyticsPage {
		return nil, invalidParamErr("maxResults must be 1-%d", maxAnalyticsPage)
	}

	all, err := readInstance(
		b,
		"ListAnalyticsDataAssociations",
		in.InstanceID,
		func(inst string) ([]AnalyticsDataAssociationResult, error) {
			recs := slices.DeleteFunc(b.analyticsAssocs.in(inst), func(r *analyticsAssocRec) bool {
				return in.DataSetID != "" && r.DataSetID != in.DataSetID
			})
			slices.SortFunc(recs, func(a, c *analyticsAssocRec) int {
				return strings.Compare(
					analyticsKey(a.DataSetID, a.TargetAccountID),
					analyticsKey(c.DataSetID, c.TargetAccountID),
				)
			})

			out := make([]AnalyticsDataAssociationResult, 0, len(recs))
			for _, r := range recs {
				out = append(out, r.view())
			}

			return out, nil
		},
	)
	if err != nil {
		return nil, err
	}

	items, next, err := paged(all, in.NextToken, in.MaxResults)

	return listBody("Results", orEmpty(items), next), err
}
