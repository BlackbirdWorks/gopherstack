package models

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

// VectorAttributeDefinition mirrors types.VectorAttributeDefinition.
type VectorAttributeDefinition struct {
	AttributeName string `json:"AttributeName"`
}

// SearchSchemaElement mirrors types.SearchSchemaElement.
type SearchSchemaElement struct {
	AttributeName           string `json:"AttributeName"`
	SearchSchemaElementType string `json:"SearchSchemaElementType"`
}

// VectorIndex mirrors types.VectorIndex and types.CreateVectorIndexAction.
type VectorIndex struct {
	VectorAttribute  *VectorAttributeDefinition `json:"VectorAttribute,omitempty"`
	Projection       *Projection                `json:"Projection,omitempty"`
	IndexName        string                     `json:"IndexName"`
	DistanceFunction string                     `json:"DistanceFunction"`
	SearchSchema     []SearchSchemaElement      `json:"SearchSchema,omitempty"`
	Dimensions       int64                      `json:"Dimensions"`
}

// DeleteVectorIndexAction mirrors types.DeleteVectorIndexAction.
type DeleteVectorIndexAction struct {
	IndexName string `json:"IndexName"`
}

// VectorIndexUpdate mirrors types.VectorIndexUpdate.
type VectorIndexUpdate struct {
	Create *VectorIndex             `json:"Create,omitempty"`
	Delete *DeleteVectorIndexAction `json:"Delete,omitempty"`
}

// VectorIndexDescription mirrors types.VectorIndexDescription.
type VectorIndexDescription struct {
	VectorAttribute  *VectorAttributeDefinition `json:"VectorAttribute,omitempty"`
	Projection       *Projection                `json:"Projection,omitempty"`
	IndexName        string                     `json:"IndexName"`
	IndexArn         string                     `json:"IndexArn,omitempty"`
	IndexStatus      string                     `json:"IndexStatus,omitempty"`
	DistanceFunction string                     `json:"DistanceFunction,omitempty"`
	SearchSchema     []SearchSchemaElement      `json:"SearchSchema,omitempty"`
	Dimensions       int64                      `json:"Dimensions,omitempty"`
	ItemCount        int64                      `json:"ItemCount"`
	IndexSizeBytes   int64                      `json:"IndexSizeBytes"`
	Backfilling      bool                       `json:"Backfilling"`
}

func toSDKVectorAttribute(v *VectorAttributeDefinition) *types.VectorAttributeDefinition {
	if v == nil {
		return nil
	}

	return &types.VectorAttributeDefinition{AttributeName: ptrconv.NilIfEmpty(v.AttributeName)}
}

func fromSDKVectorAttribute(v *types.VectorAttributeDefinition) *VectorAttributeDefinition {
	if v == nil {
		return nil
	}

	return &VectorAttributeDefinition{AttributeName: ptrconv.String(v.AttributeName)}
}

func toSDKSearchSchema(in []SearchSchemaElement) []types.SearchSchemaElement {
	if in == nil {
		return nil
	}

	out := make([]types.SearchSchemaElement, len(in))
	for i, e := range in {
		out[i] = types.SearchSchemaElement{
			AttributeName:           ptrconv.NilIfEmpty(e.AttributeName),
			SearchSchemaElementType: types.SearchSchemaElementType(e.SearchSchemaElementType),
		}
	}

	return out
}

func fromSDKSearchSchema(in []types.SearchSchemaElement) []SearchSchemaElement {
	if in == nil {
		return nil
	}

	out := make([]SearchSchemaElement, len(in))
	for i, e := range in {
		out[i] = SearchSchemaElement{
			AttributeName:           ptrconv.String(e.AttributeName),
			SearchSchemaElementType: string(e.SearchSchemaElementType),
		}
	}

	return out
}

func optionalProjection(p *types.Projection) *Projection {
	if p == nil {
		return nil
	}

	proj := FromSDKProjection(p)

	return &proj
}

func optionalSDKProjection(p *Projection) *types.Projection {
	if p == nil {
		return nil
	}

	return ToSDKProjection(*p)
}

// ToSDKVectorIndexes converts wire vector index definitions, preserving nil versus empty.
func ToSDKVectorIndexes(in []VectorIndex) []types.VectorIndex {
	if in == nil {
		return nil
	}

	out := make([]types.VectorIndex, len(in))
	for i, v := range in {
		out[i] = types.VectorIndex{
			IndexName:        ptrconv.NilIfEmpty(v.IndexName),
			VectorAttribute:  toSDKVectorAttribute(v.VectorAttribute),
			Dimensions:       &v.Dimensions,
			DistanceFunction: types.VectorDistanceFunction(v.DistanceFunction),
			Projection:       optionalSDKProjection(v.Projection),
			SearchSchema:     toSDKSearchSchema(v.SearchSchema),
		}
	}

	return out
}

// FromSDKVectorIndexes converts SDK vector index definitions to the wire form.
func FromSDKVectorIndexes(in []types.VectorIndex) []VectorIndex {
	if in == nil {
		return nil
	}

	out := make([]VectorIndex, len(in))
	for i, v := range in {
		out[i] = VectorIndex{
			IndexName:        ptrconv.String(v.IndexName),
			VectorAttribute:  fromSDKVectorAttribute(v.VectorAttribute),
			Dimensions:       ptrconv.Int64(v.Dimensions),
			DistanceFunction: string(v.DistanceFunction),
			Projection:       optionalProjection(v.Projection),
			SearchSchema:     fromSDKSearchSchema(v.SearchSchema),
		}
	}

	return out
}

// FromSDKCreateVectorIndexAction converts an UpdateTable create action.
func FromSDKCreateVectorIndexAction(a *types.CreateVectorIndexAction) *VectorIndex {
	if a == nil {
		return nil
	}

	return &VectorIndex{
		IndexName:        ptrconv.String(a.IndexName),
		VectorAttribute:  fromSDKVectorAttribute(a.VectorAttribute),
		Dimensions:       ptrconv.Int64(a.Dimensions),
		DistanceFunction: string(a.DistanceFunction),
		Projection:       optionalProjection(a.Projection),
		SearchSchema:     fromSDKSearchSchema(a.SearchSchema),
	}
}

func toSDKVectorIndexUpdates(in []VectorIndexUpdate) []types.VectorIndexUpdate {
	if len(in) == 0 {
		return nil
	}

	out := make([]types.VectorIndexUpdate, len(in))
	for i, u := range in {
		if u.Create != nil {
			out[i].Create = &types.CreateVectorIndexAction{
				IndexName:        ptrconv.NilIfEmpty(u.Create.IndexName),
				VectorAttribute:  toSDKVectorAttribute(u.Create.VectorAttribute),
				Dimensions:       &u.Create.Dimensions,
				DistanceFunction: types.VectorDistanceFunction(u.Create.DistanceFunction),
				Projection:       optionalSDKProjection(u.Create.Projection),
				SearchSchema:     toSDKSearchSchema(u.Create.SearchSchema),
			}
		}

		if u.Delete != nil {
			out[i].Delete = &types.DeleteVectorIndexAction{IndexName: ptrconv.NilIfEmpty(u.Delete.IndexName)}
		}
	}

	return out
}

// ToSDKVectorIndexDescriptions converts stored descriptions to the SDK type.
func ToSDKVectorIndexDescriptions(in []VectorIndexDescription) []types.VectorIndexDescription {
	if len(in) == 0 {
		return nil
	}

	out := make([]types.VectorIndexDescription, len(in))
	for i, d := range in {
		out[i] = types.VectorIndexDescription{
			IndexName:        ptrconv.NilIfEmpty(d.IndexName),
			IndexArn:         ptrconv.NilIfEmpty(d.IndexArn),
			IndexStatus:      types.IndexStatus(d.IndexStatus),
			Backfilling:      &d.Backfilling,
			Dimensions:       &d.Dimensions,
			DistanceFunction: types.VectorDistanceFunction(d.DistanceFunction),
			VectorAttribute:  toSDKVectorAttribute(d.VectorAttribute),
			Projection:       optionalSDKProjection(d.Projection),
			SearchSchema:     toSDKSearchSchema(d.SearchSchema),
			ItemCount:        &d.ItemCount,
			IndexSizeBytes:   &d.IndexSizeBytes,
		}
	}

	return out
}

// FromSDKVectorIndexDescriptions converts SDK descriptions to the wire form.
func FromSDKVectorIndexDescriptions(in []types.VectorIndexDescription) []VectorIndexDescription {
	if len(in) == 0 {
		return nil
	}

	out := make([]VectorIndexDescription, len(in))
	for i, d := range in {
		out[i] = VectorIndexDescription{
			IndexName:        ptrconv.String(d.IndexName),
			IndexArn:         ptrconv.String(d.IndexArn),
			IndexStatus:      string(d.IndexStatus),
			Backfilling:      ptrconv.Bool(d.Backfilling),
			Dimensions:       ptrconv.Int64(d.Dimensions),
			DistanceFunction: string(d.DistanceFunction),
			VectorAttribute:  fromSDKVectorAttribute(d.VectorAttribute),
			Projection:       optionalProjection(d.Projection),
			SearchSchema:     fromSDKSearchSchema(d.SearchSchema),
			ItemCount:        ptrconv.Int64(d.ItemCount),
			IndexSizeBytes:   ptrconv.Int64(d.IndexSizeBytes),
		}
	}

	return out
}
