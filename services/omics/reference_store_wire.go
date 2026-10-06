package omics

import "time"

// referenceStoreWire is the SDK shape of Create/GetReferenceStore and ReferenceStoreDetail.
type referenceStoreWire struct {
	CreationTime time.Time      `json:"creationTime"`
	SseConfig    map[string]any `json:"sseConfig,omitempty"`
	Arn          string         `json:"arn"`
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Description  string         `json:"description,omitempty"`
}

func newReferenceStoreWire(rs *ReferenceStore) referenceStoreWire {
	return referenceStoreWire{
		CreationTime: rs.CreationTime,
		SseConfig:    rs.SseConfig,
		Arn:          rs.Arn,
		ID:           rs.ID,
		Name:         rs.Name,
		Description:  rs.Description,
	}
}
