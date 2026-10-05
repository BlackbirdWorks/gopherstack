package omics

import "time"

// sequenceStoreWire is the SDK shape shared by Create/Get/UpdateSequenceStore
// outputs and SequenceStoreDetail (list), using the eTagAlgorithmFamily key.
type sequenceStoreWire struct {
	UpdateTime             *time.Time     `json:"updateTime,omitempty"`
	SseConfig              map[string]any `json:"sseConfig,omitempty"`
	S3Access               map[string]any `json:"s3Access,omitempty"`
	CreationTime           time.Time      `json:"creationTime"`
	Arn                    string         `json:"arn"`
	ID                     string         `json:"id"`
	Name                   string         `json:"name"`
	Description            string         `json:"description,omitempty"`
	ETagAlgorithmFamily    string         `json:"eTagAlgorithmFamily,omitempty"`
	FallbackLocation       string         `json:"fallbackLocation,omitempty"`
	Status                 string         `json:"status"`
	PropagatedSetLevelTags []string       `json:"propagatedSetLevelTags,omitempty"`
}

func newSequenceStoreWire(ss *SequenceStore, withUpdateTime bool) sequenceStoreWire {
	w := sequenceStoreWire{
		CreationTime:           ss.CreationTime,
		SseConfig:              ss.SseConfig,
		S3Access:               ss.S3Access,
		Arn:                    ss.Arn,
		ID:                     ss.ID,
		Name:                   ss.Name,
		Description:            ss.Description,
		ETagAlgorithmFamily:    ss.ETagAlgorithm,
		FallbackLocation:       ss.FallbackLocation,
		Status:                 ss.Status,
		PropagatedSetLevelTags: ss.PropagatedSetLevelTags,
	}
	if withUpdateTime {
		t := ss.UpdateTime
		w.UpdateTime = &t
	}

	return w
}
