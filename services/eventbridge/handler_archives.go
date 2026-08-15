package eventbridge

import (
	"context"
	"encoding/json"
)

type createArchiveOutput struct {
	ArchiveArn   string  `json:"ArchiveArn"`
	State        string  `json:"State"`
	StateReason  string  `json:"StateReason,omitempty"`
	CreationTime float64 `json:"CreationTime"`
}

// archiveResponse is the handler-level DTO for DescribeArchive/UpdateArchive.
// Timestamps are float64 Unix epoch seconds as required by the AWS JSON
// protocol. Real DescribeArchiveOutput (eventbridge@v1.48.4 deserializers.go)
// has ArchiveArn/Description/EventPattern/KmsKeyIdentifier that the real
// plain "Archive" type used by ListArchivesOutput does NOT -- see
// archiveSummary below for that narrower shape.
type archiveResponse struct {
	ArchiveName      string  `json:"ArchiveName"`
	ArchiveArn       string  `json:"ArchiveArn"`
	Description      string  `json:"Description,omitempty"`
	EventPattern     string  `json:"EventPattern,omitempty"`
	EventSourceArn   string  `json:"EventSourceArn"`
	State            string  `json:"State"`
	StateReason      string  `json:"StateReason,omitempty"`
	KmsKeyIdentifier string  `json:"KmsKeyIdentifier,omitempty"`
	CreationTime     float64 `json:"CreationTime"`
	EventCount       int64   `json:"EventCount"`
	RetentionDays    int     `json:"RetentionDays,omitempty"`
	SizeBytes        int64   `json:"SizeBytes"`
}

// archiveSummary is ListArchives' item shape (real "Archive" type,
// deserializers.go's awsAwsjson11_deserializeDocumentArchive case list): no
// ArchiveArn, Description, EventPattern, or KmsKeyIdentifier at all.
type archiveSummary struct {
	ArchiveName    string  `json:"ArchiveName"`
	EventSourceArn string  `json:"EventSourceArn"`
	State          string  `json:"State"`
	StateReason    string  `json:"StateReason,omitempty"`
	CreationTime   float64 `json:"CreationTime"`
	EventCount     int64   `json:"EventCount"`
	RetentionDays  int     `json:"RetentionDays,omitempty"`
	SizeBytes      int64   `json:"SizeBytes"`
}

func archiveToResponse(a *Archive) *archiveResponse {
	if a == nil {
		return nil
	}

	return &archiveResponse{
		CreationTime:     timeToEpochSeconds(a.CreationTime),
		ArchiveName:      a.ArchiveName,
		ArchiveArn:       a.ArchiveArn,
		Description:      a.Description,
		EventPattern:     a.EventPattern,
		EventSourceArn:   a.EventSourceArn,
		State:            a.State,
		StateReason:      a.StateReason,
		KmsKeyIdentifier: a.KmsKeyIdentifier,
		EventCount:       a.EventCount,
		RetentionDays:    a.RetentionDays,
		SizeBytes:        a.SizeBytes,
	}
}

func archiveToSummary(a *Archive) archiveSummary {
	return archiveSummary{
		CreationTime:   timeToEpochSeconds(a.CreationTime),
		ArchiveName:    a.ArchiveName,
		EventSourceArn: a.EventSourceArn,
		State:          a.State,
		StateReason:    a.StateReason,
		EventCount:     a.EventCount,
		RetentionDays:  a.RetentionDays,
		SizeBytes:      a.SizeBytes,
	}
}

// archiveActions returns the CreateArchive action.
func (h *Handler) archiveActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateArchive": func(ctx context.Context, b []byte) (any, error) {
			var input CreateArchiveInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			archive, err := h.Backend.CreateArchive(ctx, input)
			if err != nil {
				return nil, err
			}

			return &createArchiveOutput{
				ArchiveArn:   archive.ArchiveArn,
				CreationTime: timeToEpochSeconds(archive.CreationTime),
				State:        archive.State,
				StateReason:  archive.StateReason,
			}, nil
		},
	}
}

// extendedArchiveActions returns CRUD actions for archives beyond Create.
func (h *Handler) extendedArchiveActions() map[string]actionFn {
	return map[string]actionFn{
		"DeleteArchive": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				ArchiveName string `json:"ArchiveName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeleteArchive(ctx, input.ArchiveName)
		},
		"DescribeArchive": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				ArchiveName string `json:"ArchiveName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			archive, err := h.Backend.DescribeArchive(ctx, input.ArchiveName)
			if err != nil {
				return nil, err
			}

			return archiveToResponse(archive), nil
		},
		"ListArchives": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix     string `json:"NamePrefix"`
				EventSourceArn string `json:"EventSourceArn"`
				State          string `json:"State"`
				NextToken      string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			archives, next, err := h.Backend.ListArchives(
				ctx, input.NamePrefix, input.EventSourceArn, input.State, input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			archiveResponses := make([]archiveSummary, len(archives))
			for i, a := range archives {
				archiveResponses[i] = archiveToSummary(&a)
			}

			return &struct {
				NextToken string           `json:"NextToken,omitempty"`
				Archives  []archiveSummary `json:"Archives"`
			}{Archives: archiveResponses, NextToken: next}, nil
		},
		"UpdateArchive": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateArchiveInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			archive, err := h.Backend.UpdateArchive(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				ArchiveArn   string  `json:"ArchiveArn"`
				State        string  `json:"State"`
				StateReason  string  `json:"StateReason,omitempty"`
				CreationTime float64 `json:"CreationTime"`
			}{
				ArchiveArn:   archive.ArchiveArn,
				CreationTime: timeToEpochSeconds(archive.CreationTime),
				State:        archive.State,
				StateReason:  archive.StateReason,
			}, nil
		},
	}
}
