package timestreaminfluxdb

import "time"

type emptyResp struct{}

type listIn struct {
	NextToken  string `json:"nextToken"`
	MaxResults int32  `json:"maxResults"`
}

type listOut[T any] struct {
	NextToken string `json:"nextToken,omitempty"`
	Items     []T    `json:"items"`
}

type backupConfigIn struct {
	Type           string `json:"type"`
	CustomSchedule string `json:"customSchedule"`
	RetentionDays  int32  `json:"retentionDays"`
	Enabled        bool   `json:"enabled"`
}

type logDeliveryIn struct {
	S3Configuration *wS3 `json:"s3Configuration"`
}

type maintenanceIn struct {
	PreferredMaintenanceWindow string `json:"preferredMaintenanceWindow"`
	Timezone                   string `json:"timezone"`
}

func backupConfigs(in []backupConfigIn) []BackupConfig {
	out := make([]BackupConfig, 0, len(in))
	for _, c := range in {
		out = append(out, BackupConfig{
			Enabled: c.Enabled, RetentionDays: c.RetentionDays, Type: c.Type, CustomSchedule: c.CustomSchedule,
		})
	}

	return out
}

func logDelivery(in *logDeliveryIn) *LogDelivery {
	if in == nil || in.S3Configuration == nil {
		return nil
	}

	return &LogDelivery{Bucket: in.S3Configuration.BucketName, Enabled: in.S3Configuration.Enabled}
}

func maintenance(in *maintenanceIn) *Maintenance {
	if in == nil {
		return nil
	}

	return &Maintenance{Window: in.PreferredMaintenanceWindow, Timezone: in.Timezone}
}

func parseDateTime(s *string) *time.Time {
	if s == nil || *s == "" {
		return nil
	}

	t, err := time.Parse(time.RFC3339, *s)
	if err != nil {
		return nil
	}

	return &t
}
