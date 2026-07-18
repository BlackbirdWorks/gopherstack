package mediapackage

import (
	"maps"
)

type storedIngestEndpoint struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type storedChannel struct {
	Tags                map[string]string      `json:"tags"`
	LifecyclePolicy     *string                `json:"lifecyclePolicy,omitempty"`
	EgressLogGroupName  *string                `json:"egressLogGroupName,omitempty"`
	IngressLogGroupName *string                `json:"ingressLogGroupName,omitempty"`
	ARN                 string                 `json:"arn"`
	ID                  string                 `json:"id"`
	Description         string                 `json:"description"`
	CreatedAt           string                 `json:"createdAt"`
	IngestEndpoints     []storedIngestEndpoint `json:"ingestEndpoints"`
}

func (c *storedChannel) toChannel() *Channel {
	tags := make(map[string]string, len(c.Tags))
	maps.Copy(tags, c.Tags)

	endpoints := make([]*IngestEndpoint, 0, len(c.IngestEndpoints))
	for _, ep := range c.IngestEndpoints {
		endpoints = append(endpoints, &IngestEndpoint{
			ID:       ep.ID,
			URL:      ep.URL,
			Username: ep.Username,
			Password: ep.Password,
		})
	}

	return &Channel{
		ARN:                 c.ARN,
		ID:                  c.ID,
		Description:         c.Description,
		CreatedAt:           c.CreatedAt,
		HlsIngest:           &HlsIngest{IngestEndpoints: endpoints},
		Tags:                tags,
		EgressLogGroupName:  c.EgressLogGroupName,
		IngressLogGroupName: c.IngressLogGroupName,
	}
}

type storedOriginEndpoint struct {
	Authorization          map[string]any    `json:"authorization,omitempty"`
	CmafPackage            map[string]any    `json:"cmafPackage,omitempty"`
	DashPackage            map[string]any    `json:"dashPackage,omitempty"`
	HlsPackage             map[string]any    `json:"hlsPackage,omitempty"`
	MssPackage             map[string]any    `json:"mssPackage,omitempty"`
	Tags                   map[string]string `json:"tags"`
	ARN                    string            `json:"arn"`
	ChannelID              string            `json:"channelId"`
	ID                     string            `json:"id"`
	Description            string            `json:"description"`
	ManifestName           string            `json:"manifestName"`
	URL                    string            `json:"url"`
	Origination            string            `json:"origination"`
	CreatedAt              string            `json:"createdAt"`
	Whitelist              []string          `json:"whitelist"`
	StartoverWindowSeconds int               `json:"startoverWindowSeconds"`
	TimeDelaySeconds       int               `json:"timeDelaySeconds"`
}

func (e *storedOriginEndpoint) toOriginEndpoint() *OriginEndpoint {
	tags := make(map[string]string, len(e.Tags))
	maps.Copy(tags, e.Tags)

	whitelist := make([]string, len(e.Whitelist))
	copy(whitelist, e.Whitelist)

	return &OriginEndpoint{
		ARN:                    e.ARN,
		ChannelID:              e.ChannelID,
		ID:                     e.ID,
		Description:            e.Description,
		ManifestName:           e.ManifestName,
		URL:                    e.URL,
		Origination:            e.Origination,
		CreatedAt:              e.CreatedAt,
		StartoverWindowSeconds: e.StartoverWindowSeconds,
		TimeDelaySeconds:       e.TimeDelaySeconds,
		Whitelist:              whitelist,
		Tags:                   tags,
		Authorization:          copyAnyMap(e.Authorization),
		CmafPackage:            copyAnyMap(e.CmafPackage),
		DashPackage:            copyAnyMap(e.DashPackage),
		HlsPackage:             copyAnyMap(e.HlsPackage),
		MssPackage:             copyAnyMap(e.MssPackage),
	}
}

// copyAnyMap returns a shallow copy of m, or nil if m is empty. Used for the
// opaque packaging-protocol blocks (Authorization/CmafPackage/DashPackage/
// HlsPackage/MssPackage) so callers of toOriginEndpoint cannot mutate the
// backend's stored copy through the returned value.
func copyAnyMap(m map[string]any) map[string]any {
	if len(m) == 0 {
		return nil
	}

	out := make(map[string]any, len(m))
	maps.Copy(out, m)

	return out
}

type storedS3Destination struct {
	BucketName  string `json:"bucketName"`
	ManifestKey string `json:"manifestKey"`
	RoleArn     string `json:"roleArn"`
}

type storedHarvestJob struct {
	S3Destination    *storedS3Destination `json:"s3Destination"`
	ARN              string               `json:"arn"`
	ChannelID        string               `json:"channelId"`
	CreatedAt        string               `json:"createdAt"`
	EndTime          string               `json:"endTime"`
	ID               string               `json:"id"`
	OriginEndpointID string               `json:"originEndpointId"`
	StartTime        string               `json:"startTime"`
	Status           string               `json:"status"`
}

type storedPackagingConfiguration struct {
	Tags             map[string]string `json:"tags"`
	ARN              string            `json:"arn"`
	ID               string            `json:"id"`
	PackagingGroupID string            `json:"packagingGroupId"`
	Description      string            `json:"description"`
	CreatedAt        string            `json:"createdAt"`
}

func (p *storedPackagingConfiguration) toPackagingConfiguration() *PackagingConfiguration {
	tags := make(map[string]string, len(p.Tags))
	maps.Copy(tags, p.Tags)

	return &PackagingConfiguration{
		Tags:             tags,
		ARN:              p.ARN,
		ID:               p.ID,
		PackagingGroupID: p.PackagingGroupID,
		Description:      p.Description,
		CreatedAt:        p.CreatedAt,
	}
}

func (j *storedHarvestJob) toHarvestJob() *HarvestJob {
	var dest *S3Destination
	if j.S3Destination != nil {
		dest = &S3Destination{
			BucketName:  j.S3Destination.BucketName,
			ManifestKey: j.S3Destination.ManifestKey,
			RoleArn:     j.S3Destination.RoleArn,
		}
	}

	return &HarvestJob{
		ARN:              j.ARN,
		ChannelID:        j.ChannelID,
		CreatedAt:        j.CreatedAt,
		EndTime:          j.EndTime,
		ID:               j.ID,
		OriginEndpointID: j.OriginEndpointID,
		S3Destination:    dest,
		StartTime:        j.StartTime,
		Status:           j.Status,
	}
}
