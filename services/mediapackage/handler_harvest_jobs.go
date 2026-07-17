package mediapackage

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- harvest job handlers ---

type s3DestinationOutput struct {
	BucketName  string `json:"bucketName"`
	ManifestKey string `json:"manifestKey"`
	RoleArn     string `json:"roleArn"`
}

type harvestJobOutput struct {
	S3Destination    *s3DestinationOutput `json:"s3Destination"`
	Arn              string               `json:"arn"`
	ChannelID        string               `json:"channelId"`
	CreatedAt        string               `json:"createdAt"`
	EndTime          string               `json:"endTime"`
	ID               string               `json:"id"`
	OriginEndpointID string               `json:"originEndpointId"`
	StartTime        string               `json:"startTime"`
	Status           string               `json:"status"`
}

func toHarvestJobOutput(j *HarvestJob) harvestJobOutput {
	out := harvestJobOutput{
		Arn:              j.ARN,
		ChannelID:        j.ChannelID,
		CreatedAt:        j.CreatedAt,
		EndTime:          j.EndTime,
		ID:               j.ID,
		OriginEndpointID: j.OriginEndpointID,
		StartTime:        j.StartTime,
		Status:           j.Status,
	}

	if j.S3Destination != nil {
		out.S3Destination = &s3DestinationOutput{
			BucketName:  j.S3Destination.BucketName,
			ManifestKey: j.S3Destination.ManifestKey,
			RoleArn:     j.S3Destination.RoleArn,
		}
	}

	return out
}

func (h *Handler) handleCreateHarvestJob(c *echo.Context, body map[string]any) error {
	id, _ := body["id"].(string)
	originEndpointID, _ := body["originEndpointId"].(string)
	startTime, _ := body["startTime"].(string)
	endTime, _ := body["endTime"].(string)

	var s3Dest S3Destination

	if raw, ok := body["s3Destination"].(map[string]any); ok {
		s3Dest.BucketName, _ = raw["bucketName"].(string)
		s3Dest.ManifestKey, _ = raw["manifestKey"].(string)
		s3Dest.RoleArn, _ = raw["roleArn"].(string)
	}

	job, err := h.Backend.CreateHarvestJob(id, originEndpointID, startTime, endTime, s3Dest)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, toHarvestJobOutput(job))
}

func (h *Handler) handleDescribeHarvestJob(c *echo.Context, id string) error {
	job, err := h.Backend.DescribeHarvestJob(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toHarvestJobOutput(job))
}

func (h *Handler) handleListHarvestJobs(c *echo.Context) error {
	includeChannelID := c.QueryParam("includeChannelId")
	includeStatus := c.QueryParam("includeStatus")

	maxResults := parseMediaPkgMaxResults(c.QueryParam("maxResults"))
	jobs, nextToken, err := h.Backend.ListHarvestJobs(
		includeChannelID, includeStatus, maxResults, c.QueryParam("nextToken"),
	)
	if err != nil {
		return h.mapError(c, err)
	}

	out := make([]harvestJobOutput, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, toHarvestJobOutput(j))
	}

	resp := map[string]any{"harvestJobs": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
