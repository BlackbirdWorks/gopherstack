package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	cwlogsbackend "github.com/blackbirdworks/gopherstack/cloudwatchlogs"
)

func (h *DashboardHandler) cloudWatchLogsIndex(c *echo.Context) error {
	if h.CloudWatchLogsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	groups, _, _ := h.CloudWatchLogsOps.Backend.DescribeLogGroups("", "", 0)
	data := struct {
		PageData

		LogGroups []cwlogsbackend.LogGroup
	}{
		PageData: PageData{Title: "CloudWatch Logs", ActiveTab: "cloudwatchlogs",
			Snippet: &SnippetData{
				ID:    "cloudwatchlogs-operations",
				Title: "Using Cloudwatchlogs",
				Cli:   `aws cloudwatchlogs help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Cloudwatchlogs
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := cloudwatchlogs.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Cloudwatchlogs
import boto3

client = boto3.client('cloudwatchlogs', endpoint_url='http://localhost:8000')`,
			}},
		LogGroups: groups,
	}

	h.renderTemplate(c.Response(), "cloudwatchlogs/index.html", data)

	return nil
}

func (h *DashboardHandler) cloudWatchLogsGroupDetail(c *echo.Context) error {
	if h.CloudWatchLogsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	groupName := c.Request().URL.Query().Get("name")

	streams, _, _ := h.CloudWatchLogsOps.Backend.DescribeLogStreams(groupName, "", "", 0)
	data := struct {
		PageData

		GroupName string
		Streams   []cwlogsbackend.LogStream
	}{
		PageData: PageData{Title: "Log Group: " + groupName, ActiveTab: "cloudwatchlogs",
			Snippet: &SnippetData{
				ID:    "cloudwatchlogs-operations",
				Title: "Using Cloudwatchlogs",
				Cli:   `aws cloudwatchlogs help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Cloudwatchlogs
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := cloudwatchlogs.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Cloudwatchlogs
import boto3

client = boto3.client('cloudwatchlogs', endpoint_url='http://localhost:8000')`,
			}},
		GroupName: groupName,
		Streams:   streams,
	}

	h.renderTemplate(c.Response(), "cloudwatchlogs/group_detail.html", data)

	return nil
}

func (h *DashboardHandler) cloudWatchLogsStreamDetail(c *echo.Context) error {
	if h.CloudWatchLogsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	q := c.Request().URL.Query()
	groupName := q.Get("group")
	streamName := q.Get("stream")
	filter := q.Get("filter")

	const maxEvents = 200

	var events []cwlogsbackend.OutputLogEvent

	if filter != "" {
		evts, _, err := h.CloudWatchLogsOps.Backend.FilterLogEvents(
			groupName, []string{streamName}, filter, nil, nil, maxEvents, "",
		)
		if err != nil {
			h.Logger.Warn("failed to filter log events", "group", groupName, "stream", streamName, "err", err)
		}

		events = evts
	} else {
		evts, _, _, err := h.CloudWatchLogsOps.Backend.GetLogEvents(
			groupName, streamName, nil, nil, maxEvents, "",
		)
		if err != nil {
			h.Logger.Warn("failed to get log events", "group", groupName, "stream", streamName, "err", err)
		}

		events = evts
	}

	data := struct {
		PageData

		GroupName  string
		StreamName string
		Filter     string
		Events     []cwlogsbackend.OutputLogEvent
	}{
		PageData: PageData{Title: "Stream: " + streamName, ActiveTab: "cloudwatchlogs",
			Snippet: &SnippetData{
				ID:    "cloudwatchlogs-operations",
				Title: "Using Cloudwatchlogs",
				Cli:   `aws cloudwatchlogs help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Cloudwatchlogs
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := cloudwatchlogs.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Cloudwatchlogs
import boto3

client = boto3.client('cloudwatchlogs', endpoint_url='http://localhost:8000')`,
			}},
		GroupName:  groupName,
		StreamName: streamName,
		Filter:     filter,
		Events:     events,
	}

	h.renderTemplate(c.Response(), "cloudwatchlogs/stream_detail.html", data)

	return nil
}
