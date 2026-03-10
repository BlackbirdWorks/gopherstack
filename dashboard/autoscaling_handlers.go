package dashboard

import (
	"github.com/labstack/echo/v5"

	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// autoscalingIndexData is the template data for the Autoscaling index page.
type autoscalingIndexData struct {
	PageData

	Groups               []autoscalingbackend.AutoScalingGroup
	LaunchConfigurations []autoscalingbackend.LaunchConfiguration
}

func (h *DashboardHandler) autoscalingSnippet() *SnippetData {
	return &SnippetData{
		ID:    "autoscaling-operations",
		Title: "Using Auto Scaling",
		Cli: `# Create a launch configuration
aws autoscaling create-launch-configuration \
  --launch-configuration-name my-lc \
  --image-id ami-12345678 \
  --instance-type t2.micro \
  --endpoint-url http://localhost:8000

# Create an Auto Scaling group
aws autoscaling create-auto-scaling-group \
  --auto-scaling-group-name my-asg \
  --launch-configuration-name my-lc \
  --min-size 1 \
  --max-size 5 \
  --desired-capacity 2 \
  --availability-zones us-east-1a \
  --endpoint-url http://localhost:8000

# Describe Auto Scaling groups
aws autoscaling describe-auto-scaling-groups \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Auto Scaling
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := autoscaling.NewFromConfig(cfg, func(o *autoscaling.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})

// Create an Auto Scaling group
_, err = client.CreateAutoScalingGroup(context.TODO(), &autoscaling.CreateAutoScalingGroupInput{
    AutoScalingGroupName:    aws.String("my-asg"),
    MinSize:                 aws.Int32(1),
    MaxSize:                 aws.Int32(5),
    DesiredCapacity:         aws.Int32(2),
    AvailabilityZones:       []string{"us-east-1a"},
    LaunchConfigurationName: aws.String("my-lc"),
})`,
		Python: `# Initialize boto3 client for Auto Scaling
import boto3

client = boto3.client('autoscaling', endpoint_url='http://localhost:8000')

# Create an Auto Scaling group
client.create_auto_scaling_group(
    AutoScalingGroupName='my-asg',
    LaunchConfigurationName='my-lc',
    MinSize=1,
    MaxSize=5,
    DesiredCapacity=2,
    AvailabilityZones=['us-east-1a'],
)`,
	}
}

// autoscalingIndex handles GET /dashboard/autoscaling.
func (h *DashboardHandler) autoscalingIndex(c *echo.Context) error {
	w := c.Response()

	groups, lcs := h.loadAutoscalingData()

	data := autoscalingIndexData{
		PageData: PageData{
			Title:     "Auto Scaling",
			ActiveTab: "autoscaling",
			Snippet:   h.autoscalingSnippet(),
		},
		Groups:               groups,
		LaunchConfigurations: lcs,
	}

	h.renderTemplate(w, "autoscaling/index.html", data)

	return nil
}

// loadAutoscalingData fetches groups and launch configurations from the backend.
func (h *DashboardHandler) loadAutoscalingData() (
	[]autoscalingbackend.AutoScalingGroup,
	[]autoscalingbackend.LaunchConfiguration,
) {
	groups := []autoscalingbackend.AutoScalingGroup{}
	lcs := []autoscalingbackend.LaunchConfiguration{}

	if h.AutoscalingOps == nil {
		return groups, lcs
	}

	backend, ok := h.AutoscalingOps.Backend.(*autoscalingbackend.InMemoryBackend)
	if !ok {
		return groups, lcs
	}

	if g, _ := backend.DescribeAutoScalingGroups(nil); g != nil {
		groups = g
	}

	if l, _ := backend.DescribeLaunchConfigurations(nil); l != nil {
		lcs = l
	}

	return groups, lcs
}
