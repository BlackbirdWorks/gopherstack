package dashboard

import (
	"github.com/labstack/echo/v5"
)

// ec2InstanceView is the view model for a single EC2 instance.
type ec2InstanceView struct {
	LaunchTime   string
	ID           string
	State        string
	InstanceType string
	VPCID        string
}

// ec2SecurityGroupView is the view model for a security group.
type ec2SecurityGroupView struct {
	ID          string
	Name        string
	VPCID       string
	Description string
}

// ec2SubnetView is the view model for a subnet in a VPC tree.
type ec2SubnetView struct {
	ID               string
	CIDRBlock        string
	AvailabilityZone string
	IsDefault        bool
}

// ec2VPCView is the view model for a VPC with its subnets.
type ec2VPCView struct {
	ID        string
	CIDRBlock string
	Subnets   []ec2SubnetView
	IsDefault bool
}

// ec2IndexData is the template data for the EC2 dashboard index page.
type ec2IndexData struct {
	PageData

	Instances      []ec2InstanceView
	SecurityGroups []ec2SecurityGroupView
	VPCs           []ec2VPCView
}

// ec2Index renders the EC2 dashboard page.
//
//nolint:funlen // long due to EC2 instance type list
func (h *DashboardHandler) ec2Index(c *echo.Context) error {
	w := c.Response()

	if h.EC2Ops == nil {
		h.renderTemplate(w, "ec2/index.html", ec2IndexData{
			PageData: PageData{Title: "EC2", ActiveTab: "ec2",
				Snippet: &SnippetData{
					ID:    "ec2-operations",
					Title: "Using Ec2",
					Cli:   `aws ec2 help --endpoint-url http://localhost:8000`,
					Go: `// Initialize AWS SDK v2 for Using Ec2
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
client := ec2.NewFromConfig(cfg)`,
					Python: `# Initialize boto3 client for Using Ec2
import boto3

client = boto3.client('ec2', endpoint_url='http://localhost:8000')`,
				}},
			Instances:      []ec2InstanceView{},
			SecurityGroups: []ec2SecurityGroupView{},
			VPCs:           []ec2VPCView{},
		})

		return nil
	}

	instances := h.EC2Ops.Backend.DescribeInstances(nil, "")
	instanceViews := make([]ec2InstanceView, 0, len(instances))

	for _, inst := range instances {
		instanceViews = append(instanceViews, ec2InstanceView{
			ID:           inst.ID,
			State:        inst.State.Name,
			InstanceType: inst.InstanceType,
			LaunchTime:   inst.LaunchTime.Format("2006-01-02 15:04:05"),
			VPCID:        inst.VPCID,
		})
	}

	sgs := h.EC2Ops.Backend.DescribeSecurityGroups(nil)
	sgViews := make([]ec2SecurityGroupView, 0, len(sgs))

	for _, sg := range sgs {
		sgViews = append(sgViews, ec2SecurityGroupView{
			ID:          sg.ID,
			Name:        sg.Name,
			VPCID:       sg.VPCID,
			Description: sg.Description,
		})
	}

	vpcs := h.EC2Ops.Backend.DescribeVpcs(nil)
	subnets := h.EC2Ops.Backend.DescribeSubnets(nil)

	// Build subnet lookup by VPC ID.
	subnetsByVPC := make(map[string][]ec2SubnetView)
	for _, s := range subnets {
		subnetsByVPC[s.VPCID] = append(subnetsByVPC[s.VPCID], ec2SubnetView{
			ID:               s.ID,
			CIDRBlock:        s.CIDRBlock,
			AvailabilityZone: s.AvailabilityZone,
			IsDefault:        s.IsDefault,
		})
	}

	vpcViews := make([]ec2VPCView, 0, len(vpcs))
	for _, v := range vpcs {
		vpcViews = append(vpcViews, ec2VPCView{
			ID:        v.ID,
			CIDRBlock: v.CIDRBlock,
			IsDefault: v.IsDefault,
			Subnets:   subnetsByVPC[v.ID],
		})
	}

	h.renderTemplate(w, "ec2/index.html", ec2IndexData{
		PageData: PageData{Title: "EC2", ActiveTab: "ec2",
			Snippet: &SnippetData{
				ID:    "ec2-operations",
				Title: "Using Ec2",
				Cli:   `aws ec2 help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Ec2
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
client := ec2.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Ec2
import boto3

client = boto3.client('ec2', endpoint_url='http://localhost:8000')`,
			}},
		Instances:      instanceViews,
		SecurityGroups: sgViews,
		VPCs:           vpcViews,
	})

	return nil
}
