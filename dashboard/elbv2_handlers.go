package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
)

// elbv2LoadBalancerView is the view model for a single ELBv2 load balancer.
type elbv2LoadBalancerView struct {
	Arn     string
	Name    string
	DNSName string
	Type    string
	Scheme  string
}

// elbv2IndexData is the template data for the ELBv2 index page.
type elbv2IndexData struct {
	PageData

	LoadBalancers []elbv2LoadBalancerView
}

// elbv2Snippet returns the shared SnippetData for the ELBv2 dashboard pages.
func elbv2Snippet() *SnippetData {
	return &SnippetData{
		ID:    "elbv2-operations",
		Title: "Using Elastic Load Balancing v2",
		Cli:   `aws elbv2 help --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for ELBv2
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
client := elasticloadbalancingv2.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for ELBv2
import boto3

client = boto3.client('elbv2', endpoint_url='http://localhost:8000')`,
	}
}

// elbv2Index renders the ELBv2 dashboard index.
func (h *DashboardHandler) elbv2Index(c *echo.Context) error {
	w := c.Response()

	if h.ELBv2Ops == nil {
		h.renderTemplate(w, "elbv2/index.html", elbv2IndexData{
			PageData:      PageData{Title: "Elastic Load Balancers v2", ActiveTab: "elbv2", Snippet: elbv2Snippet()},
			LoadBalancers: []elbv2LoadBalancerView{},
		})

		return nil
	}

	lbs, _ := h.ELBv2Ops.Backend.DescribeLoadBalancers(nil, nil)
	views := make([]elbv2LoadBalancerView, 0, len(lbs))

	for _, lb := range lbs {
		views = append(views, elbv2LoadBalancerView{
			Arn:     lb.LoadBalancerArn,
			Name:    lb.LoadBalancerName,
			DNSName: lb.DNSName,
			Type:    lb.Type,
			Scheme:  lb.Scheme,
		})
	}

	h.renderTemplate(w, "elbv2/index.html", elbv2IndexData{
		PageData:      PageData{Title: "Elastic Load Balancers v2", ActiveTab: "elbv2", Snippet: elbv2Snippet()},
		LoadBalancers: views,
	})

	return nil
}

// elbv2CreateLoadBalancer handles POST /dashboard/elbv2/loadbalancer/create.
func (h *DashboardHandler) elbv2CreateLoadBalancer(c *echo.Context) error {
	if h.ELBv2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	lbType := c.Request().FormValue("type")
	if lbType == "" {
		lbType = "application"
	}

	scheme := c.Request().FormValue("scheme")
	if scheme == "" {
		scheme = "internet-facing"
	}

	_, err := h.ELBv2Ops.Backend.CreateLoadBalancer(elbv2backend.CreateLoadBalancerInput{
		Name:   name,
		Type:   lbType,
		Scheme: scheme,
	})
	if err != nil {
		h.Logger.Error("failed to create ELBv2 load balancer", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elbv2")
}

// elbv2DeleteLoadBalancer handles POST /dashboard/elbv2/loadbalancer/delete.
func (h *DashboardHandler) elbv2DeleteLoadBalancer(c *echo.Context) error {
	if h.ELBv2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	lbArn := c.Request().FormValue("arn")
	if lbArn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.ELBv2Ops.Backend.DeleteLoadBalancer(lbArn); err != nil {
		h.Logger.Error("failed to delete ELBv2 load balancer", "arn", lbArn, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elbv2")
}
