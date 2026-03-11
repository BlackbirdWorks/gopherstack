package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
)

// elbLoadBalancerView is the view model for a single Classic ELB load balancer.
type elbLoadBalancerView struct {
	Name    string
	DNSName string
}

// elbIndexData is the template data for the ELB index page.
type elbIndexData struct {
	PageData

	LoadBalancers []elbLoadBalancerView
}

// elbSnippet returns the shared SnippetData for the ELB dashboard pages.
func elbSnippet() *SnippetData {
	return &SnippetData{
		ID:    "elb-operations",
		Title: "Using Elastic Load Balancing",
		Cli:   `aws elb help --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Classic ELB
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
client := elasticloadbalancing.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Classic ELB
import boto3

client = boto3.client('elb', endpoint_url='http://localhost:8000')`,
	}
}

// elbIndex renders the ELB dashboard index.
func (h *DashboardHandler) elbIndex(c *echo.Context) error {
	w := c.Response()

	if h.ELBOps == nil {
		h.renderTemplate(w, "elb/index.html", elbIndexData{
			PageData:      PageData{Title: "Elastic Load Balancers", ActiveTab: "elb", Snippet: elbSnippet()},
			LoadBalancers: []elbLoadBalancerView{},
		})

		return nil
	}

	lbs, _ := h.ELBOps.Backend.DescribeLoadBalancers(nil)
	views := make([]elbLoadBalancerView, 0, len(lbs))

	for _, lb := range lbs {
		views = append(views, elbLoadBalancerView{
			Name:    lb.LoadBalancerName,
			DNSName: lb.DNSName,
		})
	}

	h.renderTemplate(w, "elb/index.html", elbIndexData{
		PageData:      PageData{Title: "Elastic Load Balancers", ActiveTab: "elb", Snippet: elbSnippet()},
		LoadBalancers: views,
	})

	return nil
}

// elbCreateLoadBalancer handles POST /dashboard/elb/loadbalancer/create.
func (h *DashboardHandler) elbCreateLoadBalancer(c *echo.Context) error {
	if h.ELBOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.ELBOps.Backend.CreateLoadBalancer(elbbackend.CreateLoadBalancerInput{
		LoadBalancerName: name,
		Scheme:           "internet-facing",
	})
	if err != nil {
		h.Logger.Error("failed to create load balancer", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elb")
}

// elbDeleteLoadBalancer handles POST /dashboard/elb/loadbalancer/delete.
func (h *DashboardHandler) elbDeleteLoadBalancer(c *echo.Context) error {
	if h.ELBOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.ELBOps.Backend.DeleteLoadBalancer(name); err != nil {
		h.Logger.Error("failed to delete load balancer", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elb")
}
