package dashboard

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
)

const (
	defaultTTL = int64(300)
)

// route53ZoneView is the view model for a single hosted zone in the index listing.
type route53ZoneView struct {
	Name        string
	ID          string
	RecordCount int
}

// route53IndexData is the template data for the Route 53 index page.
type route53IndexData struct {
	PageData

	Zones []route53ZoneView
}

// route53RecordView is the view model for a single DNS record.
type route53RecordView struct {
	Name   string
	Type   string
	Values string
	TTL    int64
}

// route53ZoneDetailData is the template data for the Route 53 zone detail page.
type route53ZoneDetailData struct {
	PageData

	ZoneID   string
	ZoneName string
	Records  []route53RecordView
}

// route53Index renders the list of all Route 53 hosted zones.
func (h *DashboardHandler) route53Index(c *echo.Context) error {
	w := c.Response()

	if h.Route53Ops == nil {
		h.renderTemplate(w, "route53/index.html", route53IndexData{
			PageData: PageData{Title: "Route 53 Hosted Zones", ActiveTab: "route53",
				Snippet: &SnippetData{
					ID:    "route53-operations",
					Title: "Using Route53",
					Cli:   `aws route53 help --endpoint-url http://localhost:8000`,
					Go: `// Initialize AWS SDK v2 for Using Route53
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
client := route53.NewFromConfig(cfg)`,
					Python: `# Initialize boto3 client for Using Route53
import boto3

client = boto3.client('route53', endpoint_url='http://localhost:8000')`,
				}},
			Zones: []route53ZoneView{},
		})

		return nil
	}

	p, err := h.Route53Ops.Backend.ListHostedZones("", 0)
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}
	zones := p.Data

	views := make([]route53ZoneView, 0, len(zones))

	for _, z := range zones {
		views = append(views, route53ZoneView{
			Name:        z.Name,
			ID:          z.ID,
			RecordCount: z.ResourceRecordSetCount,
		})
	}

	h.renderTemplate(w, "route53/index.html", route53IndexData{
		PageData: PageData{Title: "Route 53 Hosted Zones", ActiveTab: "route53",
			Snippet: &SnippetData{
				ID:    "route53-operations",
				Title: "Using Route53",
				Cli:   `aws route53 help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Route53
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
client := route53.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Route53
import boto3

client = boto3.client('route53', endpoint_url='http://localhost:8000')`,
			}},
		Zones: views,
	})

	return nil
}

// route53ZoneDetail renders the detail page for a single hosted zone.
func (h *DashboardHandler) route53ZoneDetail(c *echo.Context) error {
	w := c.Response()

	if h.Route53Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	zoneID := c.Request().URL.Query().Get("id")

	hz, err := h.Route53Ops.Backend.GetHostedZone(zoneID)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	rrs, err := h.Route53Ops.Backend.ListResourceRecordSets(zoneID)
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	records := make([]route53RecordView, 0, len(rrs))

	for _, r := range rrs {
		values := make([]string, 0, len(r.Records))
		for _, rec := range r.Records {
			values = append(values, rec.Value)
		}

		records = append(records, route53RecordView{
			Name:   r.Name,
			Type:   r.Type,
			TTL:    r.TTL,
			Values: strings.Join(values, ", "),
		})
	}

	h.renderTemplate(w, "route53/zone_detail.html", route53ZoneDetailData{
		PageData: PageData{Title: "Zone: " + hz.Name, ActiveTab: "route53",
			Snippet: &SnippetData{
				ID:    "route53-operations",
				Title: "Using Route53",
				Cli:   `aws route53 help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Route53
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
client := route53.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Route53
import boto3

client = boto3.client('route53', endpoint_url='http://localhost:8000')`,
			}},
		ZoneID:   zoneID,
		ZoneName: hz.Name,
		Records:  records,
	})

	return nil
}

// route53CreateZone handles POST /dashboard/route53/create.
func (h *DashboardHandler) route53CreateZone(c *echo.Context) error {
	if h.Route53Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("zone_name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	callerRef := fmt.Sprintf("dashboard-%s", name)

	if _, err := h.Route53Ops.Backend.CreateHostedZone(name, callerRef, "", false); err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusFound, "/dashboard/route53")
}

// route53DeleteZone handles DELETE /dashboard/route53/delete.
func (h *DashboardHandler) route53DeleteZone(c *echo.Context) error {
	if h.Route53Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	zoneID := c.Request().URL.Query().Get("id")
	if zoneID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.Route53Ops.Backend.DeleteHostedZone(zoneID); err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/route53")
}

// route53CreateRecord handles POST /dashboard/route53/record.
func (h *DashboardHandler) route53CreateRecord(c *echo.Context) error {
	if h.Route53Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	zoneID := c.Request().FormValue("zone_id")
	name := c.Request().FormValue("rec_name")
	recType := c.Request().FormValue("rec_type")
	ttlStr := c.Request().FormValue("rec_ttl")
	value := c.Request().FormValue("rec_value")

	if zoneID == "" || name == "" || recType == "" || value == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ttl := defaultTTL

	if ttlStr != "" {
		if v, err := strconv.ParseInt(ttlStr, 10, 64); err == nil {
			ttl = v
		}
	}

	change := route53backend.Change{
		Action: route53backend.ChangeActionUpsert,
		ResourceRecordSet: route53backend.ResourceRecordSet{
			Name:    name,
			Type:    recType,
			TTL:     ttl,
			Records: []route53backend.ResourceRecord{{Value: value}},
		},
	}

	if err := h.Route53Ops.Backend.ChangeResourceRecordSets(zoneID, []route53backend.Change{change}); err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusFound, "/dashboard/route53/zone?id="+zoneID)
}

// route53DeleteRecord handles DELETE /dashboard/route53/record.
func (h *DashboardHandler) route53DeleteRecord(c *echo.Context) error {
	if h.Route53Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	zoneID := c.Request().URL.Query().Get("zone_id")
	name := c.Request().URL.Query().Get("name")
	recType := c.Request().URL.Query().Get("type")

	if zoneID == "" || name == "" || recType == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	// Look up the existing record to delete it properly
	rrs, err := h.Route53Ops.Backend.ListResourceRecordSets(zoneID)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	target := findResourceRecordSet(rrs, name, recType)

	if target == nil {
		return c.NoContent(http.StatusNotFound)
	}

	change := route53backend.Change{
		Action:            route53backend.ChangeActionDelete,
		ResourceRecordSet: *target,
	}

	changeErr := h.Route53Ops.Backend.ChangeResourceRecordSets(zoneID, []route53backend.Change{change})
	if changeErr != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusFound, "/dashboard/route53/zone?id="+zoneID)
}

// findResourceRecordSet returns the first record matching name and recType, or nil.
func findResourceRecordSet(
	rrs []route53backend.ResourceRecordSet,
	name, recType string,
) *route53backend.ResourceRecordSet {
	for i := range rrs {
		if rrs[i].Name == name && rrs[i].Type == recType {
			return &rrs[i]
		}
	}

	return nil
}

// route53HealthCheckView is the view model for a single health check.
type route53HealthCheckView struct {
	ID     string
	Type   string
	Target string
	Status string
}

// route53HealthCheckIndexData is the template data for the Route 53 health checks index page.
type route53HealthCheckIndexData struct {
	PageData

	HealthChecks []route53HealthCheckView
}

// route53HealthCheckIndex renders the list of all Route 53 health checks.
func (h *DashboardHandler) route53HealthCheckIndex(c *echo.Context) error {
	w := c.Response()

	if h.Route53Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	p, err := h.Route53Ops.Backend.ListHealthChecks("", 0)
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	views := make([]route53HealthCheckView, 0, len(p.Data))

	for _, hc := range p.Data {
		target := hc.Config.IPAddress
		if target == "" {
			target = hc.Config.FullyQualifiedDomainName
		}

		views = append(views, route53HealthCheckView{
			ID:     hc.ID,
			Type:   string(hc.Config.Type),
			Target: target,
			Status: hc.Status,
		})
	}

	h.renderTemplate(w, "route53/healthchecks.html", route53HealthCheckIndexData{
		PageData: PageData{Title: "Route 53 Health Checks", ActiveTab: "route53",
			Snippet: &SnippetData{
				ID:    "route53-healthcheck-operations",
				Title: "Using Route53 Health Checks",
				Cli:   `aws route53 list-health-checks --endpoint-url http://localhost:8000`,
				Go: `// Create a health check
client := route53.NewFromConfig(cfg)
out, err := client.CreateHealthCheck(ctx, &route53.CreateHealthCheckInput{
    CallerReference: aws.String("unique-ref"),
    HealthCheckConfig: &types.HealthCheckConfig{
        Type: types.HealthCheckTypeHttp,
        IPAddress: aws.String("192.0.2.1"),
        Port: aws.Int32(80),
        ResourcePath: aws.String("/health"),
    },
})`,
				Python: `# Create a health check
response = client.create_health_check(
    CallerReference='unique-ref',
    HealthCheckConfig={
        'Type': 'HTTP',
        'IPAddress': '192.0.2.1',
        'Port': 80,
        'ResourcePath': '/health',
    }
)`,
			}},
		HealthChecks: views,
	})

	return nil
}

// route53CreateHealthCheck handles POST /dashboard/route53/healthchecks/create.
func (h *DashboardHandler) route53CreateHealthCheck(c *echo.Context) error {
	if h.Route53Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	hcType := c.Request().FormValue("hc_type")
	ipAddress := c.Request().FormValue("hc_ip")
	fqdn := c.Request().FormValue("hc_fqdn")
	resourcePath := c.Request().FormValue("hc_path")
	callerRef := fmt.Sprintf("dashboard-hc-%s", hcType)

	if hcType == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	cfg := route53backend.HealthCheckConfig{
		Type:                     route53backend.HealthCheckType(hcType),
		IPAddress:                ipAddress,
		FullyQualifiedDomainName: fqdn,
		ResourcePath:             resourcePath,
		RequestInterval:          30, //nolint:mnd // standard Route53 default
		FailureThreshold:         3,  //nolint:mnd // standard Route53 default
	}

	if _, err := h.Route53Ops.Backend.CreateHealthCheck(callerRef, cfg); err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusFound, "/dashboard/route53/healthchecks")
}

// route53DeleteHealthCheck handles DELETE /dashboard/route53/healthchecks/delete.
func (h *DashboardHandler) route53DeleteHealthCheck(c *echo.Context) error {
	if h.Route53Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	hcID := c.Request().URL.Query().Get("id")
	if hcID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.Route53Ops.Backend.DeleteHealthCheck(hcID); err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/route53/healthchecks")
}
