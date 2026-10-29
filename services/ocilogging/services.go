package ocilogging

import "github.com/blackbirdworks/gopherstack/pkgs/page"

type categoryWire struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
}

type resourceTypeWire struct {
	Name       string         `json:"name"`
	Categories []categoryWire `json:"categories"`
}

// ServiceSummaryWire is one entry of the ListServices catalog.
type ServiceSummaryWire struct {
	ID                   string             `json:"id"`
	Name                 string             `json:"name"`
	Endpoint             string             `json:"endpoint"`
	ServicePrincipalName string             `json:"servicePrincipalName"`
	TenantID             string             `json:"tenantId"`
	Namespace            string             `json:"namespace,omitempty"`
	ResourceTypes        []resourceTypeWire `json:"resourceTypes"`
}

type catalogService struct {
	name      string
	namespace string
	resource  string
	cats      []categoryWire
}

const (
	svcObjectStorage = "objectstorage"
	svcFunctions     = "functions"
	svcFlowLogs      = "flowlogs"
	svcLoadBalancer  = "loadbalancer"
	svcAPIGateway    = "apigateway"
	svcEvents        = "events"
)

func cat(name, display string) categoryWire { return categoryWire{Name: name, DisplayName: display} }

// catalog lists the OCI services that can emit SERVICE logs; categories follow the Logging service catalog.
func catalog() []catalogService {
	rw := []categoryWire{cat("read", "Read Access Events"), cat("write", "Write Access Events")}
	access := []categoryWire{cat("access", "Access Logs"), cat("error", "Error Logs")}
	gateway := []categoryWire{cat("access", "Access Logs"), cat("execution", "Execution Logs")}

	return []catalogService{
		{svcObjectStorage, svcObjectStorage, "bucket", rw},
		{svcFunctions, svcFunctions, "application", []categoryWire{cat("invoke", "Function Invocation Logs")}},
		{svcFlowLogs, svcFlowLogs, "subnet", []categoryWire{cat("all", "All Records")}},
		{svcLoadBalancer, svcLoadBalancer, "loadbalancer", access},
		{svcAPIGateway, svcAPIGateway, "deployment", gateway},
		{svcEvents, svcEvents, "rule", []categoryWire{cat("ruleexecutionlog", "Rule Execution Log")}},
	}
}

func findService(name, category string) *APIError {
	for _, s := range catalog() {
		if s.name != name {
			continue
		}

		for _, c := range s.cats {
			if c.Name == category {
				return nil
			}
		}

		return errParam("category " + category + " is not a log category of service " + name)
	}

	return errParam("service " + name + " does not support service logs")
}

// ListServices returns the service catalog.
func (b *InMemoryBackend) ListServices(o ListOpts) page.Page[ServiceSummaryWire] {
	cat := catalog()
	out := make([]ServiceSummaryWire, 0, len(cat))

	for _, s := range cat {
		out = append(out, ServiceSummaryWire{
			ID:                   "ocid1.logservice.oc1." + Region + "." + s.name,
			Name:                 s.name,
			Namespace:            s.namespace,
			Endpoint:             "https://" + s.name + "." + Region + ".oci.oraclecloud.com",
			TenantID:             "ocid1.tenancy.oc1..emulator",
			ServicePrincipalName: s.name + ".oci.oc1." + Region + ".oraclecloud.com",
			ResourceTypes:        []resourceTypeWire{{Name: s.resource, Categories: s.cats}},
		})
	}

	return page.New(out, o.Page, o.Limit, defaultListLimit)
}

func (h *Handler) listServices(q *req) error {
	return listReply(q, h.Backend.ListServices(q.listOpts()), nil)
}
