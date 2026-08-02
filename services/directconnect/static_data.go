package directconnect

import "fmt"

// seedLocations is a small, defensible stand-in for AWS's real, published
// Direct Connect colocation-facility catalog (DescribeLocations) -- see
// PARITY.md's "static reference-data operations" section: this is NOT the
// authoritative AWS-maintained list, just a handful of plausible entries so
// the op returns something structurally real rather than an empty envelope
// masquerading as "not implemented".
//
//nolint:gochecknoglobals // static reference table, read-only, matches services/outposts' seed_data.go pattern
var seedLocations = []locationWire{
	{
		LocationCode:              "EqDC2",
		LocationName:              "Equinix DC2, Ashburn, VA",
		Region:                    "us-east-1",
		AvailablePortSpeeds:       []string{bandwidth1Gbps, bandwidth10Gbps, bandwidth100Gbps},
		AvailableMacSecPortSpeeds: []string{bandwidth10Gbps, bandwidth100Gbps},
		AvailableProviders:        []string{"Equinix", "Megaport"},
	},
	{
		LocationCode:              "EqSE2",
		LocationName:              "Equinix SE2, Seattle, WA",
		Region:                    "us-west-2",
		AvailablePortSpeeds:       []string{bandwidth1Gbps, bandwidth10Gbps},
		AvailableMacSecPortSpeeds: []string{bandwidth10Gbps},
		AvailableProviders:        []string{"Equinix"},
	},
	{
		LocationCode:              "CS-LON2",
		LocationName:              "CoreSite LON2, London, UK",
		Region:                    "eu-west-2",
		AvailablePortSpeeds:       []string{bandwidth1Gbps, bandwidth10Gbps},
		AvailableMacSecPortSpeeds: []string{bandwidth10Gbps},
		AvailableProviders:        []string{"CoreSite", "Megaport"},
	},
}

// seedRouterTypes is a small, defensible stand-in for AWS's real customer
// router-vendor/OS catalog (DescribeRouterConfiguration) -- see PARITY.md.
// RouterTypeIdentifier values follow the exact doc-comment example format
// ("CiscoSystemsInc-2900SeriesRouters-IOS124").
//
//nolint:gochecknoglobals // static reference table, read-only, matches services/outposts' seed_data.go pattern
var seedRouterTypes = []routerTypeWire{
	{
		Platform:             "2900 Series Routers",
		RouterTypeIdentifier: "CiscoSystemsInc-2900SeriesRouters-IOS124",
		Software:             "IOS 12.4",
		Vendor:               "Cisco Systems, Inc.",
		XsltTemplateName:     "customer-router-cisco-generic.xslt",
	},
	{
		Platform:             "MX Series Routers",
		RouterTypeIdentifier: "JuniperNetworks-MXSeriesRouters-JunOS95",
		Software:             "JunOS 9.5",
		Vendor:               "Juniper Networks",
		XsltTemplateName:     "customer-router-juniper-generic.xslt",
	},
}

// DescribeLocations returns the static Location seed catalog.
func (b *InMemoryBackend) DescribeLocations() []locationWire {
	return seedLocations
}

// DescribeRouterConfiguration returns a placeholder customer router config
// for the named VIF and (optional) router type. See PARITY.md.
func (b *InMemoryBackend) DescribeRouterConfiguration(
	vifID, routerTypeIdentifier string,
) (*routerConfigurationResponse, error) {
	b.mu.RLock("DescribeRouterConfiguration")
	defer b.mu.RUnlock()

	v, ok := b.virtualInterfaces.Get(vifID)
	if !ok {
		return nil, notFoundError(resourceVif, vifID)
	}

	router := seedRouterTypes[0]

	if routerTypeIdentifier != "" {
		found := false

		for _, r := range seedRouterTypes {
			if r.RouterTypeIdentifier == routerTypeIdentifier {
				router = r
				found = true

				break
			}
		}

		if !found {
			return nil, notFoundError("router type", routerTypeIdentifier)
		}
	}

	config := fmt.Sprintf(
		"! GOPHERSTACK PLACEHOLDER CONFIG - NOT REAL - vif %s, vlan %d, %s\n",
		v.VirtualInterfaceID, v.Vlan, router.RouterTypeIdentifier,
	)

	return &routerConfigurationResponse{
		CustomerRouterConfig: config,
		Router:               &router,
		VirtualInterfaceID:   v.VirtualInterfaceID,
		VirtualInterfaceName: v.VirtualInterfaceName,
	}, nil
}

// DescribeCustomerMetadata always returns an empty Agreements list and
// NniPartnerType "nonPartner" -- see PARITY.md's honest-gap section: no
// real Direct Connect partner-agreement workflow is modeled.
func (b *InMemoryBackend) DescribeCustomerMetadata() describeCustomerMetadataResponse {
	return describeCustomerMetadataResponse{
		Agreements:     nil,
		NniPartnerType: NniPartnerTypeNonPartner,
	}
}
