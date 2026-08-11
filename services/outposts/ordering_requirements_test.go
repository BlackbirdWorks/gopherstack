package outposts

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fullAddress(countryCode, postalCode string) *Address {
	return &Address{
		AddressLine1:       "123 Main St",
		City:               "Seattle",
		ContactName:        "Jane Doe",
		ContactPhoneNumber: "+12065550100",
		CountryCode:        countryCode,
		PostalCode:         postalCode,
		StateOrRegion:      "WA",
	}
}

func completeRackProps() *RackPhysicalProperties {
	return &RackPhysicalProperties{
		FiberOpticCableType:       "SINGLE_MODE",
		MaximumSupportedWeightLbs: "NO_LIMIT",
		OpticalStandard:           "OPTIC_10GBASE_SR",
		PowerConnector:            "L6_30P",
		PowerDrawKva:              "POWER_5_KVA",
		PowerFeedDrop:             "ABOVE_RACK",
		PowerPhase:                "SINGLE_PHASE",
		UplinkCount:               "UPLINK_COUNT_1",
		UplinkGbps:                "UPLINK_1G",
	}
}

func requirementStatuses(reqs []OrderingRequirement) map[string]string {
	got := make(map[string]string, len(reqs))
	for _, r := range reqs {
		got[r.OrderingRequirementType] = r.Status
	}

	return got
}

func TestBuildOrderingRequirements(t *testing.T) {
	t.Parallel()

	activeOutpost := &Outpost{ID: "op-1", LifeCycleStatus: LifeCycleStatusActive}
	inactiveOutpost := &Outpost{ID: "op-1", LifeCycleStatus: LifeCycleStatusPendingDecommission}
	rackOutpost := &Outpost{
		ID:                    "op-2",
		LifeCycleStatus:       LifeCycleStatusActive,
		SupportedHardwareType: HardwareTypeRack,
	}
	serverOutpost := &Outpost{
		ID:                    "op-3",
		LifeCycleStatus:       LifeCycleStatusActive,
		SupportedHardwareType: HardwareTypeServer,
	}
	expiredContractOutpost := &Outpost{
		ID: "op-4", LifeCycleStatus: LifeCycleStatusActive,
		ContractEndDate: time.Now().Add(-24 * time.Hour),
	}
	activeContractOutpost := &Outpost{
		ID: "op-5", LifeCycleStatus: LifeCycleStatusActive,
		ContractEndDate: time.Now().Add(24 * time.Hour),
	}

	tests := []struct {
		outpost     *Outpost
		site        *Site
		outpostID   string
		countryCode string
		want        map[string]string
		name        string
	}{
		{
			name: "no outpost associated",
			want: map[string]string{
				OrderingRequirementTypeOutpostIDMissing: OrderingRequirementStatusFail,
				OrderingRequirementTypeOutpostNotFound:  OrderingRequirementStatusExempt,
				OrderingRequirementTypeOutpostActive:    OrderingRequirementStatusExempt,
			},
		},
		{
			name:      "outpost id set but no longer resolves",
			outpostID: "op-deleted",
			outpost:   nil,
			want: map[string]string{
				OrderingRequirementTypeOutpostIDMissing: OrderingRequirementStatusPass,
				OrderingRequirementTypeOutpostNotFound:  OrderingRequirementStatusFail,
				OrderingRequirementTypeOutpostActive:    OrderingRequirementStatusExempt,
			},
		},
		{
			name:      "active outpost, no site",
			outpostID: activeOutpost.ID,
			outpost:   activeOutpost,
			want: map[string]string{
				OrderingRequirementTypeOutpostIDMissing:          OrderingRequirementStatusPass,
				OrderingRequirementTypeOutpostNotFound:           OrderingRequirementStatusPass,
				OrderingRequirementTypeOutpostActive:             OrderingRequirementStatusPass,
				OrderingRequirementTypeOutpostRenewalRequired:    OrderingRequirementStatusExempt,
				OrderingRequirementTypeOperatingAddressExistence: OrderingRequirementStatusExempt,
				OrderingRequirementTypeShippingAddressExistence:  OrderingRequirementStatusExempt,
			},
		},
		{
			name:      "inactive outpost",
			outpostID: inactiveOutpost.ID,
			outpost:   inactiveOutpost,
			want: map[string]string{
				OrderingRequirementTypeOutpostActive: OrderingRequirementStatusFail,
			},
		},
		{
			name:      "expired contract needs renewal",
			outpostID: expiredContractOutpost.ID,
			outpost:   expiredContractOutpost,
			want: map[string]string{
				OrderingRequirementTypeOutpostRenewalRequired: OrderingRequirementStatusFail,
			},
		},
		{
			name:      "active contract does not need renewal",
			outpostID: activeContractOutpost.ID,
			outpost:   activeContractOutpost,
			want: map[string]string{
				OrderingRequirementTypeOutpostRenewalRequired: OrderingRequirementStatusPass,
			},
		},
		{
			name:      "bare site: no addresses, no rack props",
			outpostID: rackOutpost.ID,
			outpost:   rackOutpost,
			site:      &Site{ID: "os-1"},
			want: map[string]string{
				OrderingRequirementTypeOperatingAddressExistence:           OrderingRequirementStatusFail,
				OrderingRequirementTypeShippingAddressExistence:            OrderingRequirementStatusFail,
				OrderingRequirementTypeCountryCodeMismatch:                 OrderingRequirementStatusExempt,
				OrderingRequirementTypeValidZipCode:                        OrderingRequirementStatusExempt,
				OrderingRequirementTypeRackPhysicalProperties:              OrderingRequirementStatusFail,
				OrderingRequirementTypeShippingAddressMissingContactName:   OrderingRequirementStatusExempt,
				OrderingRequirementTypeShippingAddressMissingContactNumber: OrderingRequirementStatusExempt,
				OrderingRequirementTypeShippingAddressMissingContactInfo:   OrderingRequirementStatusExempt,
			},
		},
		{
			name:      "server outpost is exempt from rack physical properties",
			outpostID: serverOutpost.ID,
			outpost:   serverOutpost,
			site:      &Site{ID: "os-2"},
			want: map[string]string{
				OrderingRequirementTypeRackPhysicalProperties: OrderingRequirementStatusExempt,
			},
		},
		{
			name:      "rack outpost with complete rack properties passes",
			outpostID: rackOutpost.ID,
			outpost:   rackOutpost,
			site:      &Site{ID: "os-3", RackPhysicalProperties: completeRackProps()},
			want: map[string]string{
				OrderingRequirementTypeRackPhysicalProperties: OrderingRequirementStatusPass,
			},
		},
		{
			name:        "full matching addresses pass",
			outpostID:   serverOutpost.ID,
			outpost:     serverOutpost,
			countryCode: "US",
			site: &Site{
				ID:               "os-4",
				OperatingAddress: fullAddress("US", "98101"),
				ShippingAddress:  fullAddress("US", "98101"),
			},
			want: map[string]string{
				OrderingRequirementTypeOperatingAddressExistence:           OrderingRequirementStatusPass,
				OrderingRequirementTypeShippingAddressExistence:            OrderingRequirementStatusPass,
				OrderingRequirementTypeCountryCodeMismatch:                 OrderingRequirementStatusPass,
				OrderingRequirementTypeValidZipCode:                        OrderingRequirementStatusPass,
				OrderingRequirementTypeShippingAddressMissingContactName:   OrderingRequirementStatusPass,
				OrderingRequirementTypeShippingAddressMissingContactNumber: OrderingRequirementStatusPass,
				OrderingRequirementTypeShippingAddressMissingContactInfo:   OrderingRequirementStatusPass,
			},
		},
		{
			name:        "country code mismatch",
			outpostID:   serverOutpost.ID,
			outpost:     serverOutpost,
			countryCode: "CA",
			site:        &Site{ID: "os-5", OperatingAddress: fullAddress("US", "98101")},
			want: map[string]string{
				OrderingRequirementTypeCountryCodeMismatch: OrderingRequirementStatusFail,
			},
		},
		{
			name:        "invalid us zip",
			outpostID:   serverOutpost.ID,
			outpost:     serverOutpost,
			countryCode: "US",
			site:        &Site{ID: "os-6", OperatingAddress: fullAddress("US", "not-a-zip")},
			want: map[string]string{
				OrderingRequirementTypeValidZipCode: OrderingRequirementStatusFail,
			},
		},
		{
			name:        "non-us postal code is exempt from format validation",
			outpostID:   serverOutpost.ID,
			outpost:     serverOutpost,
			countryCode: "DE",
			site:        &Site{ID: "os-7", OperatingAddress: fullAddress("DE", "10115")},
			want: map[string]string{
				OrderingRequirementTypeValidZipCode: OrderingRequirementStatusExempt,
			},
		},
		{
			name:      "shipping address missing only contact name",
			outpostID: serverOutpost.ID,
			outpost:   serverOutpost,
			site: &Site{
				ID: "os-8",
				ShippingAddress: &Address{
					ContactPhoneNumber: "+12065550100",
				},
			},
			want: map[string]string{
				OrderingRequirementTypeShippingAddressMissingContactName:   OrderingRequirementStatusFail,
				OrderingRequirementTypeShippingAddressMissingContactNumber: OrderingRequirementStatusPass,
				OrderingRequirementTypeShippingAddressMissingContactInfo:   OrderingRequirementStatusPass,
			},
		},
		{
			name:      "shipping address missing all contact info",
			outpostID: serverOutpost.ID,
			outpost:   serverOutpost,
			site:      &Site{ID: "os-9", ShippingAddress: &Address{}},
			want: map[string]string{
				OrderingRequirementTypeShippingAddressMissingContactName:   OrderingRequirementStatusFail,
				OrderingRequirementTypeShippingAddressMissingContactNumber: OrderingRequirementStatusFail,
				OrderingRequirementTypeShippingAddressMissingContactInfo:   OrderingRequirementStatusFail,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := requirementStatuses(
				buildOrderingRequirements(tt.outpostID, tt.outpost, tt.site, tt.countryCode),
			)

			for reqType, wantStatus := range tt.want {
				assert.Equal(t, wantStatus, got[reqType], reqType)
			}
		})
	}
}

func TestHasCompleteRackPhysicalProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props *RackPhysicalProperties
		name  string
		want  bool
	}{
		{name: "nil", props: nil, want: false},
		{name: "empty", props: &RackPhysicalProperties{}, want: false},
		{name: "complete", props: completeRackProps(), want: true},
		{
			name: "missing one field",
			props: func() *RackPhysicalProperties {
				p := completeRackProps()
				p.PowerConnector = ""

				return p
			}(),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, hasCompleteRackPhysicalProperties(tt.props))
		})
	}
}
