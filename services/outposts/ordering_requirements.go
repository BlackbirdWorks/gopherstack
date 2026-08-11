package outposts

import (
	"regexp"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// usZipPattern matches a 5-digit or ZIP+4 US postal code. There is no public
// database of which specific codes are actually assigned (that data is
// USPS-internal), so this is a format check only -- narrower than whatever
// real AWS does, but honest about the boundary rather than fabricating a
// pass/fail against data this emulator doesn't have.
var usZipPattern = regexp.MustCompile(`^\d{5}(-\d{4})?$`)

// buildOrderingRequirements evaluates the 12 of 17 real OrderingRequirementType
// checks (docs.aws.amazon.com/outposts/latest/APIReference/, types/enums.go)
// this backend has real state to answer. outpostID is the quote's stored
// OutpostID (possibly "" or possibly non-empty but no-longer-resolving,
// distinctly from outpost itself, which is the resolved *Outpost or nil --
// see outpostNotFoundRequirement). site is the Outpost's Site, or nil.
// countryCode is the quote's own requested CountryCode. The remaining 5
// checks (MAXIMUM_ALLOWED_ORDERS_CHECK_ERROR, OUTPOST_GENERATION_MISMATCH_ERROR,
// UNSUPPORTED, ENTERPRISE_SUPPORT_ERROR, OUTPOST_STATE_CHANGED_ERROR) are not
// produced -- see PARITY.md for why each one is either structural (no data
// source can exist) or would require inventing undocumented AWS business
// logic this backend has no anchor for.
func buildOrderingRequirements(
	outpostID string,
	outpost *Outpost,
	site *Site,
	countryCode string,
) []OrderingRequirement {
	return []OrderingRequirement{
		outpostIDMissingRequirement(outpostID),
		outpostNotFoundRequirement(outpostID, outpost),
		outpostActiveRequirement(outpost),
		outpostRenewalRequiredRequirement(outpost),
		operatingAddressExistenceRequirement(site),
		shippingAddressExistenceRequirement(site),
		countryCodeMismatchRequirement(site, countryCode),
		validZipCodeRequirement(site),
		rackPhysicalPropertiesRequirement(outpost, site),
		shippingAddressMissingContactNameRequirement(site),
		shippingAddressMissingContactNumberRequirement(site),
		shippingAddressMissingContactInfoRequirement(site),
	}
}

func passRequirement(reqType string) OrderingRequirement {
	return OrderingRequirement{
		OrderingRequirementType: reqType,
		Status:                  OrderingRequirementStatusPass,
	}
}

func failRequirement(reqType, msg string) OrderingRequirement {
	return OrderingRequirement{
		OrderingRequirementType: reqType,
		Status:                  OrderingRequirementStatusFail,
		StatusMessage:           msg,
	}
}

func exemptRequirement(reqType, msg string) OrderingRequirement {
	return OrderingRequirement{
		OrderingRequirementType: reqType,
		Status:                  OrderingRequirementStatusExempt,
		StatusMessage:           msg,
	}
}

func outpostIDMissingRequirement(outpostID string) OrderingRequirement {
	if outpostID == "" {
		return failRequirement(
			OrderingRequirementTypeOutpostIDMissing,
			"no Outpost is associated with this quote",
		)
	}

	return passRequirement(OrderingRequirementTypeOutpostIDMissing)
}

// outpostNotFoundRequirement distinguishes "no OutpostID was ever set on
// this quote" (see outpostIDMissingRequirement) from "an OutpostID is set
// but no longer resolves to a real Outpost" -- reachable when the Outpost is
// deleted after being associated with a still-live quote (DeleteOutpost has
// no FK check against Quotes).
func outpostNotFoundRequirement(outpostID string, outpost *Outpost) OrderingRequirement {
	switch {
	case outpostID == "":
		return exemptRequirement(OrderingRequirementTypeOutpostNotFound, "no Outpost to check")
	case outpost == nil:
		return failRequirement(
			OrderingRequirementTypeOutpostNotFound,
			"the Outpost associated with this quote no longer exists",
		)
	default:
		return passRequirement(OrderingRequirementTypeOutpostNotFound)
	}
}

func outpostActiveRequirement(outpost *Outpost) OrderingRequirement {
	if outpost == nil {
		return exemptRequirement(OrderingRequirementTypeOutpostActive, "no Outpost to check")
	}

	if outpost.LifeCycleStatus != LifeCycleStatusActive {
		return failRequirement(OrderingRequirementTypeOutpostActive, "the Outpost is not ACTIVE")
	}

	return passRequirement(OrderingRequirementTypeOutpostActive)
}

// outpostRenewalRequiredRequirement reads Outpost.ContractEndDate, real
// per-Outpost state populated at order-fulfillment time (see orders.go's
// recordOriginalSubscriptionLocked) and updated by CreateRenewal. A zero
// ContractEndDate means no subscription has ever been established (no
// fulfilled order, no renewal) -- EXEMPT, not FAIL, since there is no
// contract to have lapsed.
func outpostRenewalRequiredRequirement(outpost *Outpost) OrderingRequirement {
	if outpost == nil || outpost.ContractEndDate.IsZero() {
		return exemptRequirement(
			OrderingRequirementTypeOutpostRenewalRequired,
			"no subscription contract to check",
		)
	}

	if time.Now().After(outpost.ContractEndDate) {
		return failRequirement(
			OrderingRequirementTypeOutpostRenewalRequired,
			"the Outpost's subscription contract has expired",
		)
	}

	return passRequirement(OrderingRequirementTypeOutpostRenewalRequired)
}

func operatingAddressExistenceRequirement(site *Site) OrderingRequirement {
	if site == nil {
		return exemptRequirement(
			OrderingRequirementTypeOperatingAddressExistence,
			"no Site to check",
		)
	}

	if site.OperatingAddress == nil {
		return failRequirement(
			OrderingRequirementTypeOperatingAddressExistence,
			"the Site has no operating address",
		)
	}

	return passRequirement(OrderingRequirementTypeOperatingAddressExistence)
}

func shippingAddressExistenceRequirement(site *Site) OrderingRequirement {
	if site == nil {
		return exemptRequirement(
			OrderingRequirementTypeShippingAddressExistence,
			"no Site to check",
		)
	}

	if site.ShippingAddress == nil {
		return failRequirement(
			OrderingRequirementTypeShippingAddressExistence,
			"the Site has no shipping address",
		)
	}

	return passRequirement(OrderingRequirementTypeShippingAddressExistence)
}

// countryCodeMismatchRequirement compares the quote's own requested
// CountryCode against the Site's operating address country -- both real,
// stored fields.
func countryCodeMismatchRequirement(site *Site, countryCode string) OrderingRequirement {
	if site == nil || site.OperatingAddress == nil || site.OperatingAddress.CountryCode == "" ||
		countryCode == "" {
		return exemptRequirement(
			OrderingRequirementTypeCountryCodeMismatch,
			"no Site country code to compare",
		)
	}

	if !strs.Equal(site.OperatingAddress.CountryCode, countryCode) {
		return failRequirement(OrderingRequirementTypeCountryCodeMismatch,
			"the quote's CountryCode does not match the Site's operating address country")
	}

	return passRequirement(OrderingRequirementTypeCountryCodeMismatch)
}

// validZipCodeRequirement checks the Site's operating-address postal code
// against the well-known US ZIP format. Only US addresses are validated --
// there is no public per-country postal-format table in this repo, so every
// other country is EXEMPT rather than a fabricated pass/fail.
func validZipCodeRequirement(site *Site) OrderingRequirement {
	if site == nil || site.OperatingAddress == nil || site.OperatingAddress.PostalCode == "" {
		return exemptRequirement(OrderingRequirementTypeValidZipCode, "no postal code to validate")
	}

	if site.OperatingAddress.CountryCode != "US" {
		return exemptRequirement(
			OrderingRequirementTypeValidZipCode,
			"postal code format is only validated for US addresses",
		)
	}

	if !usZipPattern.MatchString(site.OperatingAddress.PostalCode) {
		return failRequirement(
			OrderingRequirementTypeValidZipCode,
			"postal code is not a valid US ZIP code",
		)
	}

	return passRequirement(OrderingRequirementTypeValidZipCode)
}

// rackPhysicalPropertiesRequirement only applies to a RACK-type Outpost --
// a SERVER Outpost has no rack to physically describe.
func rackPhysicalPropertiesRequirement(outpost *Outpost, site *Site) OrderingRequirement {
	if outpost == nil || outpost.SupportedHardwareType != HardwareTypeRack {
		return exemptRequirement(
			OrderingRequirementTypeRackPhysicalProperties,
			"not a rack-type Outpost",
		)
	}

	if site == nil || !hasCompleteRackPhysicalProperties(site.RackPhysicalProperties) {
		return failRequirement(
			OrderingRequirementTypeRackPhysicalProperties,
			"the Site's rack physical properties are incomplete",
		)
	}

	return passRequirement(OrderingRequirementTypeRackPhysicalProperties)
}

func hasCompleteRackPhysicalProperties(r *RackPhysicalProperties) bool {
	return r != nil &&
		r.PowerConnector != "" && r.PowerDrawKva != "" && r.PowerFeedDrop != "" && r.PowerPhase != "" &&
		r.UplinkCount != "" && r.UplinkGbps != "" && r.FiberOpticCableType != "" &&
		r.OpticalStandard != "" && r.MaximumSupportedWeightLbs != ""
}

func shippingAddressMissingContactNameRequirement(site *Site) OrderingRequirement {
	if site == nil || site.ShippingAddress == nil {
		return exemptRequirement(
			OrderingRequirementTypeShippingAddressMissingContactName,
			"no shipping address to check",
		)
	}

	if site.ShippingAddress.ContactName == "" {
		return failRequirement(
			OrderingRequirementTypeShippingAddressMissingContactName,
			"the shipping address has no contact name",
		)
	}

	return passRequirement(OrderingRequirementTypeShippingAddressMissingContactName)
}

func shippingAddressMissingContactNumberRequirement(site *Site) OrderingRequirement {
	if site == nil || site.ShippingAddress == nil {
		return exemptRequirement(
			OrderingRequirementTypeShippingAddressMissingContactNumber,
			"no shipping address to check",
		)
	}

	if site.ShippingAddress.ContactPhoneNumber == "" {
		return failRequirement(
			OrderingRequirementTypeShippingAddressMissingContactNumber,
			"the shipping address has no contact phone number",
		)
	}

	return passRequirement(OrderingRequirementTypeShippingAddressMissingContactNumber)
}

// shippingAddressMissingContactInfoRequirement is a combined signal, distinct
// from the two single-field checks above: it only FAILs when BOTH contact
// fields are missing (i.e. the shipping address carries no contact
// information at all), a documented judgment call on how the three
// SHIPPING_ADDRESS_MISSING_CONTACT_* checks relate to one another (the SDK
// does not specify it).
func shippingAddressMissingContactInfoRequirement(site *Site) OrderingRequirement {
	if site == nil || site.ShippingAddress == nil {
		return exemptRequirement(
			OrderingRequirementTypeShippingAddressMissingContactInfo,
			"no shipping address to check",
		)
	}

	if site.ShippingAddress.ContactName == "" && site.ShippingAddress.ContactPhoneNumber == "" {
		return failRequirement(
			OrderingRequirementTypeShippingAddressMissingContactInfo,
			"the shipping address has no contact information",
		)
	}

	return passRequirement(OrderingRequirementTypeShippingAddressMissingContactInfo)
}
