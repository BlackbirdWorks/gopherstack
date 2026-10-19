package pricing

// Shapes follow the AWS Price List JSON: GetProducts returns one priceItem per element of PriceList,
// and the downloadable file groups the same products and terms by SKU.

type productJSON struct {
	Attributes    map[string]string `json:"attributes"`
	ProductFamily string            `json:"productFamily"`
	SKU           string            `json:"sku"`
}

type dimensionJSON struct {
	PricePerUnit map[string]string `json:"pricePerUnit"`
	Unit         string            `json:"unit"`
	EndRange     string            `json:"endRange"`
	Description  string            `json:"description"`
	RateCode     string            `json:"rateCode"`
	BeginRange   string            `json:"beginRange"`
	AppliesTo    []string          `json:"appliesTo"`
}

type termJSON struct {
	PriceDimensions map[string]dimensionJSON `json:"priceDimensions"`
	TermAttributes  map[string]string        `json:"termAttributes"`
	SKU             string                   `json:"sku"`
	EffectiveDate   string                   `json:"effectiveDate"`
	OfferTermCode   string                   `json:"offerTermCode"`
}

type termsJSON struct {
	OnDemand map[string]termJSON `json:"OnDemand"`
	Reserved map[string]termJSON `json:"Reserved,omitempty"`
}

type priceItem struct {
	Terms           termsJSON   `json:"terms"`
	Product         productJSON `json:"product"`
	ServiceCode     string      `json:"serviceCode"`
	Version         string      `json:"version"`
	PublicationDate string      `json:"publicationDate"`
}

// entry is one catalog product with its pre-rendered GetProducts element.
type entry struct {
	item    priceItem
	json    string
	region  string
	service string
}

// dim is a price tier in USD.
type dim struct {
	unit, desc, begin, end string
	usd                    float64
}

// spec describes a product before SKUs and rate codes are derived.
type spec struct {
	attrs    map[string]string
	service  string
	family   string
	region   region
	dims     []dim
	reserved bool
}

type region struct {
	code, location string
	factor         float64
}
