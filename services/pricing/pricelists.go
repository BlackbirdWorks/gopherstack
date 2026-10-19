package pricing

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	arnPrefix  = "arn:aws:pricing:::price-list/aws/"
	disclaimer = "This pricing list is for informational purposes only. All prices are subject to the additional " +
		"terms included in the pricing pages on http://aws.amazon.com. All Free Tier prices are also subject to " +
		"the terms included at https://aws.amazon.com/free/"
	fileVersion  = "v1.0"
	formatJSON   = "json"
	formatCSV    = "csv"
	arnParts     = 4
	csvFixedCols = 15
)

// PriceListInfo is one ListPriceLists element.
type PriceListInfo struct {
	CurrencyCode string   `json:"CurrencyCode"`
	PriceListArn string   `json:"PriceListArn"`
	RegionCode   string   `json:"RegionCode"`
	FileFormats  []string `json:"FileFormats"`
}

func priceListARN(code, ver, region string) string {
	return fmt.Sprintf("%s%s/%s/%s/%s", arnPrefix, code, currency, ver, region)
}

// parsePriceListARN splits an ARN into service, version and region.
func parsePriceListARN(arn string) (string, string, string, bool) {
	rest, found := strings.CutPrefix(arn, arnPrefix)
	if !found {
		return "", "", "", false
	}

	parts := strings.Split(rest, "/")
	if len(parts) != arnParts || parts[1] != currency {
		return "", "", "", false
	}

	return parts[0], parts[2], parts[3], priceListExists(parts[0], parts[2], parts[3])
}

func priceListExists(code, ver, region string) bool {
	if ver != catalogVersion {
		return false
	}

	for _, e := range theCatalog().byService[code] {
		if e.region == region {
			return true
		}
	}

	return false
}

func regionsOf(list []*entry) []string {
	seen := map[string]bool{}
	for _, e := range list {
		seen[e.region] = true
	}

	out := make([]string, 0, len(seen))
	for r := range seen {
		out = append(out, r)
	}

	sort.Strings(out)

	return out
}

// PriceLists returns the price lists of a service effective at the given time.
func PriceLists(
	code, cur, regionCode string, effective time.Time, token string, limit *int32,
) ([]PriceListInfo, string, error) {
	if cur != currency {
		return nil, "", fail(ErrInvalidParameter, "CurrencyCode %q is not supported; use %s", cur, currency)
	}

	list, ok := theCatalog().byService[code]
	if !ok {
		return nil, "", fail(ErrResourceNotFound, "service code %q not found", code)
	}

	published, _ := time.Parse(time.RFC3339, publicationDate)

	var infos []PriceListInfo

	if !effective.Before(published) {
		for _, r := range regionsOf(list) {
			if regionCode == "" || regionCode == r {
				infos = append(infos, PriceListInfo{
					CurrencyCode: currency, PriceListArn: priceListARN(code, catalogVersion, r),
					RegionCode: r, FileFormats: []string{formatCSV, formatJSON},
				})
			}
		}
	}

	pg, err := paginate(infos, token, limit)

	return pg.Data, pg.Next, err
}

// FileKind resolves a price list ARN and file format to a download path suffix.
func FileKind(arn, format string) (string, string, string, string, error) {
	kind := strings.ToLower(format)
	if kind != formatJSON && kind != formatCSV {
		return "", "", "", "", fail(ErrInvalidParameter, "FileFormat %q is not available; use json or csv", format)
	}

	code, ver, region, ok := parsePriceListARN(arn)
	if !ok {
		return "", "", "", "", fail(ErrResourceNotFound, "price list %q not found", arn)
	}

	return code, ver, region, kind, nil
}

// Disclaimer-bearing file envelope, as published at the Price List bulk endpoint.
type fileJSON struct {
	Products        map[string]productJSON    `json:"products"`
	Terms           map[string]map[string]any `json:"terms"`
	FormatVersion   string                    `json:"formatVersion"`
	Disclaimer      string                    `json:"disclaimer"`
	OfferCode       string                    `json:"offerCode"`
	Version         string                    `json:"version"`
	PublicationDate string                    `json:"publicationDate"`
}

func regionEntries(code, region string) []*entry {
	var out []*entry

	for _, e := range theCatalog().byService[code] {
		if e.region == region {
			out = append(out, e)
		}
	}

	return out
}

// RenderFile returns the price list file body and its content type; ok is false when it does not exist.
func RenderFile(code, ver, region, kind string) ([]byte, string, bool) {
	if !priceListExists(code, ver, region) {
		return nil, "", false
	}

	entries := regionEntries(code, region)

	if kind == formatCSV {
		return renderCSV(code, entries), "text/csv", true
	}

	f := fileJSON{
		FormatVersion: fileVersion, Disclaimer: disclaimer, OfferCode: code, Version: ver,
		PublicationDate: publicationDate, Products: map[string]productJSON{},
		Terms: map[string]map[string]any{"OnDemand": {}, "Reserved": {}},
	}

	for _, e := range entries {
		f.Products[e.item.Product.SKU] = e.item.Product
		f.Terms["OnDemand"][e.item.Product.SKU] = e.item.Terms.OnDemand

		if len(e.item.Terms.Reserved) > 0 {
			f.Terms["Reserved"][e.item.Product.SKU] = e.item.Terms.Reserved
		}
	}

	raw, _ := json.Marshal(f)

	return raw, "application/json", true
}

func renderCSV(code string, entries []*entry) []byte {
	attrCols := attributeNames(entries)

	var sb strings.Builder

	w := quotedCSV{&sb}
	_ = w.Write([]string{"FormatVersion", fileVersion})
	_ = w.Write([]string{"Disclaimer", disclaimer})
	_ = w.Write([]string{"Publication Date", publicationDate})
	_ = w.Write([]string{"Version", catalogVersion})
	_ = w.Write([]string{"OfferCode", code})

	header := make([]string, 0, csvFixedCols+len(attrCols))
	header = append(header,
		"SKU", "OfferTermCode", "RateCode", "TermType", "PriceDescription", "EffectiveDate", "StartingRange",
		"EndingRange", "Unit", "PricePerUnit", "Currency", "LeaseContractLength", "PurchaseOption",
		"OfferingClass", "Product Family")
	_ = w.Write(append(header, attrCols...))

	for _, e := range entries {
		for _, tt := range []struct {
			terms map[string]termJSON
			name  string
		}{{e.item.Terms.OnDemand, "OnDemand"}, {e.item.Terms.Reserved, "Reserved"}} {
			for _, term := range tt.terms {
				for _, d := range term.PriceDimensions {
					row := make([]string, 0, csvFixedCols+len(attrCols))
					row = append(row,
						e.item.Product.SKU, term.OfferTermCode, d.RateCode, tt.name, d.Description, term.EffectiveDate,
						d.BeginRange, d.EndRange, d.Unit, d.PricePerUnit[currency], currency,
						term.TermAttributes["LeaseContractLength"], term.TermAttributes["PurchaseOption"],
						term.TermAttributes["OfferingClass"], e.item.Product.ProductFamily)
					for _, c := range attrCols {
						row = append(row, e.item.Product.Attributes[c])
					}

					_ = w.Write(row)
				}
			}
		}
	}

	return []byte(sb.String())
}

// quotedCSV writes every field quoted, as the published price list CSVs do.
type quotedCSV struct{ sb *strings.Builder }

func (q quotedCSV) Write(row []string) error {
	for i, f := range row {
		if i > 0 {
			q.sb.WriteByte(',')
		}

		q.sb.WriteString(`"` + strings.ReplaceAll(f, `"`, `""`) + `"`)
	}

	q.sb.WriteByte('\n')

	return nil
}
