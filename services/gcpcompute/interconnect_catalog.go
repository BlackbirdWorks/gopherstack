package gcpcompute

import (
	computeapi "google.golang.org/api/compute/v1"
)

const (
	contAsia         = "C_ASIA_PAC"
	contEurope       = "C_EUROPE"
	contNAmerica     = "C_NORTH_AMERICA"
	contSAmerica     = "C_SOUTH_AMERICA"
	zonesPerFacility = 2
	collICLocations  = "interconnectLocations"
	icStatusAvail    = "AVAILABLE"
)

type icFacility struct {
	code      string
	id        string
	metro     string
	facility  string
	region    string
	continent string
}

// icFacilities is a subset of the Cloud Interconnect colocation facilities
// (docs.cloud.google.com/network-connectivity/docs/interconnect/concepts/colocation-facilities); each yields a
// zone1 and a zone2 location. region is the documented low-latency region.
var icFacilities = []icFacility{ //nolint:gochecknoglobals // read-only catalogue
	{"jnb", "850", "Johannesburg", "Teraco Johannesburg Campus, South Africa", "africa-south1", "C_AFRICA"},
	{"cpt", "99025", "Cape Town", "Teraco Cape Town, South Africa", "", "C_AFRICA"},
	{"hkg", "1118", "Hong Kong", "Equinix Hong Kong (HK2)", "asia-east2", contAsia},
	{"kix", "1791", "Osaka", "Equinix Osaka (OS1)", "asia-northeast2", contAsia},
	{"icn", "3829", "Seoul", "Sejong IX Center", "asia-northeast3", contAsia},
	{"sin", "2260", "Singapore", "Equinix Singapore (SG3)", "asia-southeast1", contAsia},
	{"tsa", "2886", "Taipei", "CHT Taipei-Aikuo IDC", "asia-east1", contAsia},
	{"nrt", "1893", "Tokyo", "Equinix Tokyo (TY4)", regAsiaNE1, contAsia},
	{"nrt", "452", "Tokyo", "Equinix Tokyo (TY2)", regAsiaNE1, contAsia},
	{"mel", "1988", "Melbourne", "Equinix ME1/ME2 - Melbourne", "australia-southeast2", contAsia},
	{"syd", "1605", "Sydney", "Equinix Sydney (SY3)", "australia-southeast1", contAsia},
	{"bom", "2310", "Mumbai", "Equinix MB1 - Mumbai (GPX Mumbai 1)", "asia-south1", contAsia},
	{"ams", "1236", "Amsterdam", "Equinix Amsterdam Schepenbergweg (AM5)", "europe-west4", contEurope},
	{"ber", "331", "Berlin", "Level(3) Berlin", "europe-west10", contEurope},
	{"fra", "58", "Frankfurt", "Digital Realty Frankfurt FRA1-16", regEuropeW3, contEurope},
	{"fra", "683", "Frankfurt", "Equinix Frankfurt West (FR4)", regEuropeW3, contEurope},
	{"hem", "7098", "Helsinki", "Equinix HE7 Helsinki Sinimaki", "europe-north1", contEurope},
	{"lhr", "2262", "London", "Equinix London Slough (LD6)", regEuropeW2, contEurope},
	{"lhr", "47", "London", "Global Switch (London 2)", regEuropeW2, contEurope},
	{"mad", "127", "Madrid", "Equinix MD2 - Madrid", "europe-southwest1", contEurope},
	{"mil", "1974", "Milan", "IRIDEOS Avalon Campus", "europe-west8", contEurope},
	{"cdg", "1536", "Paris", "Digital Realty Paris PAR7", "europe-west9", contEurope},
	{"arn", "156", "Stockholm", "Equinix Stockholm Bromma (SK1)", "europe-north2", contEurope},
	{"waw", "509", "Warsaw", "Equinix WA1 - Warsaw, Centrum LIM", "europe-central2", contEurope},
	{"zrh", "83", "Zurich", "Equinix ZH2", "europe-west6", contEurope},
	{"iad", "1", "Ashburn", "Equinix DC1-DC15, DC21 - Ashburn", regUSEast4, contNAmerica},
	{"iad", "5467", "Ashburn", "CoreSite - Reston (VA3)", regUSEast4, contNAmerica},
	{"atl", "940", "Atlanta", "Equinix AT1 - Atlanta", "", contNAmerica},
	{"ord", "7", "Chicago", "Equinix Chicago (CH1/CH2/CH4)", "", contNAmerica},
	{"cmh", "2377", "Columbus", "Cologix COL1-3", "us-east5", contNAmerica},
	{"dfw", "4", "Dallas", "Equinix Dallas (DA1)", "us-south1", contNAmerica},
	{"las", "770", "Las Vegas", "Switch Las Vegas", "us-west4", contNAmerica},
	{"lax", "19", "Los Angeles", "CoreSite - LA1 - One Wilshire", regUSWest2, contNAmerica},
	{"lax", "8", "Los Angeles", "Equinix Los Angeles (LA1)", regUSWest2, contNAmerica},
	{"yul", "1944", "Montreal", "Cologix MTL3", "northamerica-northeast1", contNAmerica},
	{"lga", "36", "New York", "Equinix NY5 - New York, Secaucus", "", contNAmerica},
	{"lga", "16", "New York", "Digital Realty | Telx New York (111 8th Ave)", "", contNAmerica},
	{"pdx", "1922", "Portland", "EdgeConneX Portland (EDCPOR01)", "us-west1", contNAmerica},
	{"slc", "10938", "Salt Lake City", "DataBank Salt Lake City (SLC5)", "us-west3", contNAmerica},
	{"sjc", "6", "San Francisco Bay Area", "Equinix SV1/SV5/SV10 - Silicon Valley, San Jose", "", contNAmerica},
	{"sea", "86", "Seattle", "Equinix Seattle (SE2/3)", "", contNAmerica},
	{"yyz", "2206", "Toronto", "Equinix Toronto (TR2)", "northamerica-northeast2", contNAmerica},
	{"gru", "165", "Sao Paulo", "Equinix Sao Paulo (SP4)", "southamerica-east1", contSAmerica},
	{"scl", "1779", "Santiago", "Cirion Santiago de Chile - SAN1", "southamerica-west1", contSAmerica},
	{"bog", "1354", "Bogota", "Equinix Bogota (BG1)", "", contSAmerica},
}

func icLocationList(project string) []*computeapi.InterconnectLocation {
	out := make([]*computeapi.InterconnectLocation, 0, len(icFacilities)*zonesPerFacility)

	for _, f := range icFacilities {
		for _, zone := range []string{"zone1", "zone2"} {
			name := f.code + "-" + zone + "-" + f.id
			loc := &computeapi.InterconnectLocation{
				Kind: "compute#interconnectLocation", Id: stableID("interconnectLocation", name),
				CreationTimestamp: catalogCreated, Name: name, Description: f.facility, City: f.metro,
				Continent: f.continent, Status: icStatusAvail, AvailabilityZone: zone + "-" + f.id,
				SelfLink: globalLink(project, collICLocations, name),
			}

			if f.region != "" {
				loc.RegionInfos = []*computeapi.InterconnectLocationRegionInfo{{
					Region: scopeURL(project, scopeRegion, f.region), LocationPresence: "LOCAL_REGION",
				}}
			}

			out = append(out, loc)
		}
	}

	return out
}

func icLocationByName(project, name string) (*computeapi.InterconnectLocation, bool) {
	for _, l := range icLocationList(project) {
		if l.Name == name {
			return l, true
		}
	}

	return nil, false
}

func (h *Handler) registerInterconnectLocationOps() {
	h.scopedList(collICLocations, scopeGlobal, "compute#interconnectLocationList", "", "", nil,
		func(_ *InMemoryBackend, p, _, _ string) []entry {
			locs := icLocationList(p)
			out := make([]entry, 0, len(locs))

			for _, l := range locs {
				out = append(out, entry{scope: scopeGlobal, item: l})
			}

			return out
		})
	h.reg(collICLocations, scopeGlobal, verbGet, func(_ *Handler, rq *request) (any, error) {
		if l, ok := icLocationByName(rq.rt.project, rq.rt.name); ok {
			return l, nil
		}

		return nil, errNotFound(relPath(rq.rt.project, scopeGlobal, "", collICLocations, rq.rt.name))
	})
}
