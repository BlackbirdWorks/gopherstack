package azuresearch_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresearch"
)

const testService = "svc1"

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

type stack struct {
	b     *azuresearch.InMemoryBackend
	clock *fakeClock
	url   string
	admin string
	query string
}

func newStack(t *testing.T, sku string) *stack {
	t.Helper()

	clock := &fakeClock{now: time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)}
	b := azuresearch.NewInMemoryBackend(azuresearch.WithClock(clock.Now))

	_, err := b.PutService(testService, azuresearch.ServiceConfig{
		SKU: sku, HostingMode: "default", PublicNetworkAccess: "enabled", AuthMode: "apiKeyOnly",
		ReplicaCount: 1, PartitionCount: 1,
	})
	require.NoError(t, err)

	keys, err := b.Keys(testService)
	require.NoError(t, err)

	qk, err := b.ListQueryKeys(testService)
	require.NoError(t, err)
	require.NotEmpty(t, qk)

	h := azuresearch.NewHandler(b, azuresearch.DefaultSettings())
	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return &stack{b: b, clock: clock, url: srv.URL, admin: keys.Primary, query: qk[0].Key}
}

type resp struct {
	body   map[string]any
	header http.Header
	raw    []byte
	status int
}

func (s *stack) do(t *testing.T, method, path, key string, body any) resp {
	t.Helper()

	var rd io.Reader

	switch v := body.(type) {
	case nil:
	case string:
		rd = bytes.NewBufferString(v)
	default:
		raw, err := json.Marshal(v)
		require.NoError(t, err)

		rd = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, s.url+path, rd)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	if key != "" {
		req.Header.Set("api-key", key)
	}

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer res.Body.Close()

	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	out := resp{status: res.StatusCode, header: res.Header, raw: raw}
	_ = json.Unmarshal(raw, &out.body)

	return out
}

const apiVer = "api-version=2024-07-01"

func (s *stack) admin2(t *testing.T, method, path string, body any) resp {
	t.Helper()

	return s.do(t, method, path, s.admin, body)
}

func (s *stack) must(t *testing.T, method, path string, body any, want int) resp {
	t.Helper()

	r := s.admin2(t, method, path, body)
	require.Equal(t, want, r.status, string(r.raw))

	return r
}

func hotelIndex() map[string]any {
	return map[string]any{
		"name": "hotels",
		"fields": []any{
			map[string]any{"name": "HotelId", "type": "Edm.String", "key": true, "filterable": true},
			map[string]any{"name": "HotelName", "type": "Edm.String", "searchable": true, "sortable": true},
			map[string]any{"name": "Description", "type": "Edm.String", "searchable": true, "analyzer": "en.lucene"},
			map[string]any{"name": "Category", "type": "Edm.String", "searchable": true, "filterable": true, "facetable": true, "sortable": true},
			map[string]any{"name": "Tags", "type": "Collection(Edm.String)", "searchable": true, "filterable": true, "facetable": true},
			map[string]any{"name": "Rating", "type": "Edm.Double", "filterable": true, "sortable": true, "facetable": true},
			map[string]any{"name": "ParkingIncluded", "type": "Edm.Boolean", "filterable": true},
			map[string]any{"name": "LastRenovationDate", "type": "Edm.DateTimeOffset", "filterable": true, "sortable": true, "facetable": true},
			map[string]any{"name": "Location", "type": "Edm.GeographyPoint", "filterable": true, "sortable": true},
			map[string]any{"name": "Address", "type": "Edm.ComplexType", "fields": []any{
				map[string]any{"name": "City", "type": "Edm.String", "searchable": true, "filterable": true, "facetable": true},
				map[string]any{"name": "StateProvince", "type": "Edm.String", "filterable": true},
			}},
			map[string]any{"name": "Rooms", "type": "Collection(Edm.ComplexType)", "fields": []any{
				map[string]any{"name": "Type", "type": "Edm.String", "searchable": true, "filterable": true},
				map[string]any{"name": "BaseRate", "type": "Edm.Double", "filterable": true},
			}},
			map[string]any{"name": "Secret", "type": "Edm.String", "retrievable": false, "filterable": true},
		},
		"suggesters": []any{map[string]any{"name": "sg", "searchMode": "analyzingInfixMatching", "sourceFields": []any{"HotelName", "Address/City"}}},
	}
}

func geo(lon, lat float64) map[string]any {
	return map[string]any{"type": "Point", "coordinates": []any{lon, lat}}
}

func hotelDocs() []any {
	return []any{
		map[string]any{
			"@search.action": "upload", "HotelId": "1", "HotelName": "Secret Point Motel", "Category": "Boutique",
			"Description": "The hotel is ideally located on the main commercial artery of the city in the heart of New York.",
			"Tags": []any{"pool", "air conditioning", "concierge"}, "Rating": 3.6, "ParkingIncluded": false,
			"LastRenovationDate": "1970-01-18T00:00:00Z", "Location": geo(-73.975403, 40.760586),
			"Address": map[string]any{"City": "New York", "StateProvince": "NY"}, "Secret": "a",
			"Rooms": []any{map[string]any{"Type": "Budget Room", "BaseRate": 96.99}, map[string]any{"Type": "Deluxe Room", "BaseRate": 150.0}},
		},
		map[string]any{
			"@search.action": "upload", "HotelId": "2", "HotelName": "Twin Dome Motel", "Category": "Boutique",
			"Description": "The hotel is situated in a nineteenth century plaza, which has been expanded and renovated to the highest architectural standards.",
			"Tags": []any{"pool", "free wifi", "concierge"}, "Rating": 3.6, "ParkingIncluded": true,
			"LastRenovationDate": "1979-02-18T00:00:00Z", "Location": geo(-78.475, 40.35),
			"Address": map[string]any{"City": "Sarasota", "StateProvince": "FL"}, "Secret": "b",
			"Rooms": []any{map[string]any{"Type": "Suite", "BaseRate": 250.0}},
		},
		map[string]any{
			"@search.action": "upload", "HotelId": "3", "HotelName": "Triple Landscape Hotel", "Category": "Resort and Spa",
			"Description": "The Hotel stands out for its gastronomic excellence under the management of William Dough, who advises on and oversees all of the Hotel's restaurant services.",
			"Tags": []any{"air conditioning", "bar", "continental breakfast"}, "Rating": 4.8, "ParkingIncluded": true,
			"LastRenovationDate": "2015-09-20T00:00:00Z", "Location": geo(-122.131577, 47.678581),
			"Address": map[string]any{"City": "Kirkland", "StateProvince": "WA"}, "Secret": "c",
			"Rooms": []any{map[string]any{"Type": "Budget Room", "BaseRate": 60.0}},
		},
		map[string]any{
			"@search.action": "upload", "HotelId": "4", "HotelName": "Sublime Palace Hotel", "Category": "Boutique",
			"Description": "Sublime Palace Hotel is located in the heart of the historic center of Sublime in an extremely vibrant and lively area within short walking distance to the sites and landmarks of the city and is surrounded by the extraordinary beauty of churches, buildings, shops and monuments.",
			"Tags": []any{"concierge", "view", "24-hour front desk service"}, "Rating": 4.6, "ParkingIncluded": true,
			"LastRenovationDate": "1960-02-06T00:00:00Z", "Location": geo(-122.13, 47.67),
			"Address": map[string]any{"City": "New York", "StateProvince": "NY"}, "Secret": "d",
			"Rooms": []any{},
		},
	}
}

// seedHotels creates the hotels index and uploads the sample documents.
func (s *stack) seedHotels(t *testing.T) {
	t.Helper()

	s.must(t, http.MethodPost, "/indexes?"+apiVer, hotelIndex(), http.StatusCreated)
	s.must(t, http.MethodPost, "/indexes('hotels')/docs/search.index?"+apiVer, map[string]any{"value": hotelDocs()}, http.StatusOK)
}

func keysOf(r resp) []string {
	rows, _ := r.body["value"].([]any)
	out := make([]string, 0, len(rows))

	for _, row := range rows {
		m, _ := row.(map[string]any)
		id, _ := m["HotelId"].(string)
		out = append(out, id)
	}

	return out
}

func jsonBody(v any) (io.ReadCloser, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewReader(raw)), nil
}

func serve(t *testing.T, b *azuresearch.InMemoryBackend) string {
	t.Helper()

	h := azuresearch.NewHandler(b, azuresearch.DefaultSettings())
	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv.URL
}
