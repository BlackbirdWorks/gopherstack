package dashboard_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

// TestDashboard_Route53_Index tests the Route 53 hosted zones index page.
func TestDashboard_Route53_Index(t *testing.T) {
	t.Parallel()

	t.Run("empty zone list", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/route53", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "Route 53")
	})

	t.Run("shows created zone", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create a zone via form POST.
		req := httptest.NewRequest(http.MethodPost, "/dashboard/route53/create",
			strings.NewReader("zone_name=test.example.com"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusFound, w.Code)

		// Verify zone appears in index.
		req = httptest.NewRequest(http.MethodGet, "/dashboard/route53", nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "test.example.com")
	})

	t.Run("nil Route53Ops renders empty", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/route53", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "Route 53")
	})
}

// TestDashboard_Route53_CreateZone tests creating a hosted zone via dashboard.
func TestDashboard_Route53_CreateZone(t *testing.T) {
	t.Parallel()

	t.Run("create zone successfully", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/route53/create",
			strings.NewReader("zone_name=myzone.example.com"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)
		assert.Equal(t, "/dashboard/route53", w.Header().Get("Location"))
	})

	t.Run("empty zone name returns 400", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/route53/create",
			strings.NewReader("zone_name="))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("nil Route53Ops returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodPost, "/dashboard/route53/create",
			strings.NewReader("zone_name=test.com"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// TestDashboard_Route53_DeleteZone tests deleting a hosted zone via dashboard.
func TestDashboard_Route53_DeleteZone(t *testing.T) {
	t.Parallel()

	t.Run("delete existing zone", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create first.
		req := httptest.NewRequest(http.MethodPost, "/dashboard/route53/create",
			strings.NewReader("zone_name=delete-me.example.com"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusFound, w.Code)

		// Find zone ID.
		p, err := stack.Route53Handler.Backend.ListHostedZones("", 0)
		zones := p.Data
		require.NoError(t, err)
		require.Len(t, zones, 1)
		zoneID := zones[0].ID

		// Delete.
		req = httptest.NewRequest(http.MethodDelete, "/dashboard/route53/delete?id="+zoneID, nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)
		assert.Equal(t, "/dashboard/route53", w.Header().Get("Location"))
	})

	t.Run("nil Route53Ops returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/route53/delete?id=ZTESTID", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// TestDashboard_Route53_ZoneDetail tests the zone detail page.
func TestDashboard_Route53_ZoneDetail(t *testing.T) {
	t.Parallel()

	t.Run("detail page for existing zone", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create zone.
		req := httptest.NewRequest(http.MethodPost, "/dashboard/route53/create",
			strings.NewReader("zone_name=detail.example.com"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusFound, w.Code)

		p, err := stack.Route53Handler.Backend.ListHostedZones("", 0)
		zones := p.Data
		require.NoError(t, err)
		require.Len(t, zones, 1)
		zoneID := zones[0].ID

		req = httptest.NewRequest(http.MethodGet, "/dashboard/route53/zone?id="+zoneID, nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "detail.example.com")
	})

	t.Run("detail page for non-existent zone returns 404", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/route53/zone?id=ZNONEXISTENT", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("nil Route53Ops returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/route53/zone?id=ZTEST", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// TestDashboard_Route53_CreateRecord tests creating a DNS record.
func TestDashboard_Route53_CreateRecord(t *testing.T) {
	t.Parallel()

	t.Run("create record successfully", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create zone first.
		reqCreate := httptest.NewRequest(http.MethodPost, "/dashboard/route53/create",
			strings.NewReader("zone_name=records.example.com"))
		reqCreate.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, reqCreate)
		require.Equal(t, http.StatusFound, w.Code)

		p, err := stack.Route53Handler.Backend.ListHostedZones("", 0)
		zones := p.Data
		require.NoError(t, err)
		require.Len(t, zones, 1)
		zoneID := zones[0].ID

		form := "zone_id=" + zoneID + "&rec_name=www.records.example.com&rec_type=A&rec_ttl=300&rec_value=1.2.3.4"
		req := httptest.NewRequest(http.MethodPost, "/dashboard/route53/record",
			strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)
		assert.Contains(t, w.Header().Get("Location"), zoneID)
	})

	t.Run("nil Route53Ops returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodPost, "/dashboard/route53/record",
			strings.NewReader("zone_id=ZID&rec_name=test.com&rec_type=A&rec_ttl=300&rec_value=1.2.3.4"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// TestDashboard_Route53_DeleteRecord tests deleting a DNS record.
func TestDashboard_Route53_DeleteRecord(t *testing.T) {
	t.Parallel()

	t.Run("delete existing record", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create zone.
		reqCreate := httptest.NewRequest(http.MethodPost, "/dashboard/route53/create",
			strings.NewReader("zone_name=delrec.example.com"))
		reqCreate.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, reqCreate)
		require.Equal(t, http.StatusFound, w.Code)

		p, err := stack.Route53Handler.Backend.ListHostedZones("", 0)
		zones := p.Data
		require.NoError(t, err)
		require.Len(t, zones, 1)
		zoneID := zones[0].ID

		// Create a record.
		form := "zone_id=" + zoneID + "&rec_name=www.delrec.example.com&rec_type=A&rec_ttl=300&rec_value=5.6.7.8"
		reqRec := httptest.NewRequest(http.MethodPost, "/dashboard/route53/record",
			strings.NewReader(form))
		reqRec.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, reqRec)
		require.Equal(t, http.StatusFound, w.Code)

		// Delete the record.
		req := httptest.NewRequest(http.MethodDelete,
			"/dashboard/route53/record?zone_id="+zoneID+"&name=www.delrec.example.com.&type=A", nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)
	})

	t.Run("nil Route53Ops returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodDelete,
			"/dashboard/route53/record?zone_id=ZID&name=test.com.&type=A", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}
