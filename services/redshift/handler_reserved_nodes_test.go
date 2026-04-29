package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- DescribeReservedNodes ----

func TestRedshiftHandler_DescribeReservedNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "list_empty",
			body:         "Action=DescribeReservedNodes&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeReservedNodesResponse"},
		},
		{
			name: "list_with_node",
			setup: func(b *redshift.InMemoryBackend) {
				b.AddReservedNodeInternal(&redshift.ReservedNode{
					ReservedNodeID:         "rn-test",
					ReservedNodeOfferingID: "offering-1",
					NodeType:               "dc2.large",
					State:                  "active",
				})
			},
			body:         "Action=DescribeReservedNodes&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeReservedNodesResponse", "rn-test", "dc2.large"},
		},
		{
			name: "get_specific_node",
			setup: func(b *redshift.InMemoryBackend) {
				b.AddReservedNodeInternal(&redshift.ReservedNode{
					ReservedNodeID: "rn-specific",
					NodeType:       "ra3.xlplus",
					State:          "active",
				})
			},
			body:         "Action=DescribeReservedNodes&Version=2012-12-01&ReservedNodeId=rn-specific",
			wantCode:     http.StatusOK,
			wantContains: []string{"rn-specific", "ra3.xlplus"},
		},
		{
			name:         "node_not_found",
			body:         "Action=DescribeReservedNodes&Version=2012-12-01&ReservedNodeId=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ReservedNodeNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeReservedNodeOfferings ----

func TestRedshiftHandler_DescribeReservedNodeOfferings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "list_all",
			body:         "Action=DescribeReservedNodeOfferings&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeReservedNodeOfferingsResponse", "dc2.large"},
		},
		{
			name: "get_specific_offering",
			body: "Action=DescribeReservedNodeOfferings&Version=2012-12-01" +
				"&ReservedNodeOfferingId=offering-dc2-large-1yr-allupfront",
			wantCode:     http.StatusOK,
			wantContains: []string{"offering-dc2-large-1yr-allupfront", "All Upfront"},
		},
		{
			name:         "offering_not_found",
			body:         "Action=DescribeReservedNodeOfferings&Version=2012-12-01&ReservedNodeOfferingId=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ReservedNodeOfferingNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- PurchaseReservedNodeOffering ----

func TestRedshiftHandler_PurchaseReservedNodeOffering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=PurchaseReservedNodeOffering&Version=2012-12-01" +
				"&ReservedNodeOfferingId=offering-dc2-large-1yr-allupfront" +
				"&ReservedNodeId=my-reserved-node&NodeCount=2",
			wantCode:     http.StatusOK,
			wantContains: []string{"PurchaseReservedNodeOfferingResponse", "my-reserved-node", "dc2.large"},
		},
		{
			name:         "missing_offering_id",
			body:         "Action=PurchaseReservedNodeOffering&Version=2012-12-01&ReservedNodeId=my-node",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "offering_not_found",
			body: "Action=PurchaseReservedNodeOffering&Version=2012-12-01" +
				"&ReservedNodeOfferingId=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ReservedNodeOfferingNotFound"},
		},
		{
			name: "invalid_node_count",
			body: "Action=PurchaseReservedNodeOffering&Version=2012-12-01" +
				"&ReservedNodeOfferingId=offering-dc2-large-1yr-allupfront" +
				"&NodeCount=notanumber",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeReservedNodeExchangeStatus ----

func TestRedshiftHandler_DescribeReservedNodeExchangeStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(b *redshift.InMemoryBackend) {
				b.AddReservedNodeInternal(&redshift.ReservedNode{
					ReservedNodeID: "rn-exchange",
					State:          "active",
				})
			},
			body: "Action=DescribeReservedNodeExchangeStatus&Version=2012-12-01" +
				"&ReservedNodeId=rn-exchange",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeReservedNodeExchangeStatusResponse", "Active"},
		},
		{
			name:         "missing_node_id",
			body:         "Action=DescribeReservedNodeExchangeStatus&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "node_not_found",
			body:         "Action=DescribeReservedNodeExchangeStatus&Version=2012-12-01&ReservedNodeId=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ReservedNodeNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- GetReservedNodeExchangeOfferings ----

func TestRedshiftHandler_GetReservedNodeExchangeOfferings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(b *redshift.InMemoryBackend) {
				b.AddReservedNodeInternal(&redshift.ReservedNode{
					ReservedNodeID: "rn-get-offerings",
					State:          "active",
				})
			},
			body: "Action=GetReservedNodeExchangeOfferings&Version=2012-12-01" +
				"&ReservedNodeId=rn-get-offerings",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetReservedNodeExchangeOfferingsResponse"},
		},
		{
			name:         "missing_node_id",
			body:         "Action=GetReservedNodeExchangeOfferings&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "node_not_found",
			body:         "Action=GetReservedNodeExchangeOfferings&Version=2012-12-01&ReservedNodeId=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ReservedNodeNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- GetReservedNodeExchangeConfigurationOptions ----

func TestRedshiftHandler_GetReservedNodeExchangeConfigurationOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=GetReservedNodeExchangeConfigurationOptions&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetReservedNodeExchangeConfigurationOptionsResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- Backend: ReservedNodeCount with purchase ----

func TestRedshiftBackend_PurchaseAddsNode(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	require.Equal(t, 0, redshift.ReservedNodeCount(b))

	h := redshift.NewHandler(b)
	rec := postRedshiftForm(t, h, "Action=PurchaseReservedNodeOffering&Version=2012-12-01"+
		"&ReservedNodeOfferingId=offering-dc2-large-1yr-allupfront"+
		"&ReservedNodeId=new-node")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, 1, redshift.ReservedNodeCount(b))
}
