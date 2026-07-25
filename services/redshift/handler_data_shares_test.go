package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- AssociateDataShareConsumer ----

func TestHandler_AssociateDataShareConsumer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift:us-east-1:000000000000:datashare:ds-001",
					ProducerArn:  "arn:aws:redshift:us-east-1:000000000000:namespace:ns-001",
				})
			},
			body: "Action=AssociateDataShareConsumer&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3Aus-east-1%3A000000000000%3Adatashare%3Ads-001" +
				"&ConsumerArn=arn%3Aaws%3Aredshift%3Aus-east-1%3A111111111111%3Anamespace%3Ans-002",
			wantCode:     http.StatusOK,
			wantContains: []string{"AssociateDataShareConsumerResponse"},
		},
		{
			name: "missing_data_share_arn",
			body: "Action=AssociateDataShareConsumer&Version=2012-12-01" +
				"&ConsumerArn=arn:aws:redshift:us-east-1:111111111111:namespace:ns-002",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "data_share_not_found",
			body:         "Action=AssociateDataShareConsumer&Version=2012-12-01&DataShareArn=nonexistent&ConsumerArn=consumer",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidDataShareFault"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)
			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- AuthorizeDataShare ----

func TestHandler_AuthorizeDataShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift:us-east-1:000000000000:datashare:ds-auth-001",
				})
			},
			body: "Action=AuthorizeDataShare&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3Aus-east-1%3A000000000000%3Adatashare%3Ads-auth-001" +
				"&ConsumerIdentifier=111111111111",
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeDataShareResponse"},
		},
		{
			name:         "missing_data_share_arn",
			body:         "Action=AuthorizeDataShare&Version=2012-12-01&ConsumerIdentifier=111111111111",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "missing_consumer_identifier",
			body: "Action=AuthorizeDataShare&Version=2012-12-01" +
				"&DataShareArn=arn:aws:redshift:us-east-1:000000000000:datashare:ds-001",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "data_share_not_found",
			body:         "Action=AuthorizeDataShare&Version=2012-12-01&DataShareArn=nonexistent&ConsumerIdentifier=consumer",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidDataShareFault"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)
			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- AssociateDataShareConsumer: missing DataShareArn ----

func TestAssociateDataShareConsumer_MissingArn(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h,
		"Action=AssociateDataShareConsumer&Version=2012-12-01&ConsumerArn=arn:aws:redshift::222:ns/ns1")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

// ---- AuthorizeDataShare: missing ConsumerIdentifier ----

func TestAuthorizeDataShare_MissingConsumerIdentifier(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h,
		"Action=AuthorizeDataShare&Version=2012-12-01&DataShareArn=arn:aws:redshift::123:datashare:ds1")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

// ---- DataShare: not found ----

func TestDataShare_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "authorize_data_share",
			body: "Action=AuthorizeDataShare&Version=2012-12-01" +
				"&DataShareArn=arn:nonexistent&ConsumerIdentifier=222222222222",
		},
		{
			name: "associate_data_share_consumer",
			body: "Action=AssociateDataShareConsumer&Version=2012-12-01" +
				"&DataShareArn=arn:nonexistent&ConsumerArn=arn:consumer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidDataShareFault")
		})
	}
}

// ---- DescribeDataShares ----

func TestHandler_DescribeDataShares(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeDataShares&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDataSharesResponse"},
		},
		{
			name: "with_data_share",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds1",
					ProducerArn:  "arn:aws:redshift::123:namespace:ns1",
				})
			},
			body:         "Action=DescribeDataShares&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDataSharesResponse", "ds1"},
		},
		{
			name: "specific_arn",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds2",
				})
			},
			body: "Action=DescribeDataShares&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Ads2",
			wantCode:     http.StatusOK,
			wantContains: []string{"ds2"},
		},
		{
			name: "specific_arn_not_found",
			body: "Action=DescribeDataShares&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Amissing",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeDataSharesForConsumer ----

func TestHandler_DescribeDataSharesForConsumer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeDataSharesForConsumer&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDataSharesForConsumerResponse"},
		},
		{
			name: "with_matching_consumer",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds3",
					DataShareAssociations: []redshift.DataShareAssociation{
						{ConsumerIdentifier: "consumer1", Status: "ACTIVE"},
					},
				})
			},
			body:         "Action=DescribeDataSharesForConsumer&Version=2012-12-01&ConsumerArn=consumer1",
			wantCode:     http.StatusOK,
			wantContains: []string{"ds3", "consumer1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeDataSharesForProducer ----

func TestHandler_DescribeDataSharesForProducer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeDataSharesForProducer&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDataSharesForProducerResponse"},
		},
		{
			name: "matching_producer",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds4",
					ProducerArn:  "producer1",
				})
			},
			body:         "Action=DescribeDataSharesForProducer&Version=2012-12-01&ProducerArn=producer1",
			wantCode:     http.StatusOK,
			wantContains: []string{"ds4", "producer1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeauthorizeDataShare ----

func TestHandler_DeauthorizeDataShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds5",
					DataShareAssociations: []redshift.DataShareAssociation{
						{ConsumerIdentifier: "consumer2", Status: "AUTHORIZED"},
					},
				})
			},
			body: "Action=DeauthorizeDataShare&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Ads5&ConsumerIdentifier=consumer2",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeauthorizeDataShareResponse", "DEAUTHORIZED"},
		},
		{
			name:     "missing_arn",
			body:     "Action=DeauthorizeDataShare&Version=2012-12-01&DataShareArn=&ConsumerIdentifier=consumer2",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not_found",
			body: "Action=DeauthorizeDataShare&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Amissing&ConsumerIdentifier=consumer2",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DisassociateDataShareConsumer ----

func TestHandler_DisassociateDataShareConsumer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds6",
					DataShareAssociations: []redshift.DataShareAssociation{
						{ConsumerIdentifier: "consumer3", Status: "ACTIVE"},
					},
				})
			},
			body: "Action=DisassociateDataShareConsumer&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Ads6&ConsumerArn=consumer3",
			wantCode:     http.StatusOK,
			wantContains: []string{"DisassociateDataShareConsumerResponse"},
		},
		{
			name:     "missing_arn",
			body:     "Action=DisassociateDataShareConsumer&Version=2012-12-01&DataShareArn=&ConsumerArn=consumer3",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- RejectDataShare ----

func TestHandler_RejectDataShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds7",
					DataShareAssociations: []redshift.DataShareAssociation{
						{ConsumerIdentifier: "consumer4", Status: "PENDING_AUTHORIZATION"},
					},
				})
			},
			body: "Action=RejectDataShare&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Ads7",
			wantCode:     http.StatusOK,
			wantContains: []string{"RejectDataShareResponse", "REJECTED"},
		},
		{
			name:     "missing_arn",
			body:     "Action=RejectDataShare&Version=2012-12-01&DataShareArn=",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not_found",
			body: "Action=RejectDataShare&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Amissing",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}
