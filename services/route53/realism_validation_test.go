package route53_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const realismChangeXML = `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch><Changes><Change><Action>UPSERT</Action>
    <ResourceRecordSet><Name>host.example.com</Name><Type>A</Type><TTL>300</TTL>
      <ResourceRecords><ResourceRecord><Value>1.2.3.4</Value></ResourceRecord></ResourceRecords>
    </ResourceRecordSet></Change></Changes></ChangeBatch>
</ChangeResourceRecordSetsRequest>`

func TestRRSetPath_TrailingSlash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		suffix string
		body   string
	}{
		{name: "change with slash", method: http.MethodPost, suffix: "/rrset/", body: realismChangeXML},
		{name: "change without slash", method: http.MethodPost, suffix: "/rrset", body: realismChangeXML},
		{name: "list with slash", method: http.MethodGet, suffix: "/rrset/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
			require.Equal(t, http.StatusCreated, rec.Code)
			zoneID := extractZoneID(t, rec.Body.String())

			rec = send(t, h, tt.method, "/2013-04-01/hostedzone/"+zoneID+tt.suffix, tt.body)
			assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		})
	}
}

func TestCreateHostedZone_InvalidDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		zone     string
		wantCode int
	}{
		{name: "empty label", zone: "bad..name", wantCode: http.StatusBadRequest},
		{name: "space", zone: "bad name.com", wantCode: http.StatusBadRequest},
		{name: "long label", zone: strings.Repeat("a", 64) + ".com", wantCode: http.StatusBadRequest},
		{name: "valid", zone: "ok-name.example.com", wantCode: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			body := strings.Replace(createZoneXML, "<Name>example.com</Name>", "<Name>"+tt.zone+"</Name>", 1)
			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", body)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "<Code>InvalidDomainName</Code>")
				assert.NotContains(t, rec.Body.String(), "<Message>InvalidDomainName")
			}
		})
	}
}
