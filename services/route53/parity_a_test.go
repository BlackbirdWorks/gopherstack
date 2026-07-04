package route53_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_CreateHostedZone_RequiresNameAndCallerReference verifies that
// CreateHostedZone rejects requests missing Name or CallerReference.
// Real AWS returns 400 InvalidInput for both cases; the emulator had the
// backend validation but lacked handler-level parity tests.
func TestParity_CreateHostedZone_RequiresNameAndCallerReference(t *testing.T) {
	t.Parallel()

	const path = "/2013-04-01/hostedzone"

	tests := []struct {
		body     string
		name     string
		wantCode int
	}{
		{
			name: "missing_zone_name_rejected",
			body: `<?xml version="1.0" encoding="UTF-8"?>` +
				`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
				`<CallerReference>ref-no-name</CallerReference>` +
				`</CreateHostedZoneRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_caller_reference_rejected",
			body: `<?xml version="1.0" encoding="UTF-8"?>` +
				`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
				`<Name>example.com</Name>` +
				`</CreateHostedZoneRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "valid_request_accepted",
			body: `<?xml version="1.0" encoding="UTF-8"?>` +
				`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
				`<Name>parity-test.com</Name>` +
				`<CallerReference>ref-parity-ok</CallerReference>` +
				`</CreateHostedZoneRequest>`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateHostedZone status for case %q", tt.name)

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidInput",
					"expected InvalidInput error code")
			}
		})
	}
}

// TestParity_CreateHostedZone_CallerReferenceIdempotency verifies that
// reusing the same CallerReference returns the existing zone rather than
// creating a duplicate. Real AWS guarantees this idempotency behavior.
func TestParity_CreateHostedZone_CallerReferenceIdempotency(t *testing.T) {
	t.Parallel()

	const path = "/2013-04-01/hostedzone"

	body := `<?xml version="1.0" encoding="UTF-8"?>` +
		`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
		`<Name>idem-test.com</Name>` +
		`<CallerReference>ref-idem-1</CallerReference>` +
		`</CreateHostedZoneRequest>`

	h := newHandler(t)

	rec1 := send(t, h, http.MethodPost, path, body)
	assert.Equal(t, http.StatusCreated, rec1.Code, "first create should succeed")

	zoneID := extractZoneID(t, rec1.Body.String())

	rec2 := send(t, h, http.MethodPost, path, body)
	assert.Equal(t, http.StatusCreated, rec2.Code,
		"second create with same CallerReference should return existing zone")

	zoneID2 := extractZoneID(t, rec2.Body.String())
	assert.Equal(t, zoneID, zoneID2,
		"same CallerReference should return the same zone ID both times")
}
