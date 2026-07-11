package route53_test

// parity_sweep3_test.go — AWS-accuracy audit for Route53, parity sweep 3
// (gopherstack-8l0).
//
// Covers concrete deviations from AWS behaviour found in backend.go/handler.go:
//
//  1. Tag-family ops (ListTagsForResource, ListTagsForResources,
//     ChangeTagsForResource) never validated that the target resource
//     existed. ListTagsForResource[s] silently returned an empty tag set for
//     an unknown hosted zone/health check (200 OK), and
//     ChangeTagsForResource's existence check was real in the backend but
//     the handler discarded its error return, so tagging a nonexistent
//     resource silently "succeeded". Real AWS returns NoSuchHostedZone /
//     NoSuchHealthCheck (404) for all three ops. Fix: backend now takes a
//     resourceType and validates existence; the handler no longer discards
//     the backend error.
//
//  2. DisassociateVPCFromHostedZone for a VPC that isn't associated returned
//     the generic InvalidInput (400). Real AWS returns VPCAssociationNotFound
//     (404).
//
//  3. CreateTrafficPolicyInstance allowed multiple instances for the same
//     (hostedZoneID, name) pair. Real AWS returns
//     TrafficPolicyInstanceAlreadyExists (409).
//
//  4. Resource tags (b.tags) were never wired into Snapshot/Restore, so tags
//     silently vanished across a backend restore even though persistence was
//     otherwise enabled for the service.
//
//  5. Wire-code/status mismatches found by diffing every route53 sentinel
//     error against the aws-sdk-go-v2 route53 model
//     (types/errors.go + api-2.json httpStatusCode):
//     NoSuchCidrCollection -> NoSuchCidrCollectionException (404, code fix only)
//     NoSuchDelegationSet: 404 -> 400 (status fix only)
//     QueryLoggingConfigAlreadyExists: 400 -> 409
//     TrafficPolicyAlreadyExists: 400 -> 409 (covered in handler_accuracy_test.go)
//     KeySigningKeyNotInactive (fabricated code, doesn't exist in AWS) -> InvalidKeySigningKeyStatus
//     CreateKeySigningKey duplicate name: InvalidInput -> KeySigningKeyAlreadyExists (409)
//
//  6. CreateCidrCollection allowed multiple collections with the same name.
//     Real AWS returns CidrCollectionAlreadyExistsException (400).
//
//  7. ListTagsForResources' HTTP route was unreachable: the handler checked
//     for a bare "/2013-04-01/tags" POST, but that path fails the earlier
//     route53TagsPrefix ("/2013-04-01/tags/") HasPrefix check, and even if it
//     didn't, the real AWS request URI is POST /2013-04-01/tags/{ResourceType}
//     (no trailing ResourceId) per the botocore route53 model — a path this
//     handler previously misrouted to ChangeTagsForResource.
//
//  8. CreateHealthCheck accepted a CALCULATED health check whose
//     HealthThreshold exceeded the number of ChildHealthChecks (e.g.
//     threshold=5 with only 2 children), which AWS rejects with InvalidInput.

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// ---------------------------------------------------------------------------
// Issue #1 — tag-family ops must validate resource existence
// ---------------------------------------------------------------------------

func TestParitySweep3_ListTagsForResource_NoSuchResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantBody string
	}{
		{
			name:     "unknown_hostedzone",
			path:     "/2013-04-01/tags/hostedzone/ZDOESNOTEXIST",
			wantBody: "NoSuchHostedZone",
		},
		{
			name:     "unknown_healthcheck",
			path:     "/2013-04-01/tags/healthcheck/DOESNOTEXIST",
			wantBody: "NoSuchHealthCheck",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			rec := send(t, h, http.MethodGet, tt.path, "")
			assert.Equal(t, http.StatusNotFound, rec.Code,
				"ListTagsForResource on a nonexistent resource must 404, not silently return empty tags")
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestParitySweep3_ChangeTagsForResource_NoSuchResource(t *testing.T) {
	t.Parallel()

	const body = `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <AddTags><Tag><Key>env</Key><Value>prod</Value></Tag></AddTags>
</ChangeTagsForResourceRequest>`

	tests := []struct {
		name     string
		path     string
		wantBody string
	}{
		{
			name:     "unknown_hostedzone",
			path:     "/2013-04-01/tags/hostedzone/ZDOESNOTEXIST",
			wantBody: "NoSuchHostedZone",
		},
		{
			name:     "unknown_healthcheck",
			path:     "/2013-04-01/tags/healthcheck/DOESNOTEXIST",
			wantBody: "NoSuchHealthCheck",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			rec := send(t, h, http.MethodPost, tt.path, body)
			assert.Equal(t, http.StatusNotFound, rec.Code,
				"ChangeTagsForResource on a nonexistent resource must 404, not silently succeed")
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			// Verify the tag was genuinely not applied anywhere.
			listRec := send(t, h, http.MethodGet, tt.path, "")
			assert.Equal(t, http.StatusNotFound, listRec.Code)
		})
	}
}

func TestParitySweep3_ListTagsForResources_NoSuchResource(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<ListTagsForResourcesRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ResourceType>hostedzone</ResourceType>
  <ResourceIds>
    <ResourceId>` + zoneID + `</ResourceId>
    <ResourceId>ZDOESNOTEXIST</ResourceId>
  </ResourceIds>
</ListTagsForResourcesRequest>`

	// Real AWS ListTagsForResources URI is POST /2013-04-01/tags/{ResourceType}
	// (a bare "/2013-04-01/tags" request, with no ResourceType segment, is not
	// a valid Route53 endpoint at all).
	got := send(t, h, http.MethodPost, "/2013-04-01/tags/hostedzone", body)
	assert.Equal(t, http.StatusNotFound, got.Code,
		"batch tag lookup with any unknown resource ID must 404, not return partial results")
	assert.Contains(t, got.Body.String(), "NoSuchHostedZone")
}

func TestParitySweep3_TagRoundTrip_HealthCheck(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", createHealthCheckXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	hcID := extractHealthCheckID(t, rec.Body.String())

	tagsPath := "/2013-04-01/tags/healthcheck/" + hcID

	addBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <AddTags><Tag><Key>owner</Key><Value>net-team</Value></Tag></AddTags>
</ChangeTagsForResourceRequest>`

	addRec := send(t, h, http.MethodPost, tagsPath, addBody)
	require.Equal(t, http.StatusOK, addRec.Code,
		"ChangeTagsForResource on a real health check must still succeed")

	listRec := send(t, h, http.MethodGet, tagsPath, "")
	assert.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), "net-team")
}

// ---------------------------------------------------------------------------
// Issue #2 — DisassociateVPCFromHostedZone: not-associated VPC
// ---------------------------------------------------------------------------

func TestParitySweep3_DisassociateVPC_NotAssociated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "vpc_never_associated_returns_not_found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			hz, err := b.CreateHostedZone("example.com", "ref-priv", "", true)
			require.NoError(t, err)

			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-aaa", "us-east-1"))

			// vpc-bbb was never associated, so the disassociate must be
			// rejected with VPCAssociationNotFound, not the generic
			// InvalidInput this backend previously returned.
			err = b.DisassociateVPCFromHostedZone(hz.ID, "vpc-bbb")
			require.Error(t, err)
			assert.ErrorIs(t, err, route53.ErrVPCAssociationNotFound)
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #3 — CreateTrafficPolicyInstance duplicate (zone, name)
// ---------------------------------------------------------------------------

func TestParitySweep3_CreateTrafficPolicyInstance_Duplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "duplicate_name_in_zone_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			hz, err := b.CreateHostedZone("example.com", "ref-tpi", "", false)
			require.NoError(t, err)

			tp, err := b.CreateTrafficPolicy(
				"tpi-dup-policy",
				`{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}`,
				"",
			)
			require.NoError(t, err)

			_, err = b.CreateTrafficPolicyInstance(hz.ID, "www.example.com", tp.ID, tp.Version, 60)
			require.NoError(t, err)

			_, err = b.CreateTrafficPolicyInstance(hz.ID, "www.example.com", tp.ID, tp.Version, 60)
			require.Error(t, err)
			assert.ErrorIs(t, err, route53.ErrTrafficPolicyInstanceAlreadyExists)
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #4 — tags must survive Snapshot/Restore
// ---------------------------------------------------------------------------

func TestParitySweep3_TagsPersistAcrossSnapshotRestore(t *testing.T) {
	t.Parallel()

	original := route53.NewInMemoryBackend()

	hz, err := original.CreateHostedZone("example.com", "ref-tags-persist", "", false)
	require.NoError(t, err)

	require.NoError(t, original.ChangeTagsForResource(
		"hostedzone", hz.ID, map[string]string{"env": "prod"}, nil,
	))

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := route53.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	tags, err := fresh.ListTagsForResource("hostedzone", hz.ID)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"],
		"resource tags must be wired into backendSnapshot and survive a restore")
}

// ---------------------------------------------------------------------------
// Issue #5 — wire-code / http-status corrections
// ---------------------------------------------------------------------------

func TestParitySweep3_ErrorWireCodesAndStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *route53.Handler)
		name string
	}{
		{
			name: "unknown_cidr_collection_uses_exception_suffix",
			run: func(t *testing.T, h *route53.Handler) {
				t.Helper()

				// GET /cidrcollection/{Id} (no "/cidrblocks" suffix) routes to
				// ListCidrLocations, which needs no request body.
				rec := send(t, h, http.MethodGet, "/2013-04-01/cidrcollection/DOESNOTEXIST", "")
				assert.Equal(t, http.StatusNotFound, rec.Code)
				assert.Contains(t, rec.Body.String(), "NoSuchCidrCollectionException")
			},
		},
		{
			name: "unknown_delegation_set_is_400",
			run: func(t *testing.T, h *route53.Handler) {
				t.Helper()

				rec := send(t, h, http.MethodDelete, "/2013-04-01/delegationset/N-DOESNOTEXIST", "")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "NoSuchDelegationSet")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			tt.run(t, h)
		})
	}
}

func TestParitySweep3_QueryLoggingConfigAlreadyExists_Is409(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>` + zoneID + `</HostedZoneId>
  <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123456789012:log-group:/r53/test</CloudWatchLogsLogGroupArn>
</CreateQueryLoggingConfigRequest>`

	first := send(t, h, http.MethodPost, "/2013-04-01/queryloggingconfig", body)
	require.Equal(t, http.StatusCreated, first.Code)

	second := send(t, h, http.MethodPost, "/2013-04-01/queryloggingconfig", body)
	assert.Equal(t, http.StatusConflict, second.Code,
		"real AWS QueryLoggingConfigAlreadyExists has httpStatusCode 409, not 400")
	assert.Contains(t, second.Body.String(), "QueryLoggingConfigAlreadyExists")
}

func TestParitySweep3_HostedZoneAlreadyExists_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	const body1 = `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>example.com</Name>
  <CallerReference>shared-ref</CallerReference>
</CreateHostedZoneRequest>`

	const body2 = `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>different.com</Name>
  <CallerReference>shared-ref</CallerReference>
</CreateHostedZoneRequest>`

	first := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", body1)
	require.Equal(t, http.StatusCreated, first.Code)

	second := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", body2)
	assert.Equal(t, http.StatusConflict, second.Code)
	assert.Contains(t, second.Body.String(), "HostedZoneAlreadyExists")
}

func TestParitySweep3_HealthCheckAlreadyExists_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	const body1 = `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>shared-hc-ref</CallerReference>
  <HealthCheckConfig><Type>HTTP</Type><Port>80</Port></HealthCheckConfig>
</CreateHealthCheckRequest>`

	const body2 = `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>shared-hc-ref</CallerReference>
  <HealthCheckConfig><Type>HTTPS</Type><Port>443</Port></HealthCheckConfig>
</CreateHealthCheckRequest>`

	first := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body1)
	require.Equal(t, http.StatusCreated, first.Code)

	second := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body2)
	assert.Equal(t, http.StatusConflict, second.Code)
	assert.Contains(t, second.Body.String(), "HealthCheckAlreadyExists")
}

// ---------------------------------------------------------------------------
// Issue #6 — CreateCidrCollection duplicate name
// ---------------------------------------------------------------------------

func TestParitySweep3_CreateCidrCollection_DuplicateName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "duplicate_collection_name_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>dup-cidrs</Name>
  <CallerReference>cidr-ref-dup</CallerReference>
</CreateCidrCollectionRequest>`

			first := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", body)
			require.Equal(t, http.StatusCreated, first.Code)

			second := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", body)
			assert.Equal(t, http.StatusBadRequest, second.Code,
				"real AWS CidrCollectionAlreadyExistsException has httpStatusCode 400")
			assert.Contains(t, second.Body.String(), "CidrCollectionAlreadyExistsException")
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #7 — ListTagsForResources HTTP routing was unreachable/misrouted
// ---------------------------------------------------------------------------

func TestParitySweep3_ListTagsForResources_HTTPRoute(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	addBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <AddTags><Tag><Key>env</Key><Value>prod</Value></Tag></AddTags>
</ChangeTagsForResourceRequest>`
	addRec := send(t, h, http.MethodPost, "/2013-04-01/tags/hostedzone/"+zoneID, addBody)
	require.Equal(t, http.StatusOK, addRec.Code)

	body := `<?xml version="1.0" encoding="UTF-8"?>
<ListTagsForResourcesRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ResourceType>hostedzone</ResourceType>
  <ResourceIds><ResourceId>` + zoneID + `</ResourceId></ResourceIds>
</ListTagsForResourcesRequest>`

	// Real AWS ListTagsForResources request URI: POST /2013-04-01/tags/{ResourceType}.
	got := send(t, h, http.MethodPost, "/2013-04-01/tags/hostedzone", body)
	assert.Equal(t, http.StatusOK, got.Code,
		"ListTagsForResources must be reachable at its real AWS request URI")
	assert.Contains(t, got.Body.String(), "ListTagsForResourcesResponse")
	assert.Contains(t, got.Body.String(), "prod")
}

// ---------------------------------------------------------------------------
// Issue #8 — CALCULATED health check HealthThreshold vs ChildHealthChecks
// ---------------------------------------------------------------------------

func TestParitySweep3_CreateHealthCheck_CalculatedThresholdExceedsChildren(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "threshold_exceeds_child_count_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			child1, err := b.CreateHealthCheck("child-ref-1", route53.HealthCheckConfig{
				Type: route53.HealthCheckTypeHTTP,
				Port: 80,
			})
			require.NoError(t, err)

			child2, err := b.CreateHealthCheck("child-ref-2", route53.HealthCheckConfig{
				Type: route53.HealthCheckTypeHTTP,
				Port: 80,
			})
			require.NoError(t, err)

			const thresholdExceedsChildCount = 5

			_, err = b.CreateHealthCheck("calc-ref-1", route53.HealthCheckConfig{
				Type:              route53.HealthCheckTypeCalculated,
				ChildHealthChecks: []string{child1.ID, child2.ID},
				HealthThreshold:   thresholdExceedsChildCount,
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, route53.ErrInvalidInput)
		})
	}
}
