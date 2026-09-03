package cloudfront_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestAnycastIPList_NameUniqueness verifies CreateAnycastIPList rejects a duplicate
// name and that DeleteAnycastIPList frees the name for reuse.
func TestAnycastIPList_NameUniqueness(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

	first, err := b.CreateAnycastIPList("dup-name", 3, nil)
	require.NoError(t, err)

	_, err = b.CreateAnycastIPList("dup-name", 5, nil)
	require.Error(t, err)

	require.NoError(t, b.DeleteAnycastIPList(first.ID))

	// Name is free again after delete.
	_, err = b.CreateAnycastIPList("dup-name", 5, nil)
	require.NoError(t, err)
}

// TestAnycastIPList_GeneratedIPs verifies AnycastIPs is populated with IPCount entries on
// create, and that UpdateAnycastIPList -- which real clients can only use to change
// IpAddressType, never IpCount (cloudfront@v1.67.4 api_op_UpdateAnycastIpList.go:28-56 has no
// IpCount member) -- leaves the count and generated IPs untouched.
func TestAnycastIPList_GeneratedIPs(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

	list, err := b.CreateAnycastIPList("ips-list", 4, nil)
	require.NoError(t, err)
	assert.Len(t, list.AnycastIPs, 4)
	assert.NotEmpty(t, list.ETag)

	oldETag := list.ETag
	updated, err := b.UpdateAnycastIPList(list.ID, "dualstack", nil)
	require.NoError(t, err)
	assert.Equal(t, "dualstack", updated.IPAddressType)
	assert.Len(t, updated.AnycastIPs, 4)
	assert.Equal(t, int32(4), updated.IPCount)
	assert.NotEqual(t, oldETag, updated.ETag)

	// An empty IpAddressType (unset) leaves the current type, IPs, and count unchanged.
	unchanged, err := b.UpdateAnycastIPList(list.ID, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "dualstack", unchanged.IPAddressType)
	assert.Len(t, unchanged.AnycastIPs, 4)

	_, err = b.UpdateAnycastIPList(list.ID, "bogus", nil)
	require.Error(t, err)
}

// TestAnycastIPList_IfMatchEnforcement mirrors the trust store If-Match test:
// a mismatched If-Match header on update/delete is rejected with 412.
func TestAnycastIPList_IfMatchEnforcement(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	const prefix = "/2020-05-31/"

	createRec := doXML(
		t,
		h,
		http.MethodPost,
		prefix+"anycast-ip-list",
		[]byte(
			`<CreateAnycastIpListRequest><Name>etag-list</Name><IpCount>3</IpCount></CreateAnycastIpListRequest>`,
		),
	)
	require.Equal(t, http.StatusCreated, createRec.Code)
	id := extractXMLID(t, createRec.Body.String())
	etag := createRec.Header().Get("ETag")
	require.NotEmpty(t, etag)

	badUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"anycast-ip-list/"+id,
		[]byte(`<UpdateAnycastIpListRequest><IpAddressType>ipv6</IpAddressType></UpdateAnycastIpListRequest>`),
		map[string]string{"If-Match": "bogus-etag"})
	assert.Equal(t, http.StatusPreconditionFailed, badUpdate.Code)

	goodUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"anycast-ip-list/"+id,
		[]byte(`<UpdateAnycastIpListRequest><IpAddressType>ipv6</IpAddressType></UpdateAnycastIpListRequest>`),
		map[string]string{"If-Match": etag})
	require.Equal(t, http.StatusOK, goodUpdate.Code)
	assert.Contains(t, goodUpdate.Body.String(), "<IpAddressType>ipv6</IpAddressType>")
	newETag := goodUpdate.Header().Get("ETag")
	assert.NotEmpty(t, newETag)
	assert.NotEqual(t, etag, newETag)

	badDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"anycast-ip-list/"+id, nil,
		map[string]string{"If-Match": "bogus-etag"})
	assert.Equal(t, http.StatusPreconditionFailed, badDelete.Code)

	goodDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"anycast-ip-list/"+id, nil,
		map[string]string{"If-Match": newETag})
	assert.Equal(t, http.StatusNoContent, goodDelete.Code)
}

// TestAnycastIPList_Tags verifies tags supplied on create are stored and retrievable
// via the generic ListTagsForResource endpoint.
func TestAnycastIPList_Tags(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	const prefix = "/2020-05-31/"

	createRec := doXML(t, h, http.MethodPost, prefix+"anycast-ip-list",
		[]byte(`<CreateAnycastIpListRequest><Name>tagged-list</Name><IpCount>2</IpCount>`+
			`<Tags><Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`+
			`</CreateAnycastIpListRequest>`))
	require.Equal(t, http.StatusCreated, createRec.Code)
	id := extractXMLID(t, createRec.Body.String())
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:anycast-ip-list/%s", id)

	listRec := doXML(t, h, http.MethodGet, prefix+"tagging?Resource="+arn, nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), "<Key>env</Key>")
	assert.Contains(t, listRec.Body.String(), "<Value>prod</Value>")
}

// TestAnycastIPList_PersistenceRoundTrip verifies AnycastIPList survives a
// Snapshot/Restore cycle including its ETag, tags, generated IPs, and name-uniqueness index.
func TestAnycastIPList_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	list, err := b.CreateAnycastIPList("persist-list", 3, nil)
	require.NoError(t, err)

	h := cloudfront.NewHandler(b)
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h2 := cloudfront.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	restored, err := b2.GetAnycastIPList(list.ID)
	require.NoError(t, err)
	assert.Equal(t, list.ETag, restored.ETag)
	assert.Len(t, restored.AnycastIPs, 3)

	// The name-uniqueness index survived restore: creating a second list with the same name
	// still fails.
	_, err = b2.CreateAnycastIPList("persist-list", 5, nil)
	require.Error(t, err)
}

// TestUpdateAnycastIPList_RealClient drives UpdateAnycastIpList through a real
// aws-sdk-go-v2 client (cloudfront@v1.67.4's UpdateAnycastIpListInput has no
// IpCount member at all -- api_op_UpdateAnycastIpList.go:28-56 -- so a real
// client can never send one; the only settable field is IpAddressType).
func TestUpdateAnycastIPList_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(*testing.T, *cfsdk.Client, string)
		name  string
	}{
		{
			name: "ip_count_survives_update",
			check: func(t *testing.T, client *cfsdk.Client, id string) {
				t.Helper()
				got, err := client.GetAnycastIpList(t.Context(), &cfsdk.GetAnycastIpListInput{Id: aws.String(id)})
				require.NoError(t, err)
				assert.Equal(t, int32(4), aws.ToInt32(got.AnycastIpList.IpCount))
				assert.Len(t, got.AnycastIpList.AnycastIps, 4)
			},
		},
		{
			name: "ip_address_type_applied",
			check: func(t *testing.T, client *cfsdk.Client, id string) {
				t.Helper()
				got, err := client.GetAnycastIpList(t.Context(), &cfsdk.GetAnycastIpListInput{Id: aws.String(id)})
				require.NoError(t, err)
				assert.Equal(t, types.IpAddressTypeDualStack, got.AnycastIpList.IpAddressType)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestCloudFrontClient(t, h)

			created, err := client.CreateAnycastIpList(t.Context(), &cfsdk.CreateAnycastIpListInput{
				Name:    aws.String("real-client-" + tt.name),
				IpCount: aws.Int32(4),
			})
			require.NoError(t, err)

			_, err = client.UpdateAnycastIpList(t.Context(), &cfsdk.UpdateAnycastIpListInput{
				Id:            created.AnycastIpList.Id,
				IfMatch:       created.ETag,
				IpAddressType: types.IpAddressTypeDualStack,
			})
			require.NoError(t, err)

			tt.check(t, client, aws.ToString(created.AnycastIpList.Id))
		})
	}
}

// TestAnycastIPList_LastModifiedTime_RealClient verifies LastModifiedTime is populated on
// AnycastIpList (Create/Get) and AnycastIpListSummary (List) -- both required members of the
// real output that gopherstack never set (cloudfront@v1.67.4 types/types.go:188-190, 271-274).
func TestAnycastIPList_LastModifiedTime_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	before := time.Now().Add(-time.Minute)

	created, err := client.CreateAnycastIpList(t.Context(), &cfsdk.CreateAnycastIpListInput{
		Name:    aws.String("lmt-list"),
		IpCount: aws.Int32(3),
	})
	require.NoError(t, err)
	require.NotNil(t, created.AnycastIpList.LastModifiedTime)
	assert.True(t, created.AnycastIpList.LastModifiedTime.After(before))

	got, err := client.GetAnycastIpList(t.Context(), &cfsdk.GetAnycastIpListInput{
		Id: created.AnycastIpList.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, got.AnycastIpList.LastModifiedTime)

	listed, err := client.ListAnycastIpLists(t.Context(), &cfsdk.ListAnycastIpListsInput{})
	require.NoError(t, err)
	require.Len(t, listed.AnycastIpLists.Items, 1)
	assert.NotNil(t, listed.AnycastIpLists.Items[0].LastModifiedTime)
}

// TestListAnycastIPLists_Pagination_RealClient verifies MaxItems/IsTruncated/NextMarker/Marker
// paging over ListAnycastIpLists, previously absent (cloudfront@v1.67.4 types/types.go:214-249,
// AnycastIpListCollection).
func TestListAnycastIPLists_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	for i := range 3 {
		_, err := client.CreateAnycastIpList(t.Context(), &cfsdk.CreateAnycastIpListInput{
			Name:    aws.String(fmt.Sprintf("page-list-%d", i)),
			IpCount: aws.Int32(3),
		})
		require.NoError(t, err)
	}

	first, err := client.ListAnycastIpLists(t.Context(), &cfsdk.ListAnycastIpListsInput{
		MaxItems: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, first.AnycastIpLists.Items, 2)
	assert.True(t, aws.ToBool(first.AnycastIpLists.IsTruncated))
	require.NotEmpty(t, aws.ToString(first.AnycastIpLists.NextMarker))

	second, err := client.ListAnycastIpLists(t.Context(), &cfsdk.ListAnycastIpListsInput{
		Marker: first.AnycastIpLists.NextMarker,
	})
	require.NoError(t, err)
	assert.Len(t, second.AnycastIpLists.Items, 1)
	assert.False(t, aws.ToBool(second.AnycastIpLists.IsTruncated))
}

// TestAnycastIPList_IpamCidrConfigs_RealClient verifies IpamCidrConfigs on
// CreateAnycastIpListInput and UpdateAnycastIpListInput round-trips through the stored
// AnycastIpList.IpamConfig verbatim -- gopherstack stores it as given rather than emulating
// IPAM pool allocation (cloudfront@v1.67.4 types/types.go:3732-3771).
func TestAnycastIPList_IpamCidrConfigs_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreateAnycastIpList(t.Context(), &cfsdk.CreateAnycastIpListInput{
		Name:    aws.String("ipam-list"),
		IpCount: aws.Int32(2),
		IpamCidrConfigs: []types.IpamCidrConfig{
			{
				Cidr:        aws.String("10.0.0.0/24"),
				IpamPoolArn: aws.String("arn:aws:ec2::123456789012:ipam-pool/ipam-pool-0123"),
				AnycastIp:   aws.String("15.1.2.3"),
				Status:      types.IpamCidrStatusProvisioned,
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.AnycastIpList.IpamConfig)
	require.Len(t, created.AnycastIpList.IpamConfig.IpamCidrConfigs, 1)

	got := created.AnycastIpList.IpamConfig.IpamCidrConfigs[0]
	assert.Equal(t, "10.0.0.0/24", aws.ToString(got.Cidr))
	assert.Equal(t, "arn:aws:ec2::123456789012:ipam-pool/ipam-pool-0123", aws.ToString(got.IpamPoolArn))
	assert.Equal(t, "15.1.2.3", aws.ToString(got.AnycastIp))
	assert.Equal(t, types.IpamCidrStatusProvisioned, got.Status)
	assert.Equal(t, int32(1), aws.ToInt32(created.AnycastIpList.IpamConfig.Quantity))

	updated, err := client.UpdateAnycastIpList(t.Context(), &cfsdk.UpdateAnycastIpListInput{
		Id:      created.AnycastIpList.Id,
		IfMatch: created.ETag,
		IpamCidrConfigs: []types.IpamCidrConfig{
			{
				Cidr:        aws.String("10.0.1.0/24"),
				IpamPoolArn: aws.String("arn:aws:ec2::123456789012:ipam-pool/ipam-pool-0456"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, updated.AnycastIpList.IpamConfig.IpamCidrConfigs, 1)
	assert.Equal(t, "10.0.1.0/24", aws.ToString(updated.AnycastIpList.IpamConfig.IpamCidrConfigs[0].Cidr))
}

// ---------------------------------------------------------------------------
// ContinuousDeploymentPolicy
// ---------------------------------------------------------------------------

// TestAnycastIPList_CRUD tests anycast IP list Get/List/Update/Delete.
func TestAnycastIPList_CRUD(t *testing.T) {
	t.Parallel()
	b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := cloudfront.NewHandler(b)
	const prefix = "/2020-05-31/"

	// Create via existing handler
	createBody := `<CreateAnycastIpListRequest><Name>my-list</Name><IpCount>5</IpCount></CreateAnycastIpListRequest>`
	out := cfOK(t, h, http.MethodPost, prefix+"anycast-ip-list", createBody)
	id := extractXMLID(t, out)
	if id == "" {
		t.Fatalf("expected Id: %s", out)
	}

	// Get
	out2 := cfOK(t, h, http.MethodGet, prefix+"anycast-ip-list/"+id, "")
	if extractXMLID(t, out2) != id {
		t.Errorf("get mismatch: %s", out2)
	}

	// List
	out3 := cfOK(t, h, http.MethodGet, prefix+"anycast-ip-list", "")
	if !strings.Contains(out3, id) {
		t.Errorf("list missing id: %s", out3)
	}

	// Update
	updateOut := cfOK(t, h, http.MethodPut, prefix+"anycast-ip-list/"+id,
		`<UpdateAnycastIpListRequest><IpAddressType>dualstack</IpAddressType></UpdateAnycastIpListRequest>`)
	if !strings.Contains(updateOut, "<IpAddressType>dualstack</IpAddressType>") {
		t.Errorf("update missing IpAddressType: %s", updateOut)
	}

	// Delete
	cfOK(t, h, http.MethodDelete, prefix+"anycast-ip-list/"+id, "")
}

// TestCreateAnycastIPList covers the CreateAnycastIPList operation.
func TestCreateAnycastIPList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "create_anycast_ip_list_success",
			body: []byte(
				`<CreateAnycastIpListRequest><Name>my-anycast-list</Name><IpCount>5</IpCount></CreateAnycastIpListRequest>`,
			),
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<AnycastIPList")
				assert.Contains(t, rec.Body.String(), "<Name>my-anycast-list</Name>")
				assert.Contains(t, rec.Body.String(), "<Status>Deployed</Status>")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name: "create_anycast_ip_list_empty_name",
			body: []byte(
				`<CreateAnycastIpListRequest><Name></Name><IpCount>5</IpCount></CreateAnycastIpListRequest>`,
			),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
		{
			name:       "create_anycast_ip_list_malformed_xml",
			body:       []byte(`<<<not xml`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "MalformedXML")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/anycast-ip-list", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestAnycastIPList_IPCountValidation verifies IPCount must be positive.
func TestAnycastIPList_IPCountValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "zero_count",
			body:       `<CreateAnycastIpListRequest><Name>test-list</Name><IpCount>0</IpCount></CreateAnycastIpListRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "negative_count",
			body: `<CreateAnycastIpListRequest><Name>test-list-neg</Name>` +
				`<IpCount>-5</IpCount></CreateAnycastIpListRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "valid_count",
			body: `<CreateAnycastIpListRequest><Name>test-list-valid</Name>` +
				`<IpCount>10</IpCount></CreateAnycastIpListRequest>`,
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/anycast-ip-list", []byte(tt.body))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestListAnycastIPLists_ItemShape_RealClient is a regression test for gopherstack-21my:
// ListAnycastIpLists' item struct (ailSummary, handler_anycast_ip_lists.go) omitted ETag and
// IpamConfig entirely, even though the real AnycastIpListSummary deserializer
// (awsRestxml_deserializeDocumentAnycastIpListSummary) reads both and the sibling
// GetAnycastIpList (anycastIPListXML) already emits IpamConfig correctly from the same
// backing AnycastIPList.IpamCidrConfigs field -- the "Get right, List wrong" trap. Seeds
// two lists with distinguishable IPAM CIDR configs and asserts both round-trip.
func TestListAnycastIPLists_ItemShape_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	mk := func(name, cidr, poolARN string) *cfsdk.CreateAnycastIpListOutput {
		out, err := client.CreateAnycastIpList(t.Context(), &cfsdk.CreateAnycastIpListInput{
			Name:    aws.String(name),
			IpCount: aws.Int32(2),
			IpamCidrConfigs: []types.IpamCidrConfig{
				{Cidr: aws.String(cidr), IpamPoolArn: aws.String(poolARN)},
			},
		})
		require.NoError(t, err)

		return out
	}

	first := mk("list-shape-ail-1", "10.0.0.0/24", "arn:aws:ec2::123456789012:ipam-pool/pool-1")
	second := mk("list-shape-ail-2", "10.0.1.0/24", "arn:aws:ec2::123456789012:ipam-pool/pool-2")

	listed, err := client.ListAnycastIpLists(t.Context(), &cfsdk.ListAnycastIpListsInput{})
	require.NoError(t, err)
	require.NotNil(t, listed.AnycastIpLists)
	require.Len(t, listed.AnycastIpLists.Items, 2)

	byID := make(map[string]types.AnycastIpListSummary, 2)
	for _, item := range listed.AnycastIpLists.Items {
		require.NotNil(t, item.Id)
		byID[*item.Id] = item
	}

	item1, ok := byID[*first.AnycastIpList.Id]
	require.True(t, ok)
	assert.NotEmpty(t, aws.ToString(item1.ETag), "ETag must round-trip, not decode empty")
	require.NotNil(t, item1.IpamConfig)
	require.Len(t, item1.IpamConfig.IpamCidrConfigs, 1)
	assert.Equal(t, "10.0.0.0/24", aws.ToString(item1.IpamConfig.IpamCidrConfigs[0].Cidr))
	assert.Equal(
		t,
		"arn:aws:ec2::123456789012:ipam-pool/pool-1",
		aws.ToString(item1.IpamConfig.IpamCidrConfigs[0].IpamPoolArn),
	)

	item2, ok := byID[*second.AnycastIpList.Id]
	require.True(t, ok)
	require.NotNil(t, item2.IpamConfig)
	require.Len(t, item2.IpamConfig.IpamCidrConfigs, 1)
	assert.Equal(t, "10.0.1.0/24", aws.ToString(item2.IpamConfig.IpamCidrConfigs[0].Cidr))
}
