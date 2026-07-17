package route53_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

func TestCreateCidrCollection_DuplicateName(t *testing.T) {
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

func createCidrCollectionForOpsTest(t *testing.T, h *route53.Handler, name string) string {
	t.Helper()

	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>%s</Name>
  <CallerReference>cidr-ref-%s</CallerReference>
</CreateCidrCollectionRequest>`, name, name)

	rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	type colResp struct {
		Collection struct {
			ID string `xml:"Id"`
		} `xml:"Collection"`
	}

	var resp colResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	return resp.Collection.ID
}

// TestRoute53_ListCidrCollections covers ListCidrCollections.
func TestRoute53_ListCidrCollections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
		setupCount   int
		wantCode     int
	}{
		{
			name:         "list_empty",
			setupCount:   0,
			wantCode:     http.StatusOK,
			wantContains: []string{"ListCidrCollectionsResponse"},
		},
		{
			name:         "list_with_collections",
			setupCount:   2,
			wantCode:     http.StatusOK,
			wantContains: []string{"ListCidrCollectionsResponse", "cidr-0", "cidr-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			for i := range tt.setupCount {
				createCidrCollectionForOpsTest(t, h, fmt.Sprintf("cidr-%d", i))
			}

			rec := send(t, h, http.MethodGet, "/2013-04-01/cidrcollection", "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_DeleteCidrCollection covers DeleteCidrCollection.
func TestRoute53_DeleteCidrCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		useID        string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "delete_success",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteCidrCollectionResponse"},
		},
		{
			name:     "delete_not_found",
			useID:    "nonexistent-id",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			id := tt.useID
			if id == "" {
				id = createCidrCollectionForOpsTest(t, h, "to-delete")
			}

			rec := send(t, h, http.MethodDelete, "/2013-04-01/cidrcollection/"+id, "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestExtractOperation_CreateCidrCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_cidr_collection",
			path:   "/2013-04-01/cidrcollection",
			method: http.MethodPost,
			wantOp: "CreateCidrCollection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestExtractOperation_ChangeCidrCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "change_cidr_collection",
			path:   "/2013-04-01/cidrcollection/ZXXX",
			method: http.MethodPost,
			wantOp: "ChangeCidrCollection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestChangeCidrCollection_ParseBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
		wantV2   bool
	}{
		{
			name: "with_changes_body",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<ChangeCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Changes>
    <Change>
      <LocationName>us-east-1</LocationName>
      <Action>PUT</Action>
      <CidrList><Cidr>10.0.0.0/8</Cidr></CidrList>
    </Change>
  </Changes>
</ChangeCidrCollectionRequest>`,
			wantCode: http.StatusOK,
			wantV2:   false,
		},
		{
			name:     "empty_body_still_works",
			body:     "",
			wantCode: http.StatusOK,
			wantV2:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			// First create a collection.
			createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>test-cidrs</Name>
  <CallerReference>cidr-ref-parsebody</CallerReference>
</CreateCidrCollectionRequest>`
			createRec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", createBody)
			require.Equal(t, http.StatusCreated, createRec.Code)

			type colResp struct {
				Collection struct {
					ID string `xml:"Id"`
				} `xml:"Collection"`
			}

			var colR colResp
			require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &colR))
			collectionID := colR.Collection.ID

			rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection/"+collectionID, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				assert.Contains(t, rec.Body.String(), "ChangeCidrCollectionResponse")
			}
		})
	}
}

func TestChangeCidrCollection_StoresLocations(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	col, err := b.CreateCidrCollection("my-cidrs", "ref")
	require.NoError(t, err)

	changes := []route53.CidrCollectionChange{
		{
			LocationName: "office",
			Action:       "PUT",
			CidrList:     []string{"192.168.1.0/24", "10.0.0.0/8"},
		},
	}

	updated, err := b.ChangeCidrCollection(col.ID, changes, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), updated.Version)

	// ListCidrLocations.
	locs, err := b.ListCidrLocations(col.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"office"}, locs)

	// ListCidrBlocks.
	blocks, err := b.ListCidrBlocks(col.ID, "office")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"192.168.1.0/24", "10.0.0.0/8"}, blocks)

	// DELETE_IF_EXISTS.
	_, err = b.ChangeCidrCollection(col.ID, []route53.CidrCollectionChange{
		{
			LocationName: "office",
			Action:       "DELETE_IF_EXISTS",
			CidrList:     []string{"10.0.0.0/8"},
		},
	}, nil)
	require.NoError(t, err)

	blocks, err = b.ListCidrBlocks(col.ID, "office")
	require.NoError(t, err)
	assert.Equal(t, []string{"192.168.1.0/24"}, blocks)
}

func TestListCidrBlocks_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	colID := createCidrCollectionForOpsTest(t, h, "list-cidr-test")

	// Change to add a location.
	changeBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Changes>
    <Change>
      <LocationName>datacenter</LocationName>
      <Action>PUT</Action>
      <CidrList>
        <Cidr>10.1.0.0/16</Cidr>
      </CidrList>
    </Change>
  </Changes>
</ChangeCidrCollectionRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection/"+colID, changeBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// List blocks.
	rec = send(t, h, http.MethodGet, "/2013-04-01/cidrcollection/"+colID+"/cidrblocks", "")
	require.Equal(t, http.StatusOK, rec.Code)

	// List locations.
	rec = send(t, h, http.MethodGet, "/2013-04-01/cidrcollection/"+colID+"/cidrlocations", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "datacenter")
}

func TestRoute53_CreateCidrCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_cidr_collection_success",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>my-cidrs</Name>
  <CallerReference>cidr-ref-1</CallerReference>
</CreateCidrCollectionRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateCidrCollectionResponse", "my-cidrs"},
		},
		{
			name: "create_cidr_collection_missing_name",
			body: `<?xml version="1.0" encoding="UTF-8"?>` +
				`<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
				`</CreateCidrCollectionRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_cidr_collection_invalid_xml",
			body:     "not-xml",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRoute53_ChangeCidrCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		collectionID string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "change_success",
			wantCode:     http.StatusOK,
			wantContains: []string{"ChangeCidrCollectionResponse"},
		},
		{
			name:         "change_not_found",
			collectionID: "ZNONEXISTENT",
			wantCode:     http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			collectionID := tt.collectionID

			if collectionID == "" {
				// Create a collection first.
				createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>test-cidrs</Name>
  <CallerReference>cidr-ref-change</CallerReference>
</CreateCidrCollectionRequest>`
				createRec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", createBody)
				require.Equal(t, http.StatusCreated, createRec.Code)

				type colResp struct {
					Collection struct {
						ID string `xml:"Id"`
					} `xml:"Collection"`
				}

				var resp colResp
				require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &resp))
				collectionID = resp.Collection.ID
			}

			rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection/"+collectionID, "")
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}
