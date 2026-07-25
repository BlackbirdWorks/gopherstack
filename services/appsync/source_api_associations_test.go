package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestMergedAPI_CreateAssociation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	src, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	merged, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateMergedGraphqlAPI(src.APIID, merged.APIID, "source association", "")
	require.NoError(t, err)
	assert.NotEmpty(t, assoc.AssociationID)
	assert.Equal(t, src.APIID, assoc.SourceAPIID)
	assert.Equal(t, merged.APIID, assoc.MergedAPIID)

	got, err := b.GetSourceAPIAssociation(merged.APIID, assoc.AssociationID)
	require.NoError(t, err)
	assert.Equal(t, assoc.AssociationID, got.AssociationID)
}

func TestAssociateMergedGraphqlAPI_MergeType_MANUAL(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	src, err := b.CreateGraphqlAPI("Src", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	merged, err := b.CreateGraphqlAPI("Merged", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateMergedGraphqlAPI(src.APIID, merged.APIID, "desc", "MANUAL_MERGE")
	require.NoError(t, err)
	require.NotNil(t, assoc.SourceAPIAssociationConfig)
	assert.Equal(t, "MANUAL_MERGE", assoc.SourceAPIAssociationConfig.MergeType)
}

func TestAssociateMergedGraphqlAPI_MergeType_AUTO(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	src, err := b.CreateGraphqlAPI("Src", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	merged, err := b.CreateGraphqlAPI("Merged", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateMergedGraphqlAPI(src.APIID, merged.APIID, "", "AUTO_MERGE")
	require.NoError(t, err)
	require.NotNil(t, assoc.SourceAPIAssociationConfig)
	assert.Equal(t, "AUTO_MERGE", assoc.SourceAPIAssociationConfig.MergeType)
}

func TestAssociateMergedGraphqlAPI_MergeType_DefaultManual(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	src, err := b.CreateGraphqlAPI("Src", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	merged, err := b.CreateGraphqlAPI("Merged", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateMergedGraphqlAPI(src.APIID, merged.APIID, "", "")
	require.NoError(t, err)
	require.NotNil(t, assoc.SourceAPIAssociationConfig)
	assert.Equal(t, "MANUAL_MERGE", assoc.SourceAPIAssociationConfig.MergeType)
}

func TestAssociateSourceGraphqlAPI_MergeType_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	src, err := b.CreateGraphqlAPI("Src", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	merged, err := b.CreateGraphqlAPI("Merged", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateSourceGraphqlAPI(merged.APIID, src.APIID, "desc", "AUTO_MERGE")
	require.NoError(t, err)
	require.NotNil(t, assoc.SourceAPIAssociationConfig)
	assert.Equal(t, "AUTO_MERGE", assoc.SourceAPIAssociationConfig.MergeType)

	got, err := b.GetSourceAPIAssociation(merged.APIID, assoc.AssociationID)
	require.NoError(t, err)
	require.NotNil(t, got.SourceAPIAssociationConfig)
	assert.Equal(t, "AUTO_MERGE", got.SourceAPIAssociationConfig.MergeType)
}

func TestInMemoryBackend_SourceAPIAssociation_CRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Create the source and merged APIs first (validation now requires both to exist).
	srcAPI, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	mrg1API, err := b.CreateGraphqlAPI("MergedAPI1", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	// Associate source API.
	assoc1, err := b.AssociateSourceGraphqlAPI(mrg1API.APIID, srcAPI.APIID, "test", "")
	require.NoError(t, err)
	assert.NotEmpty(t, assoc1.AssociationID)

	// Get source API association.
	got, err := b.GetSourceAPIAssociation(mrg1API.APIID, assoc1.AssociationID)
	require.NoError(t, err)
	assert.Equal(t, mrg1API.APIID, got.MergedAPIID)

	// Wrong merged API ID returns error.
	_, err = b.GetSourceAPIAssociation("wrong-id", assoc1.AssociationID)
	require.ErrorIs(t, err, awserr.ErrNotFound)

	// List source API associations.
	assocs, err := b.ListSourceAPIAssociations(mrg1API.APIID)
	require.NoError(t, err)
	assert.Len(t, assocs, 1)

	// List for empty merged API returns empty.
	assocs2, err := b.ListSourceAPIAssociations("empty-merged")
	require.NoError(t, err)
	assert.Empty(t, assocs2)

	// Disassociate.
	err = b.DisassociateSourceGraphqlAPI(mrg1API.APIID, assoc1.AssociationID)
	require.NoError(t, err)

	// Second disassociate returns error.
	err = b.DisassociateSourceGraphqlAPI(mrg1API.APIID, assoc1.AssociationID)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DisassociateMergedGraphqlAPI(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Create the source and merged APIs first.
	srcAPI, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	mrg1API, err := b.CreateGraphqlAPI("MergedAPI1", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateMergedGraphqlAPI(srcAPI.APIID, mrg1API.APIID, "", "")
	require.NoError(t, err)

	// Wrong source API ID returns error.
	err = b.DisassociateMergedGraphqlAPI("wrong-source", assoc.AssociationID)
	require.ErrorIs(t, err, awserr.ErrNotFound)

	// Correct source returns success.
	err = b.DisassociateMergedGraphqlAPI(srcAPI.APIID, assoc.AssociationID)
	require.NoError(t, err)
}

func TestInMemoryBackend_AssociateMergedAPI_ValidatesAPIsExist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sourceExists bool
		mergedExists bool
		wantErr      bool
	}{
		{
			name:         "source_not_found",
			sourceExists: false,
			mergedExists: true,
			wantErr:      true,
		},
		{
			name:         "merged_not_found",
			sourceExists: true,
			mergedExists: false,
			wantErr:      true,
		},
		{
			name:         "both_exist",
			sourceExists: true,
			mergedExists: true,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			sourceID := "nonexistent-source"
			mergedID := "nonexistent-merged"

			if tt.sourceExists {
				src, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				sourceID = src.APIID
			}

			if tt.mergedExists {
				mrg, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
				require.NoError(t, err)
				mergedID = mrg.APIID
			}

			_, err := b.AssociateMergedGraphqlAPI(sourceID, mergedID, "test", "")

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, appsync.ErrNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestInMemoryBackend_AssociateSourceAPI_ValidatesAPIsExist(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.AssociateSourceGraphqlAPI("nonexistent-merged", "nonexistent-source", "test", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrNotFound)
}

func TestBackend_UpdateSourceAPIAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		assocID     string
		mergedAPIID string
		description string
		wantDescr   string
		createAssoc bool
		wantErr     bool
	}{
		{
			name:        "success",
			createAssoc: true,
			description: "updated description",
			wantDescr:   "updated description",
		},
		{
			name:        "not_found",
			createAssoc: false,
			assocID:     "nonexistent",
			mergedAPIID: "nomerge",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			assocID := tt.assocID
			mergedAPIID := tt.mergedAPIID

			if tt.createAssoc {
				merged, err := b.CreateGraphqlAPI(
					"MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil,
				)
				require.NoError(t, err)
				source, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				assoc, err := b.AssociateSourceGraphqlAPI(merged.APIID, source.APIID, "initial", "")
				require.NoError(t, err)
				assocID = assoc.AssociationID
				mergedAPIID = merged.APIID
			}

			result, err := b.UpdateSourceAPIAssociation(mergedAPIID, assocID, tt.description)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDescr, result.Description)
		})
	}
}

func TestBackend_StartSchemaMerge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mergedAPIID   string
		assocID       string
		createAssoc   bool
		mismatchedAPI bool
		wantErr       bool
	}{
		{
			name:        "success",
			createAssoc: true,
		},
		{
			name:        "merged_api_not_found",
			createAssoc: false,
			mergedAPIID: "nonexistent-merged",
			assocID:     "nonexistent-assoc",
			wantErr:     true,
		},
		{
			name:          "association_belongs_to_different_merged_api",
			createAssoc:   true,
			mismatchedAPI: true,
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			mergedAPIID := tt.mergedAPIID
			assocID := tt.assocID

			if tt.createAssoc {
				merged, err := b.CreateGraphqlAPI(
					"MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil,
				)
				require.NoError(t, err)
				source, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				assoc, err := b.AssociateSourceGraphqlAPI(merged.APIID, source.APIID, "initial", "")
				require.NoError(t, err)
				assocID = assoc.AssociationID
				mergedAPIID = merged.APIID

				if tt.mismatchedAPI {
					other, otherErr := b.CreateGraphqlAPI(
						"OtherMergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil,
					)
					require.NoError(t, otherErr)
					mergedAPIID = other.APIID
				}
			}

			status, err := b.StartSchemaMerge(mergedAPIID, assocID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, appsync.ErrNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, appsync.SourceAPIAssociationStatusMergeSuccess, status)

			// The association's stored status must reflect the merge.
			got, getErr := b.GetSourceAPIAssociation(mergedAPIID, assocID)
			require.NoError(t, getErr)
			assert.Equal(t, appsync.SourceAPIAssociationStatusMergeSuccess, got.AssociationStatus)
		})
	}
}
