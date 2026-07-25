package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// TestBackend_Extension_Versioning verifies that UpdateExtension creates a
// new, independently addressable version rather than mutating the existing
// one in place -- matching real AWS AppConfig, where GetExtension/
// DeleteExtension both accept an optional version number and extensions are
// versioned resources (see PARITY.md's now-closed gap on this).
func TestBackend_Extension_Versioning(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	v1, err := b.CreateExtension("versioned-ext", "v1 description", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(1), v1.VersionNumber)

	v2Desc := "v2 description"
	v2, err := b.UpdateExtension(v1.ID, &v2Desc, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(2), v2.VersionNumber)
	assert.Equal(t, "v2 description", v2.Description)

	// Version 1 must still be independently gettable and unmodified.
	gotV1, err := b.GetExtension(v1.ID, 1)
	require.NoError(t, err)
	assert.Equal(t, "v1 description", gotV1.Description, "updating must not mutate the prior version")
	assert.Equal(t, int32(1), gotV1.VersionNumber)

	// An unspecified version (0) returns the highest version.
	gotLatest, err := b.GetExtension(v1.ID, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(2), gotLatest.VersionNumber)

	// Resolution by name must behave identically to resolution by ID.
	gotByName, err := b.GetExtension("versioned-ext", 1)
	require.NoError(t, err)
	assert.Equal(t, "v1 description", gotByName.Description)

	// A version that was never created is a real not-found, not the latest.
	_, err = b.GetExtension(v1.ID, 99)
	require.Error(t, err)
}

// TestBackend_DeleteExtension_SpecificVersion verifies that deleting one
// version leaves the extension's other versions intact and independently
// addressable, matching real DeleteExtensionInput's optional VersionNumber.
func TestBackend_DeleteExtension_SpecificVersion(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	ext, err := b.CreateExtension("del-ver-ext", "", nil, nil)
	require.NoError(t, err)

	desc := "v2"
	_, err = b.UpdateExtension(ext.ID, &desc, nil, nil)
	require.NoError(t, err)

	// Delete version 1 explicitly; version 2 must remain gettable.
	require.NoError(t, b.DeleteExtension(ext.ID, 1))

	_, err = b.GetExtension(ext.ID, 1)
	require.Error(t, err, "deleted version must be gone")

	stillThere, err := b.GetExtension(ext.ID, 2)
	require.NoError(t, err)
	assert.Equal(t, int32(2), stillThere.VersionNumber)
}

// TestBackend_DeleteExtension_DefaultsToHighestVersion verifies that
// omitting a version number (0) deletes only the highest version, not every
// version -- matching real AWS's "If omitted, the highest version is
// deleted" contract, not a full-extension wipe.
func TestBackend_DeleteExtension_DefaultsToHighestVersion(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	ext, err := b.CreateExtension("default-del-ext", "", nil, nil)
	require.NoError(t, err)

	desc := "v2"
	_, err = b.UpdateExtension(ext.ID, &desc, nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteExtension(ext.ID, 0))

	_, err = b.GetExtension(ext.ID, 2)
	require.Error(t, err, "highest version must be gone")

	remaining, err := b.GetExtension(ext.ID, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(1), remaining.VersionNumber, "version 1 must still be the current version")
}

// TestBackend_DeleteExtension_LastVersionRemovesExtensionAndTags verifies
// that deleting an extension's only remaining version removes the
// extension (and its tags) entirely rather than leaving a zero-version
// ghost record.
func TestBackend_DeleteExtension_LastVersionRemovesExtensionAndTags(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	ext, err := b.CreateExtension("last-ver-ext", "", nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.TagResource(ext.Arn, map[string]string{"k": "v"}))

	require.NoError(t, b.DeleteExtension(ext.ID, 0))

	_, err = b.GetExtension(ext.ID, 0)
	require.Error(t, err, "extension must be gone once its last version is deleted")

	tags, err := b.ListTagsForResource(ext.Arn)
	require.NoError(t, err)
	assert.Empty(t, tags, "tags must not survive the extension they were attached to")
}

// TestBackend_DeleteExtension_ConflictWhenAssociated verifies that deleting
// an extension version still referenced by an ExtensionAssociation returns
// a conflict, matching real AWS's requirement to remove associations first.
func TestBackend_DeleteExtension_ConflictWhenAssociated(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	ext, err := b.CreateExtension("assoc-conflict-ext", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateExtensionAssociation(
		ext.ID, "arn:aws:appconfig:us-east-1:123456789012:application/app-1", nil, nil,
	)
	require.NoError(t, err)

	err = b.DeleteExtension(ext.ID, 1)
	require.Error(t, err, "must refuse to delete a version still in use by an association")
}

// TestBackend_GetExtension_NotFound verifies the not-found error path for
// both an unknown identifier and an unknown version of a known extension.
func TestBackend_GetExtension_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.GetExtension("nonexistent", 0)
	require.Error(t, err)

	ext, err := b.CreateExtension("known-ext", "", nil, nil)
	require.NoError(t, err)

	_, err = b.GetExtension(ext.ID, 42)
	require.Error(t, err)
}

// TestBackend_DeleteExtension_NotFound verifies the not-found error path
// for both an unknown identifier and an unknown version of a known
// extension.
func TestBackend_DeleteExtension_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	err := b.DeleteExtension("nonexistent", 0)
	require.Error(t, err)

	ext, err := b.CreateExtension("known-del-ext", "", nil, nil)
	require.NoError(t, err)

	err = b.DeleteExtension(ext.ID, 42)
	require.Error(t, err)
}

// TestBackend_ListExtensions_OneRowPerExtensionAtLatestVersion verifies
// that ListExtensions -- which real AWS's ListExtensionsInput has no
// version filter for -- summarizes one row per extension at its current
// (highest) version, not one row per stored version.
func TestBackend_ListExtensions_OneRowPerExtensionAtLatestVersion(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	ext, err := b.CreateExtension("list-latest-ext", "", nil, nil)
	require.NoError(t, err)

	desc := "v2"
	_, err = b.UpdateExtension(ext.ID, &desc, nil, nil)
	require.NoError(t, err)

	items, _ := b.ListExtensions("", 0, "")
	require.Len(t, items, 1, "one extension with two versions must yield exactly one summary row")
	assert.Equal(t, int32(2), items[0].VersionNumber, "the summary row must reflect the latest version")
}
