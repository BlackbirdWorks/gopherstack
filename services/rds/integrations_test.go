package rds_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateIntegration_WireShapeIsFlat verifies CreateIntegrationOutput's members sit
// directly under <CreateIntegrationResult>, matching the real (flat) aws-sdk-go-v2 shape —
// there is no nested <Integration> wrapper element. See TestCreateDBShardGroup_WireShapeIsFlat
// for why this matters to a real SDK client.
func TestCreateIntegration_WireShapeIsFlat(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":          {"CreateIntegration"},
		"Version":         {"2014-10-31"},
		"IntegrationName": {"wire-intg"},
		"SourceArn":       {"arn:aws:rds:us-east-1:123456789012:cluster:src"},
		"TargetArn":       {"arn:aws:redshift:us-east-1:123456789012:namespace:tgt"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp struct {
		Result struct {
			IntegrationName string `xml:"IntegrationName"`
			SourceArn       string `xml:"SourceArn"`
		} `xml:"CreateIntegrationResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "wire-intg", resp.Result.IntegrationName)
	assert.Equal(t, "arn:aws:rds:us-east-1:123456789012:cluster:src", resp.Result.SourceArn)
}

// TestIntegration_WireFieldsPresentOnAllOps asserts that KMSKeyId/CreateTime/
// Tags/Errors -- fields present on types.Integration and on every one of
// Create/Delete/ModifyIntegrationOutput in the real SDK, but previously
// modeled only partially (KmsKeyID/CreatedAt existed on this emulator's
// internal Integration struct but were never serialized onto ANY of these
// three operations' XML responses; Tags and Errors didn't exist at all) --
// actually appear in the raw wire body, not just the backend struct. This is
// the "disguised stub" bug class from .claude/memories/parity-principles.md:
// a real-looking, correctly-populated Go value that a real SDK client would
// never see because the XML struct never carried it.
func TestIntegration_WireFieldsPresentOnAllOps(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	createRec := postRDSForm(t, h,
		"Action=CreateIntegration&Version=2014-10-31"+
			"&IntegrationName=wire-fields-intg"+
			"&SourceArn=arn:aws:rds:us-east-1:123456789012:cluster:src"+
			"&TargetArn=arn:aws:redshift:us-east-1:123456789012:namespace:tgt"+
			"&KMSKeyId=arn:aws:kms:us-east-1:123456789012:key/test-key")
	require.Equal(t, http.StatusOK, createRec.Code, "body: %s", createRec.Body.String())
	assertIntegrationWireFieldsPresent(t, "CreateIntegration", createRec.Body.String())

	// Tags aren't accepted inline on CreateIntegration by this emulator (no
	// RDS Create* op in this service parses Tags.Tag.N at creation time --
	// see toXMLIntegration's doc comment), so add one via the standard
	// AddTagsToResource flow every resource in this emulator uses, then
	// confirm it shows up on the wire via Modify/Delete.
	h.Backend.AddTagsToResource(
		"arn:aws:rds:us-east-1:000000000000:integration:wire-fields-intg",
		[]rds.Tag{{Key: "env", Value: "test"}},
	)

	modifyRec := postRDSForm(t, h,
		"Action=ModifyIntegration&Version=2014-10-31&IntegrationIdentifier=wire-fields-intg&DataFilter=include:*")
	require.Equal(t, http.StatusOK, modifyRec.Code, "body: %s", modifyRec.Body.String())
	assertIntegrationWireFieldsPresent(t, "ModifyIntegration", modifyRec.Body.String())
	assert.Contains(t, modifyRec.Body.String(), "<Key>env</Key>")
	assert.Contains(t, modifyRec.Body.String(), "<Value>test</Value>")

	deleteRec := postRDSForm(t, h,
		"Action=DeleteIntegration&Version=2014-10-31&IntegrationIdentifier=wire-fields-intg")
	require.Equal(t, http.StatusOK, deleteRec.Code, "body: %s", deleteRec.Body.String())
	assertIntegrationWireFieldsPresent(t, "DeleteIntegration", deleteRec.Body.String())
}

func assertIntegrationWireFieldsPresent(t *testing.T, action, respBody string) {
	t.Helper()

	assert.Contains(t, respBody, "<KMSKeyId>", "action=%s missing KMSKeyId", action)
	assert.Contains(t, respBody, "<CreateTime>", "action=%s missing CreateTime", action)
	assert.Contains(t, respBody, "<Tags>", "action=%s missing Tags", action)
	assert.Contains(t, respBody, "<Errors>", "action=%s missing Errors", action)
}

func TestCreateIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		intgName  string
		srcARN    string
		tgtARN    string
		wantErr   bool
	}{
		{
			name:     "success",
			intgName: "intg-1",
			srcARN:   "arn:aws:rds:us-east-1:123:db:src",
			tgtARN:   "arn:aws:redshift:us-east-1:123:namespace:tgt",
		},
		{name: "empty name", intgName: "", wantErr: true, wantErrIs: rds.ErrInvalidParameter},
		{
			name:     "duplicate",
			intgName: "intg-dup",
			srcARN:   "arn:aws:rds:us-east-1:123:db:src",
			tgtARN:   "arn:aws:redshift:us-east-1:123:namespace:tgt",
			wantErr:  true, wantErrIs: rds.ErrIntegrationAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)

			if tt.name == "duplicate" {
				_, err := b.CreateIntegration(tt.intgName, tt.srcARN, tt.tgtARN, "", "", "")
				require.NoError(t, err)
			}

			intg, err := b.CreateIntegration(tt.intgName, tt.srcARN, tt.tgtARN, "", "", "")
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.intgName, intg.IntegrationName)
			assert.Equal(t, tt.srcARN, intg.SourceArn)
			assert.Equal(t, tt.tgtARN, intg.TargetArn)
			assert.Equal(t, "active", intg.Status)
			assert.NotEmpty(t, intg.IntegrationArn)
		})
	}
}

func TestDeleteIntegration(t *testing.T) {
	t.Parallel()

	t.Run("success by name", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateIntegration("intg-del", "src", "tgt", "", "", "")
		require.NoError(t, err)

		intg, err := b.DeleteIntegration("intg-del")
		require.NoError(t, err)
		assert.Equal(t, "intg-del", intg.IntegrationName)
		assert.Equal(t, "deleting", intg.Status)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DeleteIntegration("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrIntegrationNotFound)
	})
}

func TestDescribeIntegrations(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		integrations, err := b.DescribeIntegrations("")
		require.NoError(t, err)
		assert.Empty(t, integrations)
	})

	t.Run("lists all sorted", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		for _, nm := range []string{"intg-z", "intg-a", "intg-m"} {
			_, err := b.CreateIntegration(nm, "src", "tgt", "", "", "")
			require.NoError(t, err)
		}
		integrations, err := b.DescribeIntegrations("")
		require.NoError(t, err)
		require.Len(t, integrations, 3)
		assert.Equal(t, "intg-a", integrations[0].IntegrationName)
		assert.Equal(t, "intg-m", integrations[1].IntegrationName)
		assert.Equal(t, "intg-z", integrations[2].IntegrationName)
	})

	t.Run("filtered", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateIntegration("intg-x", "src", "tgt", "", "", "")
		require.NoError(t, err)
		_, err = b.CreateIntegration("intg-y", "src", "tgt", "", "", "")
		require.NoError(t, err)

		integrations, err := b.DescribeIntegrations("intg-x")
		require.NoError(t, err)
		require.Len(t, integrations, 1)
		assert.Equal(t, "intg-x", integrations[0].IntegrationName)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DescribeIntegrations("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrIntegrationNotFound)
	})
}

func TestModifyIntegration(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateIntegration("intg-mod", "src", "tgt", "", "", "")
		require.NoError(t, err)

		intg, err := b.ModifyIntegration("intg-mod", "", "")
		require.NoError(t, err)
		assert.Equal(t, "intg-mod", intg.IntegrationName)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.ModifyIntegration("missing", "", "")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrIntegrationNotFound)
	})
}

func TestHandler_IntegrationCRUD(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	// Create
	rec := postRDSForm(t, h,
		"Action=CreateIntegration&Version=2014-10-31"+
			"&IntegrationName=intg-1"+
			"&SourceArn=arn:aws:rds:us-east-1:123:db:src"+
			"&TargetArn=arn:aws:redshift:us-east-1:123:namespace:tgt")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "intg-1")

	// Describe all
	rec = postRDSForm(t, h, "Action=DescribeIntegrations&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "intg-1")

	// Modify
	rec = postRDSForm(
		t,
		h,
		"Action=ModifyIntegration&Version=2014-10-31&IntegrationIdentifier=intg-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = postRDSForm(
		t,
		h,
		"Action=DeleteIntegration&Version=2014-10-31&IntegrationIdentifier=intg-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Confirm gone
	rec = postRDSForm(
		t,
		h,
		"Action=DescribeIntegrations&Version=2014-10-31&IntegrationIdentifier=intg-1",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Integration_DuplicateError(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	rec := postRDSForm(t, h,
		"Action=CreateIntegration&Version=2014-10-31"+
			"&IntegrationName=dup-intg&SourceArn=src&TargetArn=tgt")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h,
		"Action=CreateIntegration&Version=2014-10-31"+
			"&IntegrationName=dup-intg&SourceArn=src&TargetArn=tgt")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "IntegrationAlreadyExists")
}

func TestIntegration_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	for i := range 10 {
		wg.Go(func() {
			if _, err := b.CreateIntegration(fmt.Sprintf("intg-conc-%d", i), "src", "tgt", "", "", ""); err != nil {
				errs <- err
			}
		})
	}

	for range 10 {
		wg.Go(func() {
			if _, err := b.DescribeIntegrations(""); err != nil {
				errs <- err
			}
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

func TestHandler_DescribeIntegrations_Pagination(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	for i := range 5 {
		rec := postRDSForm(t, h, fmt.Sprintf(
			"Action=CreateIntegration&Version=2014-10-31"+
				"&IntegrationName=intg-%02d&SourceArn=src&TargetArn=tgt", i,
		))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := postRDSForm(t, h, "Action=DescribeIntegrations&Version=2014-10-31&MaxRecords=2")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Marker")
}

func TestCreateIntegration_ARNFormat(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)

	intg, err := b.CreateIntegration("my-intg", "src", "tgt", "", "", "")
	require.NoError(t, err)
	assert.Contains(t, intg.IntegrationArn, "arn:aws:rds:")
	assert.Contains(t, intg.IntegrationArn, ":integration:")
	assert.Contains(t, intg.IntegrationArn, "my-intg")
}

func TestIntegration_DataFilterPersisted(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	intg, err := b.CreateIntegration(
		"my-intg",
		"arn:aws:rds:us-east-1:123:cluster:source",
		"arn:aws:redshift:us-east-1:123:namespace:target",
		"",
		"include(tableName=orders)",
		"my integration description",
	)
	require.NoError(t, err)

	assert.Equal(t, "include(tableName=orders)", intg.DataFilter)
	assert.Equal(t, "my integration description", intg.IntegrationDescription)
}

func TestIntegration_ModifyDataFilter(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateIntegration("intg-mod", "src", "tgt", "", "", "original desc")
	require.NoError(t, err)

	modified, err := b.ModifyIntegration("intg-mod", "include(tableName=users)", "updated desc")
	require.NoError(t, err)

	assert.Equal(t, "include(tableName=users)", modified.DataFilter)
	assert.Equal(t, "updated desc", modified.IntegrationDescription)
}

func TestIntegration_DataFilterViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=CreateIntegration&Version=2014-10-31"+
			"&IntegrationName=df-intg"+
			"&SourceArn=arn:aws:rds:us-east-1:123:cluster:src"+
			"&TargetArn=arn:aws:redshift:us-east-1:123:namespace:dst"+
			"&DataFilter=include%28tableName%3Dorders%2Ccustomers%29"+
			"&Description=zero-etl+pipeline")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "orders")
	assert.Contains(t, respStr, "zero-etl pipeline")
}

func TestIntegration_ModifyViaHandler(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	_, err := b.CreateIntegration("mod-intg", "src", "tgt", "", "", "")
	require.NoError(t, err)

	rec := postRDSForm(t, h,
		"Action=ModifyIntegration&Version=2014-10-31"+
			"&IntegrationIdentifier=mod-intg"+
			"&DataFilter=include%28tableName%3Devents%29"+
			"&Description=updated+description")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "events")
	assert.Contains(t, respStr, "updated description")
}

func TestPersistence_IntegrationDataFilter(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateIntegration("intg-snap", "src", "tgt", "", "include(orders)", "my description")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	integrations, err := b2.DescribeIntegrations("")
	require.NoError(t, err)
	require.Len(t, integrations, 1)

	assert.Equal(t, "include(orders)", integrations[0].DataFilter, "DataFilter should survive round-trip")
	assert.Equal(t, "my description", integrations[0].IntegrationDescription)
}

func TestIntegration_ARNContainsRegionAndAccount(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("111122223333", "eu-west-1")

	intg, err := b.CreateIntegration("my-intg", "src", "tgt", "", "", "")
	require.NoError(t, err)

	assert.Contains(t, intg.IntegrationArn, "eu-west-1")
	assert.Contains(t, intg.IntegrationArn, "111122223333")
	assert.Contains(t, intg.IntegrationArn, "my-intg")
}
