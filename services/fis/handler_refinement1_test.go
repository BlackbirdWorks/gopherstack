package fis_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

// minimalTemplateBody returns a minimal valid create-experiment-template request body.
func minimalTemplateBody() map[string]any {
	return map[string]any{
		"description": "refinement test template",
		"roleArn":     "arn:aws:iam::000000000000:role/TestRole",
		"stopConditions": []map[string]any{
			{"source": "none"},
		},
		"targets": map[string]any{},
		"actions": map[string]any{},
	}
}

// seedTemplate creates a template via HTTP and returns its ID.
func seedTemplate(t *testing.T, h *fis.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", minimalTemplateBody())
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}
	mustJSON(t, rec, &resp)

	return resp.ExperimentTemplate.ID
}

// ----------------------------------------
// Reset tests
// ----------------------------------------

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-reset1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-reset1",
	})
	require.Equal(t, 1, b.TemplateCount())

	b.Reset()

	assert.Equal(t, 0, b.TemplateCount())
	assert.Equal(t, 0, b.ExperimentCount())
	assert.Equal(t, 0, b.TargetAccountConfigCount())
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	for i := range 3 {
		b.AddTemplateInternal(&fis.ExperimentTemplate{
			ID:  "EXT-cycle1",
			Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-cycle1",
		})
		b.Reset()
		assert.Equal(t, 0, b.TemplateCount(), "reset cycle %d", i)
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedTemplate(t, h)

	b := h.Backend.(*fis.ExportedInMemoryBackend)
	require.Equal(t, 1, b.TemplateCount())

	h.Reset()
	assert.Equal(t, 0, b.TemplateCount())
}

// ----------------------------------------
// Provider tests
// ----------------------------------------

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &fis.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, fis.ErrNilAppContext)
}

// ----------------------------------------
// GetSupportedOperations
// ----------------------------------------

func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateExperimentTemplate",
		"GetExperimentTemplate",
		"UpdateExperimentTemplate",
		"DeleteExperimentTemplate",
		"ListExperimentTemplates",
		"StartExperiment",
		"GetExperiment",
		"StopExperiment",
		"ListExperiments",
		"ListExperimentResolvedTargets",
		"GetAction",
		"ListActions",
		"GetTargetResourceType",
		"ListTargetResourceTypes",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"GetSafetyLever",
		"UpdateSafetyLeverState",
		"CreateTargetAccountConfiguration",
		"DeleteTargetAccountConfiguration",
		"GetExperimentTargetAccountConfiguration",
		"GetTargetAccountConfiguration",
		"ListExperimentTargetAccountConfigurations",
		"ListTargetAccountConfigurations",
		"UpdateTargetAccountConfiguration",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}

	assert.Equal(t, len(expected), h.HandlerOpsLen())
}

// ----------------------------------------
// Seed helpers and count helpers
// ----------------------------------------

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	tpl := &fis.ExperimentTemplate{
		ID:   "EXT-seed1",
		Arn:  "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-seed1",
		Tags: map[string]string{"env": "test"},
	}
	b.AddTemplateInternal(tpl)

	require.Equal(t, 1, b.TemplateCount())

	got, err := b.GetExperimentTemplate("EXT-seed1")
	require.NoError(t, err)
	assert.Equal(t, "test", got.Tags["env"])

	cfg := &fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-seed1",
		AccountID:            "111111111111",
		RoleArn:              "arn:aws:iam::111111111111:role/FISRole",
	}
	b.AddTargetAccountConfigInternal(cfg)

	assert.Equal(t, 1, b.TargetAccountConfigCount())
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	assert.Equal(t, 0, b.TemplateCount())
	assert.Equal(t, 0, b.ExperimentCount())
	assert.Equal(t, 0, b.TargetAccountConfigCount())

	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-count1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-count1",
	})
	assert.Equal(t, 1, b.TemplateCount())

	b.AddTargetAccountConfigInternal(&fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-count1",
		AccountID:            "222222222222",
		RoleArn:              "arn:aws:iam::222222222222:role/FISRole",
	})
	assert.Equal(t, 1, b.TargetAccountConfigCount())
}

// ----------------------------------------
// ErrValidation mapping
// ----------------------------------------

func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// CreateTargetAccountConfiguration with empty roleArn → 400
	rec := doRequest(
		t, h, http.MethodPost,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/123456789012",
		map[string]any{"roleArn": ""},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_CreateTargetAccountConfig_RequiresRoleArn(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-role1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-role1",
	})

	_, err := b.CreateTargetAccountConfiguration("EXT-role1", "123456789012", "", "desc")
	require.Error(t, err)
	assert.ErrorIs(t, err, fis.ErrValidation)
}

// ----------------------------------------
// Cascade delete
// ----------------------------------------

func TestRefinement1_DeleteTemplate_CascadesTargetAccountConfigs(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-cascade1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-cascade1",
	})
	b.AddTargetAccountConfigInternal(&fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-cascade1",
		AccountID:            "333333333333",
		RoleArn:              "arn:aws:iam::333333333333:role/FISRole",
	})
	require.Equal(t, 1, b.TargetAccountConfigCount())

	err := b.DeleteExperimentTemplate("EXT-cascade1")
	require.NoError(t, err)

	assert.Equal(t, 0, b.TemplateCount())
	assert.Equal(t, 0, b.TargetAccountConfigCount())
}

// ----------------------------------------
// Sorted list outputs
// ----------------------------------------

func TestRefinement1_SortedListExperimentTemplates(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	for _, id := range []string{"EXT-zzz", "EXT-aaa", "EXT-mmm"} {
		b.AddTemplateInternal(&fis.ExperimentTemplate{
			ID:  id,
			Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/" + id,
		})
	}

	list, err := b.ListExperimentTemplates()
	require.NoError(t, err)
	require.Len(t, list, 3)

	assert.Equal(t, "EXT-aaa", list[0].ID)
	assert.Equal(t, "EXT-mmm", list[1].ID)
	assert.Equal(t, "EXT-zzz", list[2].ID)
}

func TestRefinement1_SortedListTargetAccountConfigurations(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-sortcfg",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-sortcfg",
	})

	for _, acct := range []string{"999999999999", "111111111111", "555555555555"} {
		b.AddTargetAccountConfigInternal(&fis.TargetAccountConfiguration{
			ExperimentTemplateID: "EXT-sortcfg",
			AccountID:            acct,
			RoleArn:              "arn:aws:iam::" + acct + ":role/FISRole",
		})
	}

	list, err := b.ListTargetAccountConfigurations("EXT-sortcfg")
	require.NoError(t, err)
	require.Len(t, list, 3)

	assert.Equal(t, "111111111111", list[0].AccountID)
	assert.Equal(t, "555555555555", list[1].AccountID)
	assert.Equal(t, "999999999999", list[2].AccountID)
}

func TestRefinement1_SortedListActions(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	actions := b.ListActions()
	require.NotEmpty(t, actions)

	for i := 1; i < len(actions); i++ {
		assert.LessOrEqual(t, actions[i-1].ID, actions[i].ID, "actions not sorted at index %d", i)
	}
}

func TestRefinement1_SortedListTargetResourceTypes(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	types := b.ListTargetResourceTypes()
	require.NotEmpty(t, types)

	for i := 1; i < len(types); i++ {
		assert.LessOrEqual(t, types[i-1].ResourceType, types[i].ResourceType,
			"resource types not sorted at index %d", i)
	}
}

// ----------------------------------------
// Persistence round-trip
// ----------------------------------------

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:   "EXT-persist1",
		Arn:  "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-persist1",
		Tags: map[string]string{"key": "val"},
	})
	b.AddTargetAccountConfigInternal(&fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-persist1",
		AccountID:            "444444444444",
		RoleArn:              "arn:aws:iam::444444444444:role/FISRole",
		Description:          "persist test",
	})

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := fis.NewTestBackend()
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 1, b2.TemplateCount())
	assert.Equal(t, 1, b2.TargetAccountConfigCount())

	cfg, err := b2.GetTargetAccountConfiguration("EXT-persist1", "444444444444")
	require.NoError(t, err)
	assert.Equal(t, "persist test", cfg.Description)
}

func TestRefinement1_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := fis.NewTestBackend()
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 0, b2.TemplateCount())
	assert.Equal(t, 0, b2.ExperimentCount())
	assert.Equal(t, 0, b2.TargetAccountConfigCount())
}

func TestRefinement1_PersistenceRestoreNilSafetyLever(t *testing.T) {
	t.Parallel()

	// Restore from a snapshot that lacks a safetyLever field should
	// reconstruct the lever rather than leaving it nil.
	raw := []byte(`{"templates":{},"experiments":{},"targetAccountConfigs":{},
		"safetyLever":null,"accountID":"000000000000","region":"us-east-1"}`)

	b := fis.NewTestBackend()
	require.NoError(t, b.Restore(raw))

	// GetSafetyLever should succeed (lever was auto-rebuilt).
	lever, err := b.GetSafetyLever("000000000000")
	require.NoError(t, err)
	assert.Equal(t, "disengaged", lever.State.Status)
}

// ----------------------------------------
// TargetAccountConfiguration CRUD round-trip
// ----------------------------------------

func TestRefinement1_TargetAccountConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	accountID := "555555555555"
	roleArn := "arn:aws:iam::555555555555:role/FISCrossAccount"
	description := "cross-account config"

	// Create
	rec := doRequest(
		t, h, http.MethodPost,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID,
		map[string]any{"roleArn": roleArn, "description": description},
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		TargetAccountConfiguration struct {
			AccountID   string `json:"accountId"`
			RoleArn     string `json:"roleArn"`
			Description string `json:"description"`
		} `json:"targetAccountConfiguration"`
	}
	mustJSON(t, rec, &createResp)
	assert.Equal(t, accountID, createResp.TargetAccountConfiguration.AccountID)
	assert.Equal(t, roleArn, createResp.TargetAccountConfiguration.RoleArn)

	// Get
	rec2 := doRequest(t, h, http.MethodGet,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp struct {
		TargetAccountConfiguration struct {
			Description string `json:"description"`
		} `json:"targetAccountConfiguration"`
	}
	mustJSON(t, rec2, &getResp)
	assert.Equal(t, description, getResp.TargetAccountConfiguration.Description)

	// List
	rec3 := doRequest(t, h, http.MethodGet,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listResp struct {
		TargetAccountConfigurations []struct {
			AccountID string `json:"accountId"`
		} `json:"targetAccountConfigurations"`
	}
	mustJSON(t, rec3, &listResp)
	require.Len(t, listResp.TargetAccountConfigurations, 1)
	assert.Equal(t, accountID, listResp.TargetAccountConfigurations[0].AccountID)

	// Delete
	rec4 := doRequest(t, h, http.MethodDelete,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID, nil)
	require.Equal(t, http.StatusOK, rec4.Code)

	// Verify deleted
	rec5 := doRequest(t, h, http.MethodGet,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID, nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

func TestRefinement1_UpdateTargetAccountConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)
	accountID := "666666666666"

	doRequest(
		t, h, http.MethodPost,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID,
		map[string]any{
			"roleArn":     "arn:aws:iam::666666666666:role/OldRole",
			"description": "old desc",
		},
	)

	rec := doRequest(
		t, h, http.MethodPatch,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID,
		map[string]any{"description": "new desc"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		TargetAccountConfiguration struct {
			Description string `json:"description"`
			RoleArn     string `json:"roleArn"`
		} `json:"targetAccountConfiguration"`
	}
	mustJSON(t, rec, &resp)
	assert.Equal(t, "new desc", resp.TargetAccountConfiguration.Description)
	assert.Equal(t, "arn:aws:iam::666666666666:role/OldRole", resp.TargetAccountConfiguration.RoleArn)
}

// ----------------------------------------
// Experiment target account config operations
// ----------------------------------------

func TestRefinement1_ExperimentTargetAccountConfig_GetAndList(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:             "EXT-expacct1",
		Arn:            "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-expacct1",
		RoleArn:        "arn:aws:iam::000000000000:role/FIS",
		Actions:        map[string]fis.ExperimentTemplateAction{},
		Targets:        map[string]fis.ExperimentTemplateTarget{},
		StopConditions: []fis.ExperimentTemplateStopCondition{{Source: "none"}},
	})
	b.AddTargetAccountConfigInternal(&fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-expacct1",
		AccountID:            "777777777777",
		RoleArn:              "arn:aws:iam::777777777777:role/FISRole",
		Description:          "exp-account-cfg",
	})
	b.InjectExperiment(&fis.Experiment{
		ID:                   "EXP-acct1",
		Arn:                  "arn:aws:fis:us-east-1:000000000000:experiment/EXP-acct1",
		ExperimentTemplateID: "EXT-expacct1",
		Status:               fis.ExperimentStatus{Status: "completed"},
		Tags:                 map[string]string{},
	})

	// Get
	cfg, err := b.GetExperimentTargetAccountConfiguration("EXP-acct1", "777777777777")
	require.NoError(t, err)
	assert.Equal(t, "777777777777", cfg.AccountID)
	assert.Equal(t, "exp-account-cfg", cfg.Description)
	assert.Equal(t, "EXP-acct1", cfg.ExperimentID)

	// List
	list, err := b.ListExperimentTargetAccountConfigurations("EXP-acct1")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "777777777777", list[0].AccountID)
}

// ----------------------------------------
// ErrTooManyExperiments → 429
// ----------------------------------------

func TestRefinement1_ErrTooManyExperiments_Returns429(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:             "EXT-quota1",
		Arn:            "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-quota1",
		RoleArn:        "arn:aws:iam::000000000000:role/FIS",
		Actions:        map[string]fis.ExperimentTemplateAction{},
		Targets:        map[string]fis.ExperimentTemplateTarget{},
		StopConditions: []fis.ExperimentTemplateStopCondition{{Source: "none"}},
	})

	// Fill up the experiment cap with 1000 injected experiments.
	endTime := time.Now()

	for i := range 1000 {
		id := "EXP-fill" + fmt.Sprintf("%04d", i)
		b.InjectExperiment(&fis.Experiment{
			ID:      id,
			Arn:     "arn:aws:fis:us-east-1:000000000000:experiment/" + id,
			Status:  fis.ExperimentStatus{Status: "completed"},
			Tags:    map[string]string{},
			EndTime: &endTime,
		})
	}

	h := fis.NewHandler(b)
	h.DefaultRegion = "us-east-1"
	h.AccountID = "000000000000"

	rec := doRequest(
		t, h, http.MethodPost, "/experiments",
		map[string]any{"experimentTemplateId": "EXT-quota1"},
	)
	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
}

// ----------------------------------------
// Non-nil tags on templates/experiments
// ----------------------------------------

func TestRefinement1_NonNilTags_Template(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-notags1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-notags1",
		// Tags intentionally nil
	})

	tpl, err := b.GetExperimentTemplate("EXT-notags1")
	require.NoError(t, err)
	assert.NotNil(t, tpl.Tags, "tags should never be nil on returned template")
}

func TestRefinement1_NonNilTags_Experiment(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.InjectExperiment(&fis.Experiment{
		ID:     "EXP-notags1",
		Arn:    "arn:aws:fis:us-east-1:000000000000:experiment/EXP-notags1",
		Status: fis.ExperimentStatus{Status: "completed"},
		// Tags intentionally nil
	})

	exp, err := b.GetExperiment("EXP-notags1")
	require.NoError(t, err)
	assert.NotNil(t, exp.Tags, "tags should never be nil on returned experiment")
}

// ----------------------------------------
// ListTagsForResource returns non-nil map
// ----------------------------------------

func TestRefinement1_ListTagsForResource_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:   "EXT-tagsnone",
		Arn:  "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-tagsnone",
		Tags: map[string]string{},
	})

	tags, err := b.ListTagsForResource("arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-tagsnone")
	require.NoError(t, err)
	assert.NotNil(t, tags)
	assert.Empty(t, tags)
}

// ----------------------------------------
// Safety lever preserved across persistence
// ----------------------------------------

func TestRefinement1_SafetyLever_PreservedAcrossPersistence(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Engage the lever via HTTP PATCH.
	rec := doRequest(
		t, h, http.MethodPatch, "/safetyLevers/000000000000",
		map[string]any{
			"updateSafetyLeverStateInput": map[string]any{
				"status": "engaged",
				"reason": "test lock",
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	b := h.Backend.(*fis.ExportedInMemoryBackend)
	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := fis.NewTestBackend()
	require.NoError(t, b2.Restore(snap))

	lever, err := b2.GetSafetyLever("000000000000")
	require.NoError(t, err)
	assert.Equal(t, "engaged", lever.State.Status)
	assert.Equal(t, "test lock", lever.State.Reason)
}

// ----------------------------------------
// DeleteTemplate cascades via HTTP
// ----------------------------------------

func TestRefinement1_DeleteTemplate_HTTP_CascadesConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// Add a config.
	doRequest(
		t, h, http.MethodPost,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/888888888888",
		map[string]any{"roleArn": "arn:aws:iam::888888888888:role/FISRole"},
	)

	b := h.Backend.(*fis.ExportedInMemoryBackend)
	require.Equal(t, 1, b.TargetAccountConfigCount())

	// Delete template.
	rec := doRequest(t, h, http.MethodDelete, "/experimentTemplates/"+tplID, nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Config must be gone.
	assert.Equal(t, 0, b.TargetAccountConfigCount())
}

// ----------------------------------------
// AddTargetAccountConfigInternal deep-copy
// ----------------------------------------

func TestRefinement1_SeedHelper_DeepCopy(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-deepcopy1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-deepcopy1",
	})

	original := &fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-deepcopy1",
		AccountID:            "999999999999",
		RoleArn:              "arn:aws:iam::999999999999:role/Original",
		Description:          "original",
	}
	b.AddTargetAccountConfigInternal(original)

	// Mutate original after insert — stored copy should not change.
	original.Description = "mutated"

	got, err := b.GetTargetAccountConfiguration("EXT-deepcopy1", "999999999999")
	require.NoError(t, err)
	assert.Equal(t, "original", got.Description)
}
