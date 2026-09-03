package ses_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

func TestHandler_DescribeActiveReceiptRuleSet_NoActive(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DescribeActiveReceiptRuleSet&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeActiveReceiptRuleSetResponse")
}

func TestCreateReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":      {"CreateReceiptRuleSet"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"rs-new"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateReceiptRuleSetResponse")
	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).ReceiptRuleSetCount())
}

func TestCloneReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs-original"))
	require.NoError(t, h.Backend.CreateReceiptRule("rs-original", ses.ReceiptRule{Name: "r1", Enabled: true}, ""))

	rec := postForm(t, h, url.Values{
		"Action":              {"CloneReceiptRuleSet"},
		"Version":             {"2010-12-01"},
		"OriginalRuleSetName": {"rs-original"},
		"RuleSetName":         {"rs-clone"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 2, h.Backend.(*ses.InMemoryBackend).ReceiptRuleSetCount())

	cloned, err := h.Backend.DescribeReceiptRuleSet("rs-clone")
	require.NoError(t, err)
	require.Len(t, cloned.Rules, 1)
	assert.Equal(t, "r1", cloned.Rules[0].Name)
}

func TestDescribeReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs-desc"))
	require.NoError(t, h.Backend.CreateReceiptRule("rs-desc", ses.ReceiptRule{Name: "r1", Enabled: true}, ""))

	rec := postForm(t, h, url.Values{
		"Action":      {"DescribeReceiptRuleSet"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"rs-desc"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "rs-desc")
	assert.Contains(t, rec.Body.String(), "r1")
}

func TestDeleteReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs-del"))

	rec := postForm(t, h, url.Values{
		"Action":      {"DeleteReceiptRuleSet"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"rs-del"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).ReceiptRuleSetCount())
}

func TestSetActiveReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs-active"))

	rec := postForm(t, h, url.Values{
		"Action":      {"SetActiveReceiptRuleSet"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"rs-active"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "rs-active", h.Backend.(*ses.InMemoryBackend).ActiveRuleSet())
}

func TestDescribeActiveReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs-a"))
	require.NoError(t, h.Backend.SetActiveReceiptRuleSet("rs-a"))

	rec := postForm(t, h, "Action=DescribeActiveReceiptRuleSet&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeActiveReceiptRuleSetResponse")
	assert.Contains(t, rec.Body.String(), "rs-a")
}

func TestDescribeActiveReceiptRuleSet_None_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DescribeActiveReceiptRuleSet&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestListReceiptRuleSets_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs1"))
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs2"))

	rec := postForm(t, h, "Action=ListReceiptRuleSets&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListReceiptRuleSetsResponse")
}

func TestCreateReceiptRuleSet_Duplicate_Error(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))
	assert.Error(t, b.CreateReceiptRuleSet("rs"))
}

func TestCloneReceiptRuleSet_IsDeepCopy(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("original"))
	require.NoError(t, h.Backend.CreateReceiptRule("original", ses.ReceiptRule{Name: "rule1", Enabled: true}, ""))

	// clone
	rec := postForm(t, h, url.Values{
		"Action":              {"CloneReceiptRuleSet"},
		"Version":             {"2010-12-01"},
		"RuleSetName":         {"clone"},
		"OriginalRuleSetName": {"original"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// add rule to clone only
	require.NoError(t, h.Backend.CreateReceiptRule("clone", ses.ReceiptRule{Name: "rule2", Enabled: true}, ""))

	// original must still have exactly 1 rule
	orig, err := h.Backend.DescribeReceiptRuleSet("original")
	require.NoError(t, err)
	assert.Len(t, orig.Rules, 1, "original must still have 1 rule after modifying clone")

	// clone must have 2 rules
	cloned, err := h.Backend.DescribeReceiptRuleSet("clone")
	require.NoError(t, err)
	assert.Len(t, cloned.Rules, 2, "clone must have 2 rules")
}

func TestDescribeReceiptRuleSet_EmptyRulesNotAbsent(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("empty-rs"))

	rec := postForm(t, h, url.Values{
		"Action":      {"DescribeReceiptRuleSet"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"empty-rs"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "DescribeReceiptRuleSetResponse")
	assert.Contains(t, body, "Rules",
		"Rules element must be present even when rule set has no rules")
}

func TestListReceiptRuleSets_EmptyState(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":  {"ListReceiptRuleSets"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "ListReceiptRuleSetsResponse")
	assert.Contains(t, body, "RuleSets",
		"RuleSets element must be present even when empty")
}

// TestSESNewOps_CreateReceiptRuleSet covers the CreateReceiptRuleSet handler.
func TestCreateReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=CreateReceiptRuleSet&Version=2010-12-01&RuleSetName=my-rules",
			wantCode:     http.StatusOK,
			wantContains: "CreateReceiptRuleSetResponse",
		},
		{
			name:         "duplicate_returns_error",
			body:         "Action=CreateReceiptRuleSet&Version=2010-12-01&RuleSetName=existing",
			setup:        func(h *ses.Handler) { require.NoError(t, h.Backend.CreateReceiptRuleSet("existing")) },
			wantCode:     http.StatusBadRequest,
			wantContains: "AlreadyExists",
		},
		{
			name:         "empty_name_returns_error",
			body:         "Action=CreateReceiptRuleSet&Version=2010-12-01&RuleSetName=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_CloneReceiptRuleSet covers the CloneReceiptRuleSet handler.
func TestCloneReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CloneReceiptRuleSet&Version=2010-12-01&OriginalRuleSetName=source&RuleSetName=clone",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateReceiptRuleSet("source"))
			},
			wantCode:     http.StatusOK,
			wantContains: "CloneReceiptRuleSetResponse",
		},
		{
			name:         "source_not_found",
			body:         "Action=CloneReceiptRuleSet&Version=2010-12-01&OriginalRuleSetName=nonexistent&RuleSetName=clone",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
		{
			name: "destination_already_exists",
			body: "Action=CloneReceiptRuleSet&Version=2010-12-01&OriginalRuleSetName=src&RuleSetName=existing",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateReceiptRuleSet("src"))
				require.NoError(t, h.Backend.CreateReceiptRuleSet("existing"))
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "AlreadyExists",
		},
		{
			name:         "empty_original_name",
			body:         "Action=CloneReceiptRuleSet&Version=2010-12-01&OriginalRuleSetName=&RuleSetName=clone",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_CloneReceiptRuleSet_CopiesRules verifies that cloning copies rules.
func TestCloneReceiptRuleSet_CopiesRules(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("source"))
	require.NoError(t, h.Backend.CreateReceiptRule("source", ses.ReceiptRule{
		Name:    "rule1",
		Enabled: true,
	}, ""))

	rec := postForm(t, h, "Action=CloneReceiptRuleSet&Version=2010-12-01&OriginalRuleSetName=source&RuleSetName=clone")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 2, h.Backend.(*ses.InMemoryBackend).ReceiptRuleSetCount())
}

// TestHandler_ListReceiptRuleSets tests the ListReceiptRuleSets handler.
func TestHandler_ListReceiptRuleSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "empty_returns_empty_list",
			wantCode:     http.StatusOK,
			wantContains: "ListReceiptRuleSetsResponse",
		},
		{
			name: "with_rule_sets",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "set1", CreatedAt: time.Now()})
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "set2", CreatedAt: time.Now()})
			},
			wantCode:     http.StatusOK,
			wantContains: "set1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			body := "Action=ListReceiptRuleSets&Version=2010-12-01"
			rec := postForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_DescribeReceiptRuleSet tests the DescribeReceiptRuleSet handler.
func TestHandler_DescribeReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{
					Name:      "my-set",
					CreatedAt: time.Now(),
					Rules:     []ses.ReceiptRule{{Name: "rule1", Enabled: true}},
				})
			},
			body:         "Action=DescribeReceiptRuleSet&Version=2010-12-01&RuleSetName=my-set",
			wantCode:     http.StatusOK,
			wantContains: "DescribeReceiptRuleSetResponse",
		},
		{
			name:         "not_found",
			body:         "Action=DescribeReceiptRuleSet&Version=2010-12-01&RuleSetName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
		{
			name:         "empty_name",
			body:         "Action=DescribeReceiptRuleSet&Version=2010-12-01&RuleSetName=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_DeleteReceiptRuleSet tests the DeleteReceiptRuleSet handler.
func TestHandler_DeleteReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "my-set", CreatedAt: time.Now()})
			},
			body:         "Action=DeleteReceiptRuleSet&Version=2010-12-01&RuleSetName=my-set",
			wantCode:     http.StatusOK,
			wantContains: "DeleteReceiptRuleSetResponse",
		},
		{
			// Idempotent: DeleteReceiptRuleSet's own deserializer (ses@v1.37.4
			// deserializers.go) declares only CannotDelete, not RuleSetDoesNotExist.
			name:         "not_found_is_idempotent",
			body:         "Action=DeleteReceiptRuleSet&Version=2010-12-01&RuleSetName=nonexistent",
			wantCode:     http.StatusOK,
			wantContains: "DeleteReceiptRuleSetResponse",
		},
		{
			name:         "empty_name",
			body:         "Action=DeleteReceiptRuleSet&Version=2010-12-01&RuleSetName=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_SetActiveReceiptRuleSet tests the SetActiveReceiptRuleSet handler.
func TestHandler_SetActiveReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "my-set", CreatedAt: time.Now()})
			},
			body:         "Action=SetActiveReceiptRuleSet&Version=2010-12-01&RuleSetName=my-set",
			wantCode:     http.StatusOK,
			wantContains: "SetActiveReceiptRuleSetResponse",
		},
		{
			name:         "clear_active_with_empty_name",
			body:         "Action=SetActiveReceiptRuleSet&Version=2010-12-01&RuleSetName=",
			wantCode:     http.StatusOK,
			wantContains: "SetActiveReceiptRuleSetResponse",
		},
		{
			name:         "not_found",
			body:         "Action=SetActiveReceiptRuleSet&Version=2010-12-01&RuleSetName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_DescribeActiveReceiptRuleSet tests the DescribeActiveReceiptRuleSet handler.
func TestHandler_DescribeActiveReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "no_active_rule_set",
			wantCode:     http.StatusOK,
			wantContains: "DescribeActiveReceiptRuleSetResponse",
		},
		{
			name: "with_active_rule_set",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{
					Name:      "active-set",
					CreatedAt: time.Now(),
					Rules:     []ses.ReceiptRule{{Name: "rule1", Enabled: true}},
				})
				require.NoError(t, b.SetActiveReceiptRuleSet("active-set"))
			},
			wantCode:     http.StatusOK,
			wantContains: "active-set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			body := "Action=DescribeActiveReceiptRuleSet&Version=2010-12-01"
			rec := postForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestBackend_SetActiveReceiptRuleSet tests active rule set management.
func TestBackend_SetActiveReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *ses.InMemoryBackend)
		name       string
		setName    string
		wantActive string
		wantErr    bool
	}{
		{
			name: "set_active",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "rs1", CreatedAt: time.Now()})
			},
			setName:    "rs1",
			wantActive: "rs1",
		},
		{
			name: "clear_active",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "rs1", CreatedAt: time.Now()})
				require.NoError(t, b.SetActiveReceiptRuleSet("rs1"))
			},
			setName:    "",
			wantActive: "",
		},
		{
			name:    "not_found",
			setName: "missing",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.SetActiveReceiptRuleSet(tt.setName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantActive, b.ActiveRuleSet())
		})
	}
}

// TestBackend_DeleteReceiptRuleSet_ActiveSetRejected tests that deleting the
// currently active rule set is rejected with ErrReceiptRuleSetActive, matching
// real AWS SES ("The currently active rule set cannot be deleted."), and that
// clearing the active pointer first allows the delete to succeed.
func TestBackend_DeleteReceiptRuleSet_ActiveSetRejected(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "rs1", CreatedAt: time.Now()})
	require.NoError(t, b.SetActiveReceiptRuleSet("rs1"))
	assert.Equal(t, "rs1", b.ActiveRuleSet())

	err := b.DeleteReceiptRuleSet("rs1")
	require.ErrorIs(t, err, ses.ErrReceiptRuleSetActive)
	assert.Equal(t, "rs1", b.ActiveRuleSet(), "active pointer must survive a rejected delete")

	require.NoError(t, b.SetActiveReceiptRuleSet(""))
	require.NoError(t, b.DeleteReceiptRuleSet("rs1"))
	assert.Empty(t, b.ActiveRuleSet())
}

// TestBackend_DescribeActiveReceiptRuleSet_NoActive tests when no active rule set.
func TestBackend_DescribeActiveReceiptRuleSet_NoActive(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	rs, active, err := b.DescribeActiveReceiptRuleSet()
	require.NoError(t, err)
	assert.False(t, active)
	assert.Empty(t, rs.Name)
}

// TestBackend_Reset_ClearsActiveRuleSet tests that Reset clears active rule set.
func TestBackend_Reset_ClearsActiveRuleSet(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "rs1", CreatedAt: time.Now()})
	require.NoError(t, b.SetActiveReceiptRuleSet("rs1"))
	assert.Equal(t, "rs1", b.ActiveRuleSet())

	b.Reset()
	assert.Empty(t, b.ActiveRuleSet())
}

// TestBackend_ListReceiptRuleSets_SortedOrder tests that rule sets are returned sorted.
func TestBackend_ListReceiptRuleSets_SortedOrder(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "zzz-set", CreatedAt: time.Now()})
	b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "aaa-set", CreatedAt: time.Now()})

	sets := b.ListReceiptRuleSets("")
	require.Len(t, sets.Data, 2)
	assert.Equal(t, "aaa-set", sets.Data[0].Name)
	assert.Equal(t, "zzz-set", sets.Data[1].Name)
}
