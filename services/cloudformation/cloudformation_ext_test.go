package cloudformation_test

import (
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// ---- Backend: DetectStackDrift ----------------------------------------------

func TestBackend_DetectStackDrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		stackName string
		setup     bool
	}{
		{
			name:      "success",
			stackName: "my-stack",
			setup:     true,
		},
		{
			name:      "stack_not_found",
			stackName: "missing-stack",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup {
				_, err := b.CreateStack(t.Context(), tt.stackName, simpleTemplate, nil, nil)
				require.NoError(t, err)
			}

			detectionID, err := b.DetectStackDrift(tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, detectionID)
		})
	}
}

// ---- Backend: DescribeStackDriftDetectionStatus -----------------------------

func TestBackend_DescribeStackDriftDetectionStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr         error
		name            string
		stackName       string
		detectionIDFn   func(b *cloudformation.InMemoryBackend) string
		wantStatus      string
		wantDriftStatus string
	}{
		{
			name:      "success",
			stackName: "my-stack",
			detectionIDFn: func(b *cloudformation.InMemoryBackend) string {
				id, err := b.DetectStackDrift("my-stack")
				if err != nil {
					return ""
				}

				return id
			},
			wantStatus:      "DETECTION_COMPLETE",
			wantDriftStatus: "IN_SYNC",
		},
		{
			name:      "not_found",
			stackName: "my-stack",
			detectionIDFn: func(_ *cloudformation.InMemoryBackend) string {
				return "nonexistent-detection-id"
			},
			wantErr: cloudformation.ErrDriftDetectionNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.stackName != "" {
				_, err := b.CreateStack(t.Context(), tt.stackName, simpleTemplate, nil, nil)
				require.NoError(t, err)
			}

			detectionID := tt.detectionIDFn(b)
			status, err := b.DescribeStackDriftDetectionStatus(detectionID)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status.DetectionStatus)
			assert.Equal(t, tt.wantDriftStatus, status.StackDriftStatus)
		})
	}
}

// ---- Backend: DetectStackResourceDrift -------------------------------------

func TestBackend_DetectStackResourceDrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		stackName string
		logicalID string
		setup     bool
	}{
		{
			name:      "success",
			stackName: "my-stack",
			logicalID: "MyBucket",
			setup:     true,
		},
		{
			name:      "stack_not_found",
			stackName: "missing",
			logicalID: "MyBucket",
			wantErr:   cloudformation.ErrStackNotFound,
		},
		{
			name:      "resource_not_found",
			stackName: "my-stack",
			logicalID: "NoSuchResource",
			setup:     true,
			wantErr:   cloudformation.ErrResourceNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup {
				_, err := b.CreateStack(t.Context(), tt.stackName, simpleTemplate, nil, nil)
				require.NoError(t, err)
			}

			detectionID, err := b.DetectStackResourceDrift(tt.stackName, tt.logicalID)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, detectionID)
		})
	}
}

// ---- Backend: DescribeStackResourceDrifts ----------------------------------

func TestBackend_DescribeStackResourceDrifts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		name         string
		stackName    string
		template     string
		setup        bool
		wantDriftLen int
	}{
		{
			name:         "stack_with_resources",
			stackName:    "my-stack",
			template:     simpleTemplate,
			setup:        true,
			wantDriftLen: 1,
		},
		{
			name:      "stack_not_found",
			stackName: "missing",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup {
				_, err := b.CreateStack(t.Context(), tt.stackName, tt.template, nil, nil)
				require.NoError(t, err)
			}

			drifts, err := b.DescribeStackResourceDrifts(tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, drifts, tt.wantDriftLen)

			for _, d := range drifts {
				assert.Equal(t, "IN_SYNC", d.StackResourceDriftStatus)
			}
		})
	}
}

// ---- Backend: SetStackPolicy / GetStackPolicy ------------------------------

func TestBackend_StackPolicy(t *testing.T) {
	t.Parallel()

	policy := `{"Statement":[{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"}]}`

	tests := []struct {
		setErr    error
		getErr    error
		name      string
		stackName string
		policy    string
		setup     bool
		wantEmpty bool
	}{
		{
			name:      "set_and_get",
			stackName: "my-stack",
			policy:    policy,
			setup:     true,
		},
		{
			name:      "get_empty_policy",
			stackName: "my-stack",
			setup:     true,
			wantEmpty: true,
		},
		{
			name:      "set_stack_not_found",
			stackName: "missing",
			setErr:    cloudformation.ErrStackNotFound,
		},
		{
			name:      "get_stack_not_found",
			stackName: "missing",
			getErr:    cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup {
				_, err := b.CreateStack(t.Context(), tt.stackName, simpleTemplate, nil, nil)
				require.NoError(t, err)
			}

			if tt.setErr != nil {
				err := b.SetStackPolicy(tt.stackName, tt.policy)
				require.ErrorIs(t, err, tt.setErr)

				return
			}

			if tt.getErr != nil {
				_, err := b.GetStackPolicy(tt.stackName)
				require.ErrorIs(t, err, tt.getErr)

				return
			}

			if !tt.wantEmpty {
				err := b.SetStackPolicy(tt.stackName, tt.policy)
				require.NoError(t, err)
			}

			got, err := b.GetStackPolicy(tt.stackName)
			require.NoError(t, err)

			if tt.wantEmpty {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, tt.policy, got)
			}
		})
	}
}

// ---- Backend: GetTemplateSummary -------------------------------------------

func TestBackend_GetTemplateSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		name         string
		templateBody string
		stackName    string
		wantDesc     string
		wantParamLen int
		wantResTypes int
		setupStack   bool
	}{
		{
			name:         "from_template_body",
			templateBody: simpleTemplate,
			wantResTypes: 1,
		},
		{
			name:         "from_template_body_with_params",
			templateBody: templateWithParams,
			wantParamLen: 1,
			wantResTypes: 1,
		},
		{
			name:         "from_stack_name",
			stackName:    "my-stack",
			setupStack:   true,
			wantResTypes: 1,
		},
		{
			name:         "empty_body",
			wantResTypes: 0,
		},
		{
			name:      "stack_not_found",
			stackName: "missing",
			wantErr:   cloudformation.ErrStackNotFound,
		},
		{
			name:         "yaml_template",
			templateBody: yamlTemplate,
			wantDesc:     "YAML template",
			wantResTypes: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setupStack {
				_, err := b.CreateStack(t.Context(), tt.stackName, simpleTemplate, nil, nil)
				require.NoError(t, err)
			}

			summary, err := b.GetTemplateSummary(tt.templateBody, tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, summary)
			assert.Len(t, summary.Parameters, tt.wantParamLen)
			assert.Len(t, summary.ResourceTypes, tt.wantResTypes)

			if tt.wantDesc != "" {
				assert.Equal(t, tt.wantDesc, summary.Description)
			}
		})
	}
}

// ---- Backend: EstimateTemplateCost -----------------------------------------

func TestBackend_EstimateTemplateCost(t *testing.T) {
	t.Parallel()

	b := newBackend()
	url, err := b.EstimateTemplateCost(simpleTemplate, nil)

	require.NoError(t, err)
	assert.NotEmpty(t, url)
	assert.Contains(t, url, "calculator")
}

// ---- Backend: ContinueUpdateRollback ---------------------------------------

func TestBackend_ContinueUpdateRollback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		stackName   string
		forceStatus string
		wantStatus  string
		setup       bool
	}{
		{
			name:        "rollback_in_progress",
			stackName:   "my-stack",
			setup:       true,
			forceStatus: "ROLLBACK_IN_PROGRESS",
			wantStatus:  "ROLLBACK_COMPLETE",
		},
		{
			name:        "update_rollback_in_progress",
			stackName:   "my-stack",
			setup:       true,
			forceStatus: "UPDATE_ROLLBACK_IN_PROGRESS",
			wantStatus:  "UPDATE_ROLLBACK_COMPLETE",
		},
		{
			name:       "no_op_when_create_complete",
			stackName:  "my-stack",
			setup:      true,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:      "stack_not_found",
			stackName: "missing",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if tt.setup {
				_, err := b.CreateStack(t.Context(), tt.stackName, simpleTemplate, nil, nil)
				require.NoError(t, err)
			}

			if tt.forceStatus != "" {
				b.ForceStackStatus(tt.stackName, tt.forceStatus)
			}

			err := b.ContinueUpdateRollback(t.Context(), tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantStatus != "" {
				stack, descErr := b.DescribeStack(tt.stackName)
				require.NoError(t, descErr)
				assert.Equal(t, tt.wantStatus, stack.StackStatus)
			}
		})
	}
}

// ---- Backend: CancelUpdateStack --------------------------------------------

func TestBackend_CancelUpdateStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		stackName   string
		forceStatus string
		wantStatus  string
		setup       bool
	}{
		{
			name:        "update_in_progress",
			stackName:   "my-stack",
			setup:       true,
			forceStatus: "UPDATE_IN_PROGRESS",
			wantStatus:  "UPDATE_ROLLBACK_COMPLETE",
		},
		{
			name:       "no_op_when_create_complete",
			stackName:  "my-stack",
			setup:      true,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:      "stack_not_found",
			stackName: "missing",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup {
				_, err := b.CreateStack(t.Context(), tt.stackName, simpleTemplate, nil, nil)
				require.NoError(t, err)
			}

			if tt.forceStatus != "" {
				b.ForceStackStatus(tt.stackName, tt.forceStatus)
			}

			err := b.CancelUpdateStack(t.Context(), tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantStatus != "" {
				stack, descErr := b.DescribeStack(tt.stackName)
				require.NoError(t, descErr)
				assert.Equal(t, tt.wantStatus, stack.StackStatus)
			}
		})
	}
}

// ---- Backend: DescribeAccountLimits ----------------------------------------

func TestBackend_DescribeAccountLimits(t *testing.T) {
	t.Parallel()

	b := newBackend()
	limits := b.DescribeAccountLimits()

	assert.NotEmpty(t, limits)

	for _, l := range limits {
		assert.NotEmpty(t, l.Name)
		assert.Positive(t, l.Value)
	}
}

// ---- Handler: DetectStackDrift ---------------------------------------------

func TestHandler_DetectStackDrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantIDPath string
		wantCode   int
	}{
		{
			name: "success",
			body: "Action=CreateStack&StackName=drift-stack&TemplateBody=" + simpleTemplate,
		},
		{
			name:     "missing_stack_name",
			body:     "Action=DetectStackDrift",
			wantCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.wantCode == 0 {
				// Create stack first
				postForm(t, h, tt.body)
				rec := postForm(t, h, "Action=DetectStackDrift&StackName=drift-stack")
				assert.Equal(t, 200, rec.Code)

				var resp struct {
					XMLName xml.Name `xml:"DetectStackDriftResponse"`
					Result  struct {
						StackDriftDetectionID string `xml:"StackDriftDetectionId"`
					} `xml:"DetectStackDriftResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.Result.StackDriftDetectionID)
			} else {
				rec := postForm(t, h, tt.body)
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

// ---- Handler: DescribeStackDriftDetectionStatus ----------------------------

func TestHandler_DescribeStackDriftDetectionStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "success"},
		{name: "missing_detection_id", wantCode: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.wantCode != 0 {
				rec := postForm(t, h, "Action=DescribeStackDriftDetectionStatus")
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			postForm(t, h, "Action=CreateStack&StackName=test-stack&TemplateBody="+simpleTemplate)
			rec := postForm(t, h, "Action=DetectStackDrift&StackName=test-stack")
			assert.Equal(t, 200, rec.Code)

			var detectResp struct {
				Result struct {
					ID string `xml:"StackDriftDetectionId"`
				} `xml:"DetectStackDriftResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &detectResp))

			rec2 := postForm(
				t,
				h,
				"Action=DescribeStackDriftDetectionStatus&StackDriftDetectionId="+detectResp.Result.ID,
			)
			assert.Equal(t, 200, rec2.Code)

			var statusResp struct {
				Result struct {
					DetectionStatus string `xml:"DetectionStatus"`
					DriftStatus     string `xml:"StackDriftStatus"`
				} `xml:"DescribeStackDriftDetectionStatusResult"`
			}
			require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &statusResp))
			assert.Equal(t, "DETECTION_COMPLETE", statusResp.Result.DetectionStatus)
			assert.Equal(t, "IN_SYNC", statusResp.Result.DriftStatus)
		})
	}
}

// ---- Handler: DetectStackResourceDrift -------------------------------------

func TestHandler_DetectStackResourceDrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "missing_stack_name",
			body:     "Action=DetectStackResourceDrift&LogicalResourceId=MyBucket",
			wantCode: 400,
		},
		{
			name:     "missing_logical_id",
			body:     "Action=DetectStackResourceDrift&StackName=my-stack",
			wantCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		postForm(t, h, "Action=CreateStack&StackName=my-stack&TemplateBody="+simpleTemplate)
		rec := postForm(t, h, "Action=DetectStackResourceDrift&StackName=my-stack&LogicalResourceId=MyBucket")
		assert.Equal(t, 200, rec.Code)

		var resp struct {
			Result struct {
				ID string `xml:"StackDriftDetectionId"`
			} `xml:"DetectStackResourceDriftResult"`
		}
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.NotEmpty(t, resp.Result.ID)
	})
}

// ---- Handler: DescribeStackResourceDrifts ----------------------------------

func TestHandler_DescribeStackResourceDrifts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{name: "success"},
		{
			name:     "missing_stack_name",
			body:     "Action=DescribeStackResourceDrifts",
			wantCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.wantCode != 0 {
				rec := postForm(t, h, tt.body)
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			postForm(t, h, "Action=CreateStack&StackName=my-stack&TemplateBody="+simpleTemplate)
			rec := postForm(t, h, "Action=DescribeStackResourceDrifts&StackName=my-stack")
			assert.Equal(t, 200, rec.Code)

			var resp struct {
				Result struct {
					Drifts []struct {
						DriftStatus string `xml:"StackResourceDriftStatus"`
					} `xml:"StackResourceDrifts>member"`
				} `xml:"DescribeStackResourceDriftsResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			for _, d := range resp.Result.Drifts {
				assert.Equal(t, "IN_SYNC", d.DriftStatus)
			}
		})
	}
}

// ---- Handler: SetStackPolicy / GetStackPolicy ------------------------------

func TestHandler_StackPolicy(t *testing.T) {
	t.Parallel()

	policy := `{"Statement":[{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"}]}`

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{name: "success"},
		{
			name:     "set_missing_stack_name",
			body:     "Action=SetStackPolicy&StackPolicyBody=" + policy,
			wantCode: 400,
		},
		{
			name:     "get_missing_stack_name",
			body:     "Action=GetStackPolicy",
			wantCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.wantCode != 0 {
				rec := postForm(t, h, tt.body)
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			postForm(t, h, "Action=CreateStack&StackName=my-stack&TemplateBody="+simpleTemplate)

			rec := postForm(t, h, "Action=SetStackPolicy&StackName=my-stack&StackPolicyBody="+policy)
			assert.Equal(t, 200, rec.Code)

			rec2 := postForm(t, h, "Action=GetStackPolicy&StackName=my-stack")
			assert.Equal(t, 200, rec2.Code)

			var resp struct {
				Result struct {
					Body string `xml:"StackPolicyBody"`
				} `xml:"GetStackPolicyResult"`
			}
			require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
			assert.Equal(t, policy, resp.Result.Body)
		})
	}
}

// ---- Handler: GetTemplateSummary -------------------------------------------

func TestHandler_GetTemplateSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "from_template_body",
			body:     "Action=GetTemplateSummary&TemplateBody=" + simpleTemplate,
			wantCode: 200,
		},
		{
			name:     "empty_body_empty_stack",
			body:     "Action=GetTemplateSummary",
			wantCode: 200,
		},
		{
			name:     "stack_not_found",
			body:     "Action=GetTemplateSummary&StackName=missing-stack",
			wantCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- Handler: EstimateTemplateCost -----------------------------------------

func TestHandler_EstimateTemplateCost(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=EstimateTemplateCost&TemplateBody="+simpleTemplate)
	assert.Equal(t, 200, rec.Code)

	var resp struct {
		Result struct {
			URL string `xml:"Url"`
		} `xml:"EstimateTemplateCostResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Result.URL)
}

// ---- Handler: ContinueUpdateRollback ---------------------------------------

func TestHandler_ContinueUpdateRollback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{name: "success"},
		{
			name:     "missing_stack_name",
			body:     "Action=ContinueUpdateRollback",
			wantCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.wantCode != 0 {
				rec := postForm(t, h, tt.body)
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			postForm(t, h, "Action=CreateStack&StackName=my-stack&TemplateBody="+simpleTemplate)
			rec := postForm(t, h, "Action=ContinueUpdateRollback&StackName=my-stack")
			assert.Equal(t, 200, rec.Code)
		})
	}
}

// ---- Handler: CancelUpdateStack --------------------------------------------

func TestHandler_CancelUpdateStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{name: "success"},
		{
			name:     "missing_stack_name",
			body:     "Action=CancelUpdateStack",
			wantCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.wantCode != 0 {
				rec := postForm(t, h, tt.body)
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			postForm(t, h, "Action=CreateStack&StackName=my-stack&TemplateBody="+simpleTemplate)
			rec := postForm(t, h, "Action=CancelUpdateStack&StackName=my-stack")
			assert.Equal(t, 200, rec.Code)
		})
	}
}

// ---- Handler: DescribeAccountLimits ----------------------------------------

func TestHandler_DescribeAccountLimits(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DescribeAccountLimits")
	assert.Equal(t, 200, rec.Code)

	var resp struct {
		Result struct {
			Limits []struct {
				Name  string `xml:"Name"`
				Value int    `xml:"Value"`
			} `xml:"AccountLimits>member"`
		} `xml:"DescribeAccountLimitsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Result.Limits)
}

// ---- Handler: Chaos and metadata methods -----------------------------------

func TestHandler_ChaosAndMetadata(t *testing.T) {
	t.Parallel()

	h := newHandler()

	t.Run("chaos_service_name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "cloudformation", h.ChaosServiceName())
	})

	t.Run("chaos_operations", func(t *testing.T) {
		t.Parallel()
		ops := h.ChaosOperations()
		assert.NotEmpty(t, ops)
		assert.Contains(t, ops, "CreateStack")
		assert.Contains(t, ops, "DetectStackDrift")
	})

	t.Run("chaos_regions", func(t *testing.T) {
		t.Parallel()
		regions := h.ChaosRegions()
		assert.NotEmpty(t, regions)
	})
}

// ---- Handler: GetSupportedOperations includes new ops ----------------------

func TestHandler_GetSupportedOperationsExt(t *testing.T) {
	t.Parallel()

	h := newHandler()
	ops := h.GetSupportedOperations()

	newOps := []string{
		"DetectStackDrift",
		"DetectStackResourceDrift",
		"DescribeStackDriftDetectionStatus",
		"DescribeStackResourceDrifts",
		"SetStackPolicy",
		"GetStackPolicy",
		"GetTemplateSummary",
		"EstimateTemplateCost",
		"ContinueUpdateRollback",
		"CancelUpdateStack",
		"DescribeAccountLimits",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op)
	}
}

// ---- Persistence: snapshot/restore includes new state ----------------------

func TestPersistence_SnapshotRestoreWithExtState(t *testing.T) {
	t.Parallel()

	policy := `{"Statement":[{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"}]}`

	b := newBackend()

	_, err := b.CreateStack(t.Context(), "my-stack", simpleTemplate, nil, nil)
	require.NoError(t, err)

	err = b.SetStackPolicy("my-stack", policy)
	require.NoError(t, err)

	detectionID, err := b.DetectStackDrift("my-stack")
	require.NoError(t, err)
	assert.NotEmpty(t, detectionID)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	fresh := cloudformation.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(snap))

	gotPolicy, err := fresh.GetStackPolicy("my-stack")
	require.NoError(t, err)
	assert.Equal(t, policy, gotPolicy)

	status, err := fresh.DescribeStackDriftDetectionStatus(detectionID)
	require.NoError(t, err)
	assert.Equal(t, "DETECTION_COMPLETE", status.DetectionStatus)
}

func TestHandler_Snapshot_Restore_Delegation(t *testing.T) {
	t.Parallel()

	h := newHandler()

	_, err := h.Backend.(*cloudformation.InMemoryBackend).CreateStack(
		t.Context(), "snap-stack", simpleTemplate, nil, nil,
	)
	require.NoError(t, err)

	snap := h.Snapshot()
	require.NotNil(t, snap)

	h2 := newHandler()
	err = h2.Restore(snap)
	require.NoError(t, err)

	stack, err := h2.Backend.(*cloudformation.InMemoryBackend).DescribeStack("snap-stack")
	require.NoError(t, err)
	assert.Equal(t, "snap-stack", stack.StackName)
}

// ---- Backend: UpdateStack with invalid template (covers applyTemplateToStack error) ----

func TestBackend_UpdateStack_InvalidTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		updateBody string
		wantStatus string
	}{
		{
			name:       "invalid_template_body_on_update",
			updateBody: "{bad json}",
			wantStatus: "UPDATE_FAILED",
		},
		{
			name: "import_value_missing_on_update",
			updateBody: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyBucket": {
						"Type": "AWS::S3::Bucket",
						"Properties": {
							"BucketName": {"Fn::ImportValue": "NonExistentExport"}
						}
					}
				}
			}`,
			wantStatus: "UPDATE_FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateStack(t.Context(), "upd-stack", simpleTemplate, nil, nil)
			require.NoError(t, err)

			updated, err := b.UpdateStack(t.Context(), "upd-stack", tt.updateBody, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, updated.StackStatus)
		})
	}
}

// ---- Template: evalAndExpr and evalOrExpr coverage -------------------------

func TestBackend_TemplateConditions_AndOr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		template   string
		wantOutput string
	}{
		{
			name: "fn_and_both_true",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Parameters": {
					"Env": {"Type": "String", "Default": "prod"}
				},
				"Conditions": {
					"IsEnvProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
					"AlwaysTrue": {"Fn::Equals": ["x", "x"]},
					"BothTrue": {"Fn::And": [{"Condition": "IsEnvProd"}, {"Condition": "AlwaysTrue"}]}
				},
				"Resources": {
					"Placeholder": {"Type": "AWS::CloudFormation::WaitConditionHandle", "Properties": {}}
				},
				"Outputs": {
					"Result": {"Value": {"Fn::If": ["BothTrue", "yes", "no"]}}
				}
			}`,
			wantOutput: "yes",
		},
		{
			name: "fn_and_one_false",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Parameters": {
					"Env": {"Type": "String", "Default": "dev"}
				},
				"Conditions": {
					"IsEnvProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
					"AlwaysTrue": {"Fn::Equals": ["x", "x"]},
					"BothTrue": {"Fn::And": [{"Condition": "IsEnvProd"}, {"Condition": "AlwaysTrue"}]}
				},
				"Resources": {
					"Placeholder": {"Type": "AWS::CloudFormation::WaitConditionHandle", "Properties": {}}
				},
				"Outputs": {
					"Result": {"Value": {"Fn::If": ["BothTrue", "yes", "no"]}}
				}
			}`,
			wantOutput: "no",
		},
		{
			name: "fn_or_one_true",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Parameters": {
					"Env": {"Type": "String", "Default": "dev"}
				},
				"Conditions": {
					"IsEnvProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
					"IsEnvDev": {"Fn::Equals": [{"Ref": "Env"}, "dev"]},
					"EitherEnv": {"Fn::Or": [{"Condition": "IsEnvProd"}, {"Condition": "IsEnvDev"}]}
				},
				"Resources": {
					"Placeholder": {"Type": "AWS::CloudFormation::WaitConditionHandle", "Properties": {}}
				},
				"Outputs": {
					"Result": {"Value": {"Fn::If": ["EitherEnv", "yes", "no"]}}
				}
			}`,
			wantOutput: "yes",
		},
		{
			name: "fn_or_all_false",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Parameters": {
					"Env": {"Type": "String", "Default": "staging"}
				},
				"Conditions": {
					"IsEnvProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
					"IsEnvDev": {"Fn::Equals": [{"Ref": "Env"}, "dev"]},
					"EitherEnv": {"Fn::Or": [{"Condition": "IsEnvProd"}, {"Condition": "IsEnvDev"}]}
				},
				"Resources": {
					"Placeholder": {"Type": "AWS::CloudFormation::WaitConditionHandle", "Properties": {}}
				},
				"Outputs": {
					"Result": {"Value": {"Fn::If": ["EitherEnv", "yes", "no"]}}
				}
			}`,
			wantOutput: "no",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stack, err := b.CreateStack(t.Context(), tt.name, tt.template, nil, nil)
			require.NoError(t, err)
			require.NotNil(t, stack)

			var gotOutput string
			for _, out := range stack.Outputs {
				if out.OutputKey == "Result" {
					gotOutput = out.OutputValue

					break
				}
			}
			assert.Equal(t, tt.wantOutput, gotOutput)
		})
	}
}

// ---- Backend: resolveStack by StackID (ARN lookup) -------------------------

func TestBackend_ResolveStackByID(t *testing.T) {
	t.Parallel()

	b := newBackend()
	stack, err := b.CreateStack(t.Context(), "my-stack", simpleTemplate, nil, nil)
	require.NoError(t, err)

	// Look up by StackID (ARN) rather than name
	found, err := b.DescribeStack(stack.StackID)
	require.NoError(t, err)
	assert.Equal(t, "my-stack", found.StackName)
}

// ---- Handler: ChangeSet error paths ----------------------------------------

func TestHandler_ChangeSetErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "execute_not_found",
			body: "Action=ExecuteChangeSet&StackName=no-stack&ChangeSetName=no-cs",
		},
		{
			name: "delete_not_found",
			body: "Action=DeleteChangeSet&StackName=no-stack&ChangeSetName=no-cs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, 400, rec.Code)
		})
	}
}

// ---- Template: collectImportValuesFromValue with array ----------------------

func TestBackend_ListImports_WithArrayProperty(t *testing.T) {
	t.Parallel()

	// Template that exports a value
	exporterTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {}}
		},
		"Outputs": {
			"BucketOut": {
				"Value": "my-bucket",
				"Export": {"Name": "shared-bucket"}
			}
		}
	}`

	// Template that imports the exported value inside a list property
	importerTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyQueue": {
				"Type": "AWS::SQS::Queue",
				"Properties": {
					"Tags": [
						{"Key": "bucket", "Value": {"Fn::ImportValue": "shared-bucket"}}
					]
				}
			}
		}
	}`

	b := newBackend()

	_, err := b.CreateStack(t.Context(), "exporter", exporterTemplate, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateStack(t.Context(), "importer", importerTemplate, nil, nil)
	require.NoError(t, err)

	imports, err := b.ListImports("shared-bucket", "")
	require.NoError(t, err)
	assert.Contains(t, imports.Data, "importer")
}

// ---- Handler: new op error paths -------------------------------------------

func TestHandler_ExtOpsErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "detect_drift_stack_not_found",
			body: "Action=DetectStackDrift&StackName=no-such-stack",
		},
		{
			name: "detect_resource_drift_stack_not_found",
			body: "Action=DetectStackResourceDrift&StackName=no-such-stack&LogicalResourceId=MyBucket",
		},
		{
			name: "describe_drift_status_not_found",
			body: "Action=DescribeStackDriftDetectionStatus&StackDriftDetectionId=no-such-id",
		},
		{
			name: "describe_resource_drifts_stack_not_found",
			body: "Action=DescribeStackResourceDrifts&StackName=no-such-stack",
		},
		{
			name: "set_policy_stack_not_found",
			body: "Action=SetStackPolicy&StackName=no-such-stack&StackPolicyBody={}",
		},
		{
			name: "get_policy_stack_not_found",
			body: "Action=GetStackPolicy&StackName=no-such-stack",
		},
		{
			name: "continue_rollback_stack_not_found",
			body: "Action=ContinueUpdateRollback&StackName=no-such-stack",
		},
		{
			name: "cancel_update_stack_not_found",
			body: "Action=CancelUpdateStack&StackName=no-such-stack",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, 400, rec.Code)
		})
	}
}
