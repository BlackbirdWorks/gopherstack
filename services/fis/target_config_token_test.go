package fis_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	fissdk "github.com/aws/aws-sdk-go-v2/service/fis"
	fistypes "github.com/aws/aws-sdk-go-v2/service/fis/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

func TestCreateTargetAccountConfigurationDuplicatesAndTokens(t *testing.T) {
	t.Parallel()

	const (
		accountID = "111111111111"
		roleA     = "arn:aws:iam::1:role/a"
		roleB     = "arn:aws:iam::1:role/b"
	)

	tests := []struct {
		name       string
		firstToken string
		nextToken  string
		nextRole   string
		wantCode   string
		wantRole   string
	}{
		{name: "same_token_replays", firstToken: "tok", nextToken: "tok", nextRole: roleA, wantRole: roleA},
		{
			name: "token_other_params", firstToken: "tok", nextToken: "tok", nextRole: roleB,
			wantCode: "ConflictException",
		},
		{name: "duplicate_no_token", nextRole: roleB, wantCode: "ConflictException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := fis.NewInMemoryBackend("000000000000", "us-east-1")
			t.Cleanup(backend.Close)
			client, _ := newTestFISClient(t, fis.NewHandler(backend))
			ctx := t.Context()

			tpl, err := client.CreateExperimentTemplate(ctx, &fissdk.CreateExperimentTemplateInput{
				Description:    aws.String("tpl"),
				RoleArn:        aws.String("arn:aws:iam::000000000000:role/FISRole"),
				StopConditions: []fistypes.CreateExperimentTemplateStopConditionInput{{Source: aws.String("none")}},
				Actions: map[string]fistypes.CreateExperimentTemplateActionInput{
					"w": {ActionId: aws.String("aws:fis:wait"), Parameters: map[string]string{"duration": "PT1S"}},
				},
			})
			require.NoError(t, err)

			create := func(token, role string) (*fissdk.CreateTargetAccountConfigurationOutput, error) {
				in := &fissdk.CreateTargetAccountConfigurationInput{
					ExperimentTemplateId: tpl.ExperimentTemplate.Id,
					AccountId:            aws.String(accountID),
					RoleArn:              aws.String(role),
				}
				if token != "" {
					in.ClientToken = aws.String(token)
				}

				return client.CreateTargetAccountConfiguration(ctx, in)
			}

			_, err = create(tt.firstToken, roleA)
			require.NoError(t, err)

			out, err := create(tt.nextToken, tt.nextRole)
			if tt.wantCode != "" {
				var apiErr smithy.APIError

				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				got, getErr := client.GetTargetAccountConfiguration(ctx, &fissdk.GetTargetAccountConfigurationInput{
					ExperimentTemplateId: tpl.ExperimentTemplate.Id, AccountId: aws.String(accountID),
				})
				require.NoError(t, getErr)
				assert.Equal(t, roleA, aws.ToString(got.TargetAccountConfiguration.RoleArn))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantRole, aws.ToString(out.TargetAccountConfiguration.RoleArn))
		})
	}
}

func TestListExperimentResolvedTargetsTargetNameFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{name: "no_filter", query: "", want: []string{"Target0", "Target1", "Target2"}},
		{name: "one_name", query: "?targetName=Target1", want: []string{"Target1"}},
		{name: "unknown_name", query: "?targetName=Nope", want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := fis.NewTestBackend(t)
			h := fis.NewHandler(b)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			targets := make(map[string]fis.ExperimentTarget, 3)
			for i := range 3 {
				targets[fmt.Sprintf("Target%d", i)] = fis.ExperimentTarget{ResourceType: "aws:ec2:instance"}
			}

			b.InjectExperiment(&fis.Experiment{
				ID: "EXPtest000000000000000000009", Status: fis.ExperimentStatus{Status: "running"},
				Targets: targets, CreationTime: time.Now(),
			})

			path := "/experiments/EXPtest000000000000000000009/resolvedTargets" + tt.query
			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ResolvedTargets []struct {
					TargetName string `json:"targetName"`
				} `json:"resolvedTargets"`
			}

			mustJSON(t, rec, &resp)

			got := make([]string, 0, len(resp.ResolvedTargets))
			for _, rt := range resp.ResolvedTargets {
				got = append(got, rt.TargetName)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExperimentOptionsEnumsValidated(t *testing.T) {
	t.Parallel()

	type opts = fistypes.CreateExperimentTemplateExperimentOptionsInput

	tests := []struct {
		options  *opts
		name     string
		wantCode string
	}{
		{name: "valid", options: &opts{
			AccountTargeting:          fistypes.AccountTargetingMultiAccount,
			EmptyTargetResolutionMode: fistypes.EmptyTargetResolutionModeSkip,
		}},
		{name: "bad_account_targeting", wantCode: "ValidationException", options: &opts{AccountTargeting: "all"}},
		{name: "bad_empty_mode", wantCode: "ValidationException", options: &opts{EmptyTargetResolutionMode: "ignore"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := fis.NewInMemoryBackend("000000000000", "us-east-1")
			t.Cleanup(backend.Close)
			client, _ := newTestFISClient(t, fis.NewHandler(backend))

			_, err := client.CreateExperimentTemplate(t.Context(), &fissdk.CreateExperimentTemplateInput{
				Description:       aws.String("tpl"),
				RoleArn:           aws.String("arn:aws:iam::000000000000:role/FISRole"),
				ExperimentOptions: tt.options,
				StopConditions:    []fistypes.CreateExperimentTemplateStopConditionInput{{Source: aws.String("none")}},
				Actions: map[string]fistypes.CreateExperimentTemplateActionInput{
					"w": {ActionId: aws.String("aws:fis:wait"), Parameters: map[string]string{"duration": "PT1S"}},
				},
			})
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
		})
	}
}
