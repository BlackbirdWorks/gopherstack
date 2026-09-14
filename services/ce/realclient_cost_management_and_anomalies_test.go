package ce_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	costexplorersdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestRealClient_CostManagementAndAnomalies drives every remaining uncovered op through a real
// aws-sdk-go-v2 costexplorer client: DeleteAnomalySubscription,
// DeleteCostCategoryDefinition, GetApproximateUsageRecords,
// GetCostAndUsageWithResources, GetCostComparisonDrivers,
// GetReservationPurchaseRecommendation, GetReservationUtilization,
// GetSavingsPlanPurchaseRecommendationDetails,
// GetSavingsPlansPurchaseRecommendation, GetTags, GetUsageForecast,
// ListCostAllocationTags, ListCostCategoryResourceAssociations,
// ListTagsForResource, ProvideAnomalyFeedback, TagResource, UntagResource,
// UpdateAnomalyMonitor, UpdateCostAllocationTagsStatus.
func TestRealClient_CostManagementAndAnomalies(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "cost category: tags, DeleteCostCategoryDefinition, ListCostCategoryResourceAssociations",
			run: func(t *testing.T) {
				t.Helper()

				h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
				client := newTestCEClient(t, h)
				ctx := t.Context()

				created, err := client.CreateCostCategoryDefinition(
					ctx,
					&costexplorersdk.CreateCostCategoryDefinitionInput{
						Name:        aws.String("slice25-cost-category"),
						RuleVersion: cetypes.CostCategoryRuleVersionCostCategoryExpressionV1,
						Rules: []cetypes.CostCategoryRule{
							{Value: aws.String("slice25-value")},
						},
					},
				)
				require.NoError(t, err)
				catArn := aws.ToString(created.CostCategoryArn)
				require.NotEmpty(t, catArn)

				_, err = client.TagResource(ctx, &costexplorersdk.TagResourceInput{
					ResourceArn: aws.String(catArn),
					ResourceTags: []cetypes.ResourceTag{
						{Key: aws.String("env"), Value: aws.String("test")},
					},
				})
				require.NoError(t, err)

				tagsOut, err := client.ListTagsForResource(
					ctx,
					&costexplorersdk.ListTagsForResourceInput{
						ResourceArn: aws.String(catArn),
					},
				)
				require.NoError(t, err)
				require.Len(t, tagsOut.ResourceTags, 1)
				assert.Equal(t, "env", aws.ToString(tagsOut.ResourceTags[0].Key))

				_, err = client.UntagResource(ctx, &costexplorersdk.UntagResourceInput{
					ResourceArn:     aws.String(catArn),
					ResourceTagKeys: []string{"env"},
				})
				require.NoError(t, err)

				tagsOut, err = client.ListTagsForResource(
					ctx,
					&costexplorersdk.ListTagsForResourceInput{
						ResourceArn: aws.String(catArn),
					},
				)
				require.NoError(t, err)
				assert.Empty(t, tagsOut.ResourceTags)

				assocOut, err := client.ListCostCategoryResourceAssociations(
					ctx,
					&costexplorersdk.ListCostCategoryResourceAssociationsInput{
						CostCategoryArn: aws.String(catArn),
					},
				)
				require.NoError(t, err)
				assert.NotNil(t, assocOut.CostCategoryResourceAssociations)

				_, err = client.DeleteCostCategoryDefinition(
					ctx,
					&costexplorersdk.DeleteCostCategoryDefinitionInput{
						CostCategoryArn: aws.String(catArn),
					},
				)
				require.NoError(t, err)

				_, err = client.DescribeCostCategoryDefinition(
					ctx,
					&costexplorersdk.DescribeCostCategoryDefinitionInput{
						CostCategoryArn: aws.String(catArn),
					},
				)
				require.Error(t, err)
			},
		},
		{
			name: "anomaly family: UpdateAnomalyMonitor, DeleteAnomalySubscription, ProvideAnomalyFeedback",
			run: func(t *testing.T) {
				t.Helper()

				h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
				client := newTestCEClient(t, h)
				ctx := t.Context()

				monOut, err := client.CreateAnomalyMonitor(
					ctx,
					&costexplorersdk.CreateAnomalyMonitorInput{
						AnomalyMonitor: &cetypes.AnomalyMonitor{
							MonitorName:      aws.String("slice25-monitor"),
							MonitorType:      cetypes.MonitorTypeDimensional,
							MonitorDimension: cetypes.MonitorDimensionService,
						},
					},
				)
				require.NoError(t, err)
				monArn := aws.ToString(monOut.MonitorArn)

				updated, err := client.UpdateAnomalyMonitor(
					ctx,
					&costexplorersdk.UpdateAnomalyMonitorInput{
						MonitorArn:  aws.String(monArn),
						MonitorName: aws.String("slice25-monitor-renamed"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, monArn, aws.ToString(updated.MonitorArn))

				gotMon, err := client.GetAnomalyMonitors(ctx, &costexplorersdk.GetAnomalyMonitorsInput{
					MonitorArnList: []string{monArn},
				})
				require.NoError(t, err)
				require.Len(t, gotMon.AnomalyMonitors, 1)
				assert.Equal(
					t,
					"slice25-monitor-renamed",
					aws.ToString(gotMon.AnomalyMonitors[0].MonitorName),
				)

				subOut, err := client.CreateAnomalySubscription(
					ctx,
					&costexplorersdk.CreateAnomalySubscriptionInput{
						AnomalySubscription: &cetypes.AnomalySubscription{
							SubscriptionName: aws.String("slice25-subscription"),
							Frequency:        cetypes.AnomalySubscriptionFrequencyDaily,
							MonitorArnList:   []string{monArn},
							Subscribers: []cetypes.Subscriber{
								{
									Address: aws.String("a@example.com"),
									Type:    cetypes.SubscriberTypeEmail,
								},
							},
							ThresholdExpression: &cetypes.Expression{
								Dimensions: &cetypes.DimensionValues{
									Key:    cetypes.DimensionAnomalyTotalImpactAbsolute,
									Values: []string{"50"},
								},
							},
						},
					},
				)
				require.NoError(t, err)
				subArn := aws.ToString(subOut.SubscriptionArn)
				require.NotEmpty(t, subArn)

				h.Backend.AddAnomaly(ce.Anomaly{
					MonitorARN:      monArn,
					SubscriptionARN: subArn,
					DimensionValue:  "Amazon Elastic Compute Cloud - Compute",
				})

				anomOut, err := client.GetAnomalies(ctx, &costexplorersdk.GetAnomaliesInput{
					MonitorArn: aws.String(monArn),
					DateInterval: &cetypes.AnomalyDateInterval{
						StartDate: aws.String("2024-01-01"),
					},
				})
				require.NoError(t, err)
				require.Len(t, anomOut.Anomalies, 1)
				anomalyID := aws.ToString(anomOut.Anomalies[0].AnomalyId)
				require.NotEmpty(t, anomalyID)

				fbOut, err := client.ProvideAnomalyFeedback(
					ctx,
					&costexplorersdk.ProvideAnomalyFeedbackInput{
						AnomalyId: aws.String(anomalyID),
						Feedback:  cetypes.AnomalyFeedbackTypeYes,
					},
				)
				require.NoError(t, err)
				assert.Equal(t, anomalyID, aws.ToString(fbOut.AnomalyId))

				_, err = client.DeleteAnomalySubscription(
					ctx,
					&costexplorersdk.DeleteAnomalySubscriptionInput{
						SubscriptionArn: aws.String(subArn),
					},
				)
				require.NoError(t, err)

				_, err = client.GetAnomalySubscriptions(
					ctx,
					&costexplorersdk.GetAnomalySubscriptionsInput{
						SubscriptionArnList: []string{subArn},
					},
				)
				require.Error(t, err)

				gotSub, err := client.GetAnomalySubscriptions(
					ctx,
					&costexplorersdk.GetAnomalySubscriptionsInput{
						MonitorArn: aws.String(monArn),
					},
				)
				require.NoError(t, err)
				assert.Empty(t, gotSub.AnomalySubscriptions)
			},
		},
		{
			name: "reservations: GetReservationPurchaseRecommendation, GetReservationUtilization",
			run: func(t *testing.T) {
				t.Helper()

				h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
				client := newTestCEClient(t, h)
				ctx := t.Context()

				purchaseOut, err := client.GetReservationPurchaseRecommendation(
					ctx,
					&costexplorersdk.GetReservationPurchaseRecommendationInput{
						Service:              aws.String("Amazon Elastic Compute Cloud - Compute"),
						LookbackPeriodInDays: cetypes.LookbackPeriodInDaysSixtyDays,
						TermInYears:          cetypes.TermInYearsOneYear,
						PaymentOption:        cetypes.PaymentOptionNoUpfront,
					},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, purchaseOut.Recommendations)

				utilOut, err := client.GetReservationUtilization(
					ctx,
					&costexplorersdk.GetReservationUtilizationInput{
						TimePeriod: &cetypes.DateInterval{
							Start: aws.String("2024-01-01"),
							End:   aws.String("2024-02-01"),
						},
						Granularity: cetypes.GranularityMonthly,
					},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, utilOut.UtilizationsByTime)
			},
		},
		{
			name: "savings plans: GetSavingsPlanPurchaseRecommendationDetails, GetSavingsPlansPurchaseRecommendation",
			run: func(t *testing.T) {
				t.Helper()

				h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
				client := newTestCEClient(t, h)
				ctx := t.Context()

				recOut, err := client.GetSavingsPlansPurchaseRecommendation(
					ctx,
					&costexplorersdk.GetSavingsPlansPurchaseRecommendationInput{
						LookbackPeriodInDays: cetypes.LookbackPeriodInDaysSixtyDays,
						PaymentOption:        cetypes.PaymentOptionNoUpfront,
						SavingsPlansType:     cetypes.SupportedSavingsPlansTypeComputeSp,
						TermInYears:          cetypes.TermInYearsOneYear,
					},
				)
				require.NoError(t, err)
				require.NotNil(t, recOut.SavingsPlansPurchaseRecommendation)

				detailsOut, err := client.GetSavingsPlanPurchaseRecommendationDetails(
					ctx,
					&costexplorersdk.GetSavingsPlanPurchaseRecommendationDetailsInput{
						RecommendationDetailId: aws.String("slice25-rec-detail-id"),
					},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					"slice25-rec-detail-id",
					aws.ToString(detailsOut.RecommendationDetailId),
				)
				require.NotNil(t, detailsOut.RecommendationDetailData)
			},
		},
		{name: "cost usage and forecast family", run: func(t *testing.T) {
			t.Helper()

			h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestCEClient(t, h)
			ctx := t.Context()

			tagsOut, err := client.GetTags(ctx, &costexplorersdk.GetTagsInput{
				TimePeriod: &cetypes.DateInterval{
					Start: aws.String("2024-01-01"),
					End:   aws.String("2024-02-01"),
				},
			})
			require.NoError(t, err)
			assert.NotNil(t, tagsOut.Tags)

			forecastOut, err := client.GetUsageForecast(ctx, &costexplorersdk.GetUsageForecastInput{
				TimePeriod: &cetypes.DateInterval{
					Start: aws.String("2024-06-01"),
					End:   aws.String("2024-09-01"),
				},
				Granularity: cetypes.GranularityMonthly,
				Metric:      cetypes.MetricUsageQuantity,
			})
			require.NoError(t, err)
			require.NotNil(t, forecastOut.Total)
			assert.NotEmpty(t, aws.ToString(forecastOut.Total.Amount))

			usageOut, err := client.GetApproximateUsageRecords(
				ctx,
				&costexplorersdk.GetApproximateUsageRecordsInput{
					ApproximationDimension: cetypes.ApproximationDimensionService,
					Granularity:            cetypes.GranularityDaily,
				},
			)
			require.NoError(t, err)
			assert.NotNil(t, usageOut.Services)

			withResOut, err := client.GetCostAndUsageWithResources(
				ctx,
				&costexplorersdk.GetCostAndUsageWithResourcesInput{
					Filter: &cetypes.Expression{
						Dimensions: &cetypes.DimensionValues{
							Key:    cetypes.DimensionService,
							Values: []string{"Amazon Elastic Compute Cloud - Compute"},
						},
					},
					TimePeriod: &cetypes.DateInterval{
						Start: aws.String("2024-01-01"),
						End:   aws.String("2024-02-01"),
					},
					Granularity: cetypes.GranularityMonthly,
					Metrics:     []string{"UnblendedCost"},
				},
			)
			require.NoError(t, err)
			assert.NotNil(t, withResOut.ResultsByTime)

			driversOut, err := client.GetCostComparisonDrivers(
				ctx,
				&costexplorersdk.GetCostComparisonDriversInput{
					BaselineTimePeriod: &cetypes.DateInterval{
						Start: aws.String("2024-01-01"),
						End:   aws.String("2024-02-01"),
					},
					ComparisonTimePeriod: &cetypes.DateInterval{
						Start: aws.String("2024-02-01"),
						End:   aws.String("2024-03-01"),
					},
					MetricForComparison: aws.String("UnblendedCost"),
				},
			)
			require.NoError(t, err)
			assert.NotNil(t, driversOut.CostComparisonDrivers)
		}},
		{name: "cost allocation tags: ListCostAllocationTags, UpdateCostAllocationTagsStatus", run: func(t *testing.T) {
			t.Helper()

			h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestCEClient(t, h)
			ctx := t.Context()

			_, err := client.UpdateCostAllocationTagsStatus(
				ctx,
				&costexplorersdk.UpdateCostAllocationTagsStatusInput{
					CostAllocationTagsStatus: []cetypes.CostAllocationTagStatusEntry{
						{
							TagKey: aws.String("slice25-tag"),
							Status: cetypes.CostAllocationTagStatusActive,
						},
					},
				},
			)
			require.NoError(t, err)

			listOut, err := client.ListCostAllocationTags(
				ctx,
				&costexplorersdk.ListCostAllocationTagsInput{
					TagKeys: []string{"slice25-tag"},
				},
			)
			require.NoError(t, err)
			require.Len(t, listOut.CostAllocationTags, 1)
			assert.Equal(t, "slice25-tag", aws.ToString(listOut.CostAllocationTags[0].TagKey))
			assert.Equal(
				t,
				cetypes.CostAllocationTagStatusActive,
				listOut.CostAllocationTags[0].Status,
			)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
