package forecast

import (
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	// backtestWindowDuration is the synthetic span between a backtest window's
	// start and end in GetAccuracyMetrics responses.
	backtestWindowDuration = 24 * time.Hour

	// Synthetic accuracy-metric generation. The metrics returned by
	// GetAccuracyMetrics are deterministic, derived from a per-window seed so
	// the same resource always yields the same values. The constants below
	// name the otherwise-magic numbers used in that derivation.

	// windowSeedPrime is a prime multiplier mixed into the seed to vary
	// metrics between backtest windows.
	windowSeedPrime = 7919

	// Per-metric base values and the modulus/scale used to spread the seed
	// across a small synthetic range.
	rmseBase       = 10.0
	rmseSeedMod    = 500
	rmseSeedScale  = 10.0
	wapeBase       = 0.05
	wapeSeedMod    = 200
	wapeSeedScale  = 1000.0
	mapeBase       = 0.10
	mapeSeedMod    = 150
	mapeSeedScale  = 1000.0
	maseBase       = 0.50
	maseSeedMod    = 300
	maseSeedScale  = 1000.0
	lossValueBase  = 0.02
	lossValueMod   = 100
	lossValueScale = 1000.0
	itemCountBase  = 100
	itemCountMod   = 900
)

// GetAccuracyMetrics returns deterministic backtest accuracy metrics for a
// predictor, modeled on the AWS Forecast GetAccuracyMetrics response shape
// (PredictorEvaluationResults -> TestWindows -> Metrics with RMSE, weighted
// quantile losses, and WAPE/MAPE/MASE error metrics). Values are derived from a
// stable hash of the predictor ARN so repeated calls return identical numbers,
// which is what a Terraform/SDK client comparing state expects. This exceeds
// LocalStack, which returns no evaluation results at all.
func (b *InMemoryBackend) GetAccuracyMetrics(predictorArn string) (map[string]any, error) {
	b.mu.RLock("GetAccuracyMetrics")
	defer b.mu.RUnlock()

	resource, ok := b.lookupLocked(kindPredictor, predictorArn)
	if !ok {
		return nil, fmt.Errorf("%w: predictor %q", ErrNotFound, predictorArn)
	}

	quantiles := predictorQuantiles(resource)
	seed := stableSeed(resource.ARN)

	// Two backtest windows is AWS's default (NumberOfBacktestWindows defaults to 1,
	// but the response always carries at least the configured count).
	numWindows := backtestWindowCount(resource)
	windows := make([]map[string]any, 0, numWindows)

	for w := range numWindows {
		windowSeed := seed + uint32(w)*windowSeedPrime

		rmse := rmseBase + float64(windowSeed%rmseSeedMod)/rmseSeedScale
		wape := wapeBase + float64(windowSeed%wapeSeedMod)/wapeSeedScale
		mape := mapeBase + float64(windowSeed%mapeSeedMod)/mapeSeedScale
		mase := maseBase + float64(windowSeed%maseSeedMod)/maseSeedScale

		// Quantile is a JSON number (or the Smithy-special "NaN"/"Infinity"/
		// "-Infinity" strings) -- confirmed against aws-sdk-go-v2/service/
		// forecast@v1.44.4's deserializers.go
		// (awsAwsjson11_deserializeDocumentWeightedQuantileLoss). Any other
		// string, including a plain "0.1" label, fails GetAccuracyMetrics'
		// decode with "unknown JSON number value". ForecastTypes can also
		// include "mean", which is not a quantile and has no
		// WeightedQuantileLosses entry in the real API.
		quantileLosses := make([]map[string]any, 0, len(quantiles))
		for i, q := range quantiles {
			qv, err := strconv.ParseFloat(q, 64)
			if err != nil {
				continue
			}

			quantileLosses = append(quantileLosses, map[string]any{
				"Quantile":  qv,
				"LossValue": lossValueBase + float64((windowSeed+uint32(i))%lossValueMod)/lossValueScale,
			})
		}

		windows = append(windows, map[string]any{
			"EvaluationType":  evaluationTypeForWindow(w),
			"ItemCount":       int64(itemCountBase + windowSeed%itemCountMod),
			"TestWindowStart": awstime.Epoch(resource.CreatedAt.UTC()),
			"TestWindowEnd":   awstime.Epoch(resource.CreatedAt.UTC().Add(backtestWindowDuration)),
			"Metrics": map[string]any{
				"RMSE":                   rmse,
				"WeightedQuantileLosses": quantileLosses,
				"ErrorMetrics": []map[string]any{
					{
						"ForecastType": "mean",
						"WAPE":         wape,
						"MAPE":         mape,
						"MASE":         mase,
						"RMSE":         rmse,
					},
				},
				"AverageWeightedQuantileLoss": averageQuantileLoss(quantileLosses),
			},
		})
	}

	return map[string]any{
		"PredictorEvaluationResults": []map[string]any{
			{
				"AlgorithmArn": "arn:aws:forecast:::algorithm/CNN-QR",
				"TestWindows":  windows,
			},
		},
		"IsAutoPredictor": true,
	}, nil
}

// stableSeed returns a deterministic 32-bit value derived from s.
func stableSeed(s string) uint32 {
	return httputils.FNV32a(s)
}

// predictorQuantiles returns the forecast quantiles configured on the predictor,
// defaulting to AWS's default set when none were provided.
func predictorQuantiles(r *Resource) []string {
	if raw, ok := r.Data["ForecastTypes"].([]any); ok && len(raw) > 0 {
		out := make([]string, 0, len(raw))

		for _, v := range raw {
			if s, isStr := v.(string); isStr && s != "" {
				out = append(out, s)
			}
		}

		if len(out) > 0 {
			return out
		}
	}

	return []string{"0.1", "0.5", "0.9"}
}

// backtestWindowCount returns the configured number of backtest windows
// (defaulting to 1, AWS's default).
func backtestWindowCount(r *Resource) int {
	if eval, ok := r.Data["EvaluationParameters"].(map[string]any); ok {
		if n, isNum := eval["NumberOfBacktestWindows"].(float64); isNum && n >= 1 {
			return int(n)
		}
	}

	return 1
}

func evaluationTypeForWindow(window int) string {
	if window == 0 {
		return "SUMMARY"
	}

	return "COMPUTED"
}

func averageQuantileLoss(losses []map[string]any) float64 {
	if len(losses) == 0 {
		return 0
	}

	var sum float64

	for _, l := range losses {
		if v, ok := l["LossValue"].(float64); ok {
			sum += v
		}
	}

	return sum / float64(len(losses))
}
