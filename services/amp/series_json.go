package amp

import (
	"encoding/json"
	"slices"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

// seriesPlain has SeriesRecord's fields without its JSON methods.
type seriesPlain SeriesRecord

type seriesWithStale struct {
	seriesPlain

	Samples []promql.Sample `json:"samples"`
	StaleAt []int64         `json:"staleAt,omitempty"`
}

// MarshalJSON writes stale markers as timestamps because JSON cannot carry NaN; other series are unchanged.
func (r SeriesRecord) MarshalJSON() ([]byte, error) {
	if !slices.ContainsFunc(r.Samples, func(s promql.Sample) bool { return promql.IsStale(s.V) }) {
		return json.Marshal(seriesPlain(r))
	}

	out := seriesWithStale{seriesPlain: seriesPlain(r)}

	for _, s := range r.Samples {
		if promql.IsStale(s.V) {
			out.StaleAt = append(out.StaleAt, s.T)
		} else {
			out.Samples = append(out.Samples, s)
		}
	}

	return json.Marshal(out)
}

// UnmarshalJSON restores stale markers in time order.
func (r *SeriesRecord) UnmarshalJSON(data []byte) error {
	var in seriesWithStale
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}

	*r = SeriesRecord(in.seriesPlain)
	r.Samples = in.Samples
	// JSON mangles the 0xff/0xfe separator bytes of the stored key, so rebuild it.
	r.Key = seriesKey(r.WorkspaceID, promql.Fingerprint(r.Labels))

	for _, t := range in.StaleAt {
		insertSample(r, promql.Sample{T: t, V: promql.StaleNaN()})
	}

	return nil
}
