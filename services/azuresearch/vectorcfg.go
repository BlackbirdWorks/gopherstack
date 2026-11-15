package azuresearch

import (
	"math"
	"net/url"
	"slices"
)

const (
	compScalar      = "scalarQuantization"
	compBinary      = "binaryQuantization"
	vecKindCustom   = "customWebApi"
	vecKindOpenAI   = "azureOpenAI"
	vecKindVision   = "aiServicesVision"
	vecKindAML      = "aml"
	storeDiscard    = "discardOriginals"
	storePreserve   = "preserveOriginals"
	quantizedInt8   = "int8"
	minOversampling = 1.0
)

func (c *compiler) compressions(vs map[string]any) (map[string]vectorProfile, error) {
	out := map[string]vectorProfile{}

	for _, raw := range listOf(vs, "compressions") {
		m := objOf(raw)
		name, kind := strOf(m, "name"), strOf(m, "kind")

		if name == "" {
			return nil, badf("The request is invalid. Details: a vector compression requires a name.")
		}

		if _, dup := out[name]; dup {
			return nil, badf("The request is invalid. Details: duplicate vector compression '%s'.", name)
		}

		if kind != compScalar && kind != compBinary {
			return nil, badf("The request is invalid. Details: unsupported vector compression kind '%s'.", kind)
		}

		cp, err := compressionOptions(m, kind)
		if err != nil {
			return nil, err
		}

		cp.Compression = name
		out[name] = cp
	}

	return out, nil
}

func compressionOptions(m map[string]any, kind string) (vectorProfile, error) {
	cp := vectorProfile{Quantizer: kind, Rescore: true, Oversample: minOversampling}

	if dt := strOf(objOf(m["scalarQuantizationParameters"]), "quantizedDataType"); dt != "" && dt != quantizedInt8 {
		return cp, badf("The request is invalid. Details: unsupported quantizedDataType '%s'.", dt)
	}

	if v, ok := m["rerankWithOriginalVectors"].(bool); ok {
		cp.Rescore = v
	}

	over, hasOver := numOf(m, "defaultOversampling")

	if ro := objOf(m["rescoringOptions"]); ro != nil {
		if v, ok := ro["enableRescoring"].(bool); ok {
			cp.Rescore = v
		}

		if o, ok := numOf(ro, "defaultOversampling"); ok {
			over, hasOver = o, true
		}

		switch st := strOf(ro, "rescoreStorageMethod"); st {
		case "", storePreserve:
		case storeDiscard:
			cp.Rescore = false
		default:
			return cp, badf("The request is invalid. Details: unsupported rescoreStorageMethod '%s'.", st)
		}
	}

	if hasOver {
		if over < minOversampling {
			return cp, badf("The request is invalid. Details: defaultOversampling must be at least 1.")
		}

		if !cp.Rescore {
			return cp, badf("The request is invalid. Details: defaultOversampling requires rescoring to be enabled.")
		}

		cp.Oversample = over
	}

	return cp, nil
}

func numOf(m map[string]any, key string) (float64, bool) {
	f, ok := asFloat(m[key])

	return f, ok
}

func (c *compiler) vectorizerDefs(vs map[string]any) error {
	kinds := []string{vecKindOpenAI, vecKindCustom, vecKindVision, vecKindAML}

	for _, raw := range listOf(vs, "vectorizers") {
		m := objOf(raw)
		name, kind := strOf(m, "name"), strOf(m, "kind")

		if name == "" || !slices.Contains(kinds, kind) {
			return badf("The request is invalid. Details: vectorizer '%s' has unsupported kind '%s'.", name, kind)
		}

		if _, dup := c.comp.vectorizers[name]; dup {
			return badf("The request is invalid. Details: duplicate vectorizer '%s'.", name)
		}

		if kind == vecKindCustom {
			u, err := url.Parse(strOf(objOf(m["customWebApiParameters"]), "uri"))
			if err != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" {
				return badf(
					"The request is invalid. Details: vectorizer '%s' requires a customWebApiParameters.uri.",
					name,
				)
			}
		}

		c.comp.vectorizers[name] = m
	}

	return nil
}

func (c *compiler) bindProfile(
	p map[string]any,
	alg vectorProfile,
	comps map[string]vectorProfile,
) (vectorProfile, error) {
	if name := strOf(p, "compression"); name != "" {
		cp, ok := comps[name]
		if !ok {
			return alg, badf(
				"The request is invalid. Details: vector search profile '%s' references unknown compression '%s'.",
				strOf(p, "name"),
				name,
			)
		}

		alg.Compression, alg.Quantizer, alg.Rescore, alg.Oversample = name, cp.Quantizer, cp.Rescore, cp.Oversample
	}

	if name := strOf(p, "vectorizer"); name != "" {
		if _, ok := c.comp.vectorizers[name]; !ok {
			return alg, badf(
				"The request is invalid. Details: vector search profile '%s' references unknown vectorizer '%s'.",
				strOf(p, "name"),
				name,
			)
		}

		alg.Vectorizer = name
	}

	return alg, nil
}

// quantize returns the vector as the compressed index stores it.
func quantize(kind string, v []float64) []float64 {
	out := make([]float64, len(v))

	switch kind {
	case compBinary:
		for i, x := range v {
			out[i] = -1
			if x > 0 {
				out[i] = 1
			}
		}
	case compScalar:
		lo, hi := slices.Min(v), slices.Max(v)
		if hi == lo {
			copy(out, v)

			return out
		}

		const levels = 255.0

		step := (hi - lo) / levels
		for i, x := range v {
			out[i] = lo + math.Round((x-lo)/step)*step
		}
	default:
		copy(out, v)
	}

	return out
}
