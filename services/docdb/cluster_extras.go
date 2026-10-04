package docdb

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
)

const (
	networkTypeIPv4 = "IPV4"
	networkTypeDual = "DUAL"
	halfStepDCU     = 0.5
)

// ServerlessV2Scaling mirrors the SDK's ServerlessV2ScalingConfiguration (DCUs).
type ServerlessV2Scaling struct {
	MinCapacity *float64 `json:"minCapacity,omitempty"`
	MaxCapacity *float64 `json:"maxCapacity,omitempty"`
}

// ClusterExtras holds the NetworkType and serverless scaling inputs shared by
// the cluster create, modify and restore operations.
type ClusterExtras struct {
	Scaling     *ServerlessV2Scaling
	NetworkType string
}

func parseClusterExtras(vals url.Values) ClusterExtras {
	e := ClusterExtras{NetworkType: vals.Get("NetworkType")}
	var s ServerlessV2Scaling
	if f, ok := parseFloatParam(vals, "ServerlessV2ScalingConfiguration.MinCapacity"); ok {
		s.MinCapacity = &f
	}
	if f, ok := parseFloatParam(vals, "ServerlessV2ScalingConfiguration.MaxCapacity"); ok {
		s.MaxCapacity = &f
	}
	if s.MinCapacity != nil || s.MaxCapacity != nil {
		e.Scaling = &s
	}

	return e
}

func parseFloatParam(vals url.Values, key string) (float64, bool) {
	s := vals.Get(key)
	if s == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return math.NaN(), true
	}

	return f, true
}

func validateNetworkType(nt string) error {
	if nt == "" || nt == networkTypeIPv4 || nt == networkTypeDual {
		return nil
	}

	return fmt.Errorf("%w: NetworkType %q is not valid; valid values: IPV4, DUAL", ErrInvalidParameter, nt)
}

// validateScaling checks half-step DCU increments (SDK doc) and Min <= Max.
func validateScaling(s *ServerlessV2Scaling) error {
	if s == nil {
		return nil
	}
	for name, v := range map[string]*float64{"MinCapacity": s.MinCapacity, "MaxCapacity": s.MaxCapacity} {
		if v == nil {
			continue
		}
		if math.IsNaN(*v) || math.IsInf(*v, 0) || *v < 0 || math.Mod(*v, halfStepDCU) != 0 {
			return fmt.Errorf(
				"%w: ServerlessV2ScalingConfiguration.%s must be a non-negative half-step value",
				ErrInvalidParameter, name,
			)
		}
	}
	if s.MinCapacity != nil && s.MaxCapacity != nil && *s.MinCapacity > *s.MaxCapacity {
		return fmt.Errorf(
			"%w: ServerlessV2ScalingConfiguration.MinCapacity must not exceed MaxCapacity",
			ErrInvalidParameter,
		)
	}

	return nil
}

func mergeScaling(cur, in *ServerlessV2Scaling) *ServerlessV2Scaling {
	if in == nil {
		return cur
	}
	out := &ServerlessV2Scaling{}
	if cur != nil {
		*out = *copyScaling(cur)
	}
	if in.MinCapacity != nil {
		v := *in.MinCapacity
		out.MinCapacity = &v
	}
	if in.MaxCapacity != nil {
		v := *in.MaxCapacity
		out.MaxCapacity = &v
	}

	return out
}

func copyScaling(s *ServerlessV2Scaling) *ServerlessV2Scaling {
	if s == nil {
		return nil
	}
	out := &ServerlessV2Scaling{}
	if s.MinCapacity != nil {
		v := *s.MinCapacity
		out.MinCapacity = &v
	}
	if s.MaxCapacity != nil {
		v := *s.MaxCapacity
		out.MaxCapacity = &v
	}

	return out
}

// validate checks the extras on their own, before any state is touched.
func (e ClusterExtras) validate() error {
	if err := validateNetworkType(e.NetworkType); err != nil {
		return err
	}

	return validateScaling(e.Scaling)
}

// applyTo merges the extras onto c; callers validate the merged result first.
func (e ClusterExtras) applyTo(c *DBCluster) {
	if e.NetworkType != "" {
		c.NetworkType = e.NetworkType
	}
	c.ServerlessV2Scaling = mergeScaling(c.ServerlessV2Scaling, e.Scaling)
}

type xmlServerlessV2Scaling struct {
	MinCapacity *float64 `xml:"MinCapacity"`
	MaxCapacity *float64 `xml:"MaxCapacity"`
}

func toXMLScaling(s *ServerlessV2Scaling) *xmlServerlessV2Scaling {
	if s == nil {
		return nil
	}
	cp := copyScaling(s)

	return &xmlServerlessV2Scaling{MinCapacity: cp.MinCapacity, MaxCapacity: cp.MaxCapacity}
}
