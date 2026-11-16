package azureblob

import (
	"encoding/xml"
	"net/http"
)

const (
	maxCorsRules       = 5
	maxRetentionDays   = 365
	maxServicePropsXML = 64 << 10
	metricsVersion     = "1.0"
)

// RetentionPolicy is a retention element of the Blob service properties.
type RetentionPolicy struct {
	Days                 *int  `xml:"Days,omitempty"`
	AllowPermanentDelete *bool `xml:"AllowPermanentDelete,omitempty"`
	Enabled              bool  `xml:"Enabled"`
}

// LoggingProps is the Logging element.
type LoggingProps struct {
	RetentionPolicy *RetentionPolicy `xml:"RetentionPolicy,omitempty"`
	Version         string           `xml:"Version"`
	Delete          bool             `xml:"Delete"`
	Read            bool             `xml:"Read"`
	Write           bool             `xml:"Write"`
}

// MetricsProps is the HourMetrics / MinuteMetrics element.
type MetricsProps struct {
	IncludeAPIs     *bool            `xml:"IncludeAPIs,omitempty"`
	RetentionPolicy *RetentionPolicy `xml:"RetentionPolicy,omitempty"`
	Version         string           `xml:"Version"`
	Enabled         bool             `xml:"Enabled"`
}

// CorsRule is one CORS rule.
type CorsRule struct {
	AllowedOrigins  string `xml:"AllowedOrigins"`
	AllowedMethods  string `xml:"AllowedMethods"`
	ExposedHeaders  string `xml:"ExposedHeaders"`
	AllowedHeaders  string `xml:"AllowedHeaders"`
	MaxAgeInSeconds int    `xml:"MaxAgeInSeconds"`
}

// CorsProps is the Cors element.
type CorsProps struct {
	Rules []CorsRule `xml:"CorsRule"`
}

// StaticWebsiteProps is the StaticWebsite element.
type StaticWebsiteProps struct {
	IndexDocument           string `xml:"IndexDocument,omitempty"`
	ErrorDocument404Path    string `xml:"ErrorDocument404Path,omitempty"`
	DefaultIndexDocumentPth string `xml:"DefaultIndexDocumentPath,omitempty"`
	Enabled                 bool   `xml:"Enabled"`
}

// ServiceProperties is the StorageServiceProperties document of Get/Set Blob Service Properties.
type ServiceProperties struct {
	Logging               *LoggingProps       `xml:"Logging,omitempty"`
	HourMetrics           *MetricsProps       `xml:"HourMetrics,omitempty"`
	MinuteMetrics         *MetricsProps       `xml:"MinuteMetrics,omitempty"`
	Cors                  *CorsProps          `xml:"Cors,omitempty"`
	DeleteRetentionPolicy *RetentionPolicy    `xml:"DeleteRetentionPolicy,omitempty"`
	StaticWebsite         *StaticWebsiteProps `xml:"StaticWebsite,omitempty"`
	XMLName               xml.Name            `xml:"StorageServiceProperties"        json:"-"`
	DefaultServiceVersion string              `xml:"DefaultServiceVersion,omitempty"`
}

func defaultServiceProperties() ServiceProperties {
	t, days := true, 7

	return ServiceProperties{
		Logging: &LoggingProps{Version: metricsVersion, RetentionPolicy: &RetentionPolicy{}},
		HourMetrics: &MetricsProps{
			Version:         metricsVersion,
			Enabled:         true,
			IncludeAPIs:     &t,
			RetentionPolicy: &RetentionPolicy{Enabled: true, Days: &days},
		},
		MinuteMetrics:         &MetricsProps{Version: metricsVersion, RetentionPolicy: &RetentionPolicy{}},
		Cors:                  &CorsProps{},
		DeleteRetentionPolicy: &RetentionPolicy{},
		StaticWebsite:         &StaticWebsiteProps{},
	}
}

func (sp ServiceProperties) clone() ServiceProperties {
	out := sp

	if sp.Logging != nil {
		l := *sp.Logging
		l.RetentionPolicy = cloneRetention(l.RetentionPolicy)
		out.Logging = &l
	}

	out.HourMetrics = cloneMetrics(sp.HourMetrics)
	out.MinuteMetrics = cloneMetrics(sp.MinuteMetrics)

	if sp.Cors != nil {
		out.Cors = &CorsProps{Rules: append([]CorsRule(nil), sp.Cors.Rules...)}
	}

	out.DeleteRetentionPolicy = cloneRetention(sp.DeleteRetentionPolicy)

	if sp.StaticWebsite != nil {
		w := *sp.StaticWebsite
		out.StaticWebsite = &w
	}

	return out
}

func cloneRetention(r *RetentionPolicy) *RetentionPolicy {
	if r == nil {
		return nil
	}

	c := *r
	if r.Days != nil {
		d := *r.Days
		c.Days = &d
	}

	if r.AllowPermanentDelete != nil {
		a := *r.AllowPermanentDelete
		c.AllowPermanentDelete = &a
	}

	return &c
}

func cloneMetrics(m *MetricsProps) *MetricsProps {
	if m == nil {
		return nil
	}

	c := *m
	c.RetentionPolicy = cloneRetention(m.RetentionPolicy)

	if m.IncludeAPIs != nil {
		v := *m.IncludeAPIs
		c.IncludeAPIs = &v
	}

	return &c
}

func errInvalidXML(msg string) *StorageError {
	return storageErr(http.StatusBadRequest, "InvalidXmlDocument", msg)
}

func validRetention(r *RetentionPolicy) bool {
	if r == nil {
		return true
	}

	if !r.Enabled {
		return r.Days == nil
	}

	return r.Days != nil && *r.Days >= 1 && *r.Days <= maxRetentionDays
}

func validServiceProperties(sp ServiceProperties) *StorageError {
	if sp.Cors != nil && len(sp.Cors.Rules) > maxCorsRules {
		return errInvalidXML("XML specified is not syntactically valid (more than 5 CORS rules).")
	}

	if sp.Cors != nil {
		for _, r := range sp.Cors.Rules {
			if r.AllowedOrigins == "" || r.AllowedMethods == "" || r.MaxAgeInSeconds < 0 {
				return errInvalidXML("XML specified is not syntactically valid (invalid CORS rule).")
			}
		}
	}

	for _, r := range []*RetentionPolicy{sp.DeleteRetentionPolicy} {
		if !validRetention(r) {
			return errInvalidXML("XML specified is not syntactically valid (invalid retention days).")
		}
	}

	if sp.Logging != nil && !validRetention(sp.Logging.RetentionPolicy) {
		return errInvalidXML("XML specified is not syntactically valid (invalid logging retention).")
	}

	for _, m := range []*MetricsProps{sp.HourMetrics, sp.MinuteMetrics} {
		if m != nil && !validRetention(m.RetentionPolicy) {
			return errInvalidXML("XML specified is not syntactically valid (invalid metrics retention).")
		}
	}

	return nil
}

// merge applies the elements present in in over sp; omitted elements stay unchanged.
func (sp ServiceProperties) merge(in ServiceProperties) ServiceProperties {
	out := sp.clone()
	in = in.clone()

	if in.Logging != nil {
		out.Logging = in.Logging
	}

	if in.HourMetrics != nil {
		out.HourMetrics = in.HourMetrics
	}

	if in.MinuteMetrics != nil {
		out.MinuteMetrics = in.MinuteMetrics
	}

	if in.Cors != nil {
		out.Cors = in.Cors
	}

	if in.DefaultServiceVersion != "" {
		out.DefaultServiceVersion = in.DefaultServiceVersion
	}

	if in.DeleteRetentionPolicy != nil {
		out.DeleteRetentionPolicy = in.DeleteRetentionPolicy
	}

	if in.StaticWebsite != nil {
		out.StaticWebsite = in.StaticWebsite
	}

	return out
}

// GetServiceProperties returns the account's Blob service properties.
func (b *InMemoryBackend) GetServiceProperties() ServiceProperties {
	b.mu.RLock("GetServiceProperties")
	defer b.mu.RUnlock()

	return b.serviceLocked().clone()
}

func (b *InMemoryBackend) serviceLocked() ServiceProperties {
	if b.service == nil {
		return defaultServiceProperties()
	}

	return *b.service
}

// SetServiceProperties merges the supplied elements into the stored properties.
func (b *InMemoryBackend) SetServiceProperties(sp ServiceProperties) error {
	if err := validServiceProperties(sp); err != nil {
		return err
	}

	b.mu.Lock("SetServiceProperties")
	defer b.mu.Unlock()

	merged := b.serviceLocked().merge(sp)
	b.service = &merged

	return nil
}
