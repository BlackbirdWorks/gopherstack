package redshift

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// parseDurationSecondsParam reads the shared GetClusterCredentials(WithIAM)
// DurationSeconds request parameter. ok is false when the caller omitted it
// (the backend then applies its own default).
func parseDurationSecondsParam(vals url.Values) (int, bool, error) {
	v := vals.Get("DurationSeconds")
	if v == "" {
		return 0, false, nil
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false, fmt.Errorf("%w: DurationSeconds must be an integer", ErrInvalidParameter)
	}

	return n, true, nil
}

// ---- GetClusterCredentials ----

type xmlClusterCredentials struct {
	DBUser     string `xml:"DbUser"`
	DBPassword string `xml:"DbPassword"`
	Expiration string `xml:"Expiration,omitempty"`
}

type getClusterCredentialsResponse struct {
	XMLName xml.Name              `xml:"GetClusterCredentialsResponse"`
	Xmlns   string                `xml:"xmlns,attr"`
	Result  xmlClusterCredentials `xml:"GetClusterCredentialsResult"`
}

func (h *Handler) handleGetClusterCredentials(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	dbUser := vals.Get("DbUser")
	autoCreate := vals.Get("AutoCreate") == paramValueTrue

	duration, hasDuration, err := parseDurationSecondsParam(vals)
	if err != nil {
		return nil, err
	}

	var durationPtr *int
	if hasDuration {
		durationPtr = &duration
	}

	creds, err := h.Backend.GetClusterCredentials(clusterID, dbUser, autoCreate, durationPtr)
	if err != nil {
		return nil, err
	}

	return &getClusterCredentialsResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlClusterCredentials{
			DBUser:     creds.DBUser,
			DBPassword: creds.DBPassword,
			Expiration: creds.Expiration.UTC().Format(time.RFC3339),
		},
	}, nil
}

// ---- GetClusterCredentialsWithIAM ----

type getClusterCredentialsWithIAMResponse struct {
	XMLName    xml.Name `xml:"GetClusterCredentialsWithIAMResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	DBUser     string   `xml:"GetClusterCredentialsWithIAMResult>DbUser"`
	DBPassword string   `xml:"GetClusterCredentialsWithIAMResult>DbPassword"`
	Expiration string   `xml:"GetClusterCredentialsWithIAMResult>Expiration"`
}

func (h *Handler) handleGetClusterCredentialsWithIAM(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	dbName := vals.Get("DbName")

	duration, hasDuration, err := parseDurationSecondsParam(vals)
	if err != nil {
		return nil, err
	}

	var durationPtr *int
	if hasDuration {
		durationPtr = &duration
	}

	creds, err := h.Backend.GetClusterCredentialsWithIAM(clusterID, dbName, durationPtr)
	if err != nil {
		return nil, err
	}

	return &getClusterCredentialsWithIAMResponse{
		Xmlns:      redshiftXMLNS,
		DBUser:     creds.DBUser,
		DBPassword: creds.DBPassword,
		Expiration: creds.Expiration.UTC().Format("2006-01-02T15:04:05Z"),
	}, nil
}
