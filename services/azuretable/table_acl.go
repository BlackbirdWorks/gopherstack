package azuretable

import (
	"encoding/xml"
	"errors"
	"net/http"
	"regexp"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	minTableNameLen = 3
	maxTableNameLen = 63

	headerNextPartitionKey      = "X-Ms-Continuation-Nextpartitionkey"
	headerNextRowKey            = "X-Ms-Continuation-Nextrowkey"
	headerNextTableName         = "X-Ms-Continuation-Nexttablename"
	headerNextPartitionKeyParam = "NextPartitionKey"
	headerNextRowKeyParam       = "NextRowKey"
	headerNextTableNameParam    = "NextTableName"
)

var tableNameRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`)

// validTableName reports whether name is 3-63 alphanumerics not starting with a digit; "tables" is reserved.
func validTableName(name string) bool {
	if len(name) < minTableNameLen || len(name) > maxTableNameLen || !tableNameRe.MatchString(name) {
		return false
	}

	return !strs.Equal(name, tablesResourceName)
}

type signedIdentifiersXML struct {
	XMLName     xml.Name              `xml:"SignedIdentifiers"`
	Identifiers []signedIdentifierXML `xml:"SignedIdentifier"`
}

type signedIdentifierXML struct {
	ID           string          `xml:"Id"`
	AccessPolicy accessPolicyXML `xml:"AccessPolicy"`
}

type accessPolicyXML struct {
	Start      string `xml:"Start,omitempty"`
	Expiry     string `xml:"Expiry,omitempty"`
	Permission string `xml:"Permission,omitempty"`
}

// setTableACL serves PUT /<account>/<table>?comp=acl (Set Table ACL).
func (h *Handler) setTableACL(c *echo.Context, table string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var req signedIdentifiersXML
	if len(body) > 0 {
		if unmarshalErr := xml.Unmarshal(body, &req); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidXmlDocument",
				"XML specified is not syntactically valid.")
		}
	}

	if len(req.Identifiers) > odatatable.MaxSignedIdentifiers {
		return h.writeError(c, http.StatusBadRequest, "InvalidXmlDocument",
			"A maximum of 5 access policies may be set on a table.")
	}

	ids := make([]odatatable.SignedIdentifier, 0, len(req.Identifiers))
	for _, si := range req.Identifiers {
		ids = append(ids, odatatable.SignedIdentifier{
			ID: si.ID, AccessPolicy: odatatable.AccessPolicy(si.AccessPolicy),
		})
	}

	if err = h.Backend.SetTableACL(table, ids); err != nil {
		return h.writeTableACLError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// getTableACL serves GET /<account>/<table>?comp=acl (Get Table ACL).
func (h *Handler) getTableACL(c *echo.Context, table string) error {
	ids, err := h.Backend.GetTableACL(table)
	if err != nil {
		return h.writeTableACLError(c, err)
	}

	resp := signedIdentifiersXML{Identifiers: make([]signedIdentifierXML, 0, len(ids))}
	for _, id := range ids {
		resp.Identifiers = append(resp.Identifiers, signedIdentifierXML{
			ID: id.ID, AccessPolicy: accessPolicyXML(id.AccessPolicy),
		})
	}

	return h.writeXML(c, http.StatusOK, resp)
}

func (h *Handler) writeTableACLError(c *echo.Context, err error) error {
	if errors.Is(err, ErrTableNotFound) {
		return h.writeTableNotFoundError(c)
	}

	return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
}
