package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleDissociatePackage(
	w http.ResponseWriter,
	r *http.Request,
	packageID, domainName string,
) {
	details, err := h.Backend.DissociatePackage(packageID, domainName)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, map[string]any{"DomainPackageDetails": domainPackageDetailsJSON{
		PackageID:           details.PackageID,
		DomainName:          details.DomainName,
		DomainPackageStatus: details.State,
	}})
}

func (h *Handler) handleDissociatePackages(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req struct {
		DomainName  string            `json:"DomainName"`
		PackageList []packageForAssoc `json:"PackageList"`
	}

	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	packageIDs := make([]string, 0, len(req.PackageList))

	for _, p := range req.PackageList {
		packageIDs = append(packageIDs, p.PackageID)
	}

	details, dissocErr := h.Backend.DissociatePackages(req.DomainName, packageIDs)
	if dissocErr != nil {
		if errors.Is(dissocErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", dissocErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", dissocErr.Error())
		}

		return
	}

	outList := make([]domainPackageDetailsJSON, 0, len(details))

	for _, d := range details {
		outList = append(outList, domainPackageDetailsJSON{
			PackageID:           d.PackageID,
			DomainName:          d.DomainName,
			DomainPackageStatus: d.State,
		})
	}

	h.writeJSON(r, w, map[string]any{"DomainPackageDetailsList": outList})
}

// handlePackageRoutes handles package routes.
func (h *Handler) handlePackageRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchPackagesPath)

	// Root paths first.
	if rest == "" || rest == "/" {
		h.handlePackageRootRoutes(w, r)

		return
	}

	// Named sub-paths: associate, dissociate.
	if h.handlePackageAssocRoutes(w, r, rest) {
		return
	}

	// Sub-resource paths: history, domains, scope.
	if h.handlePackageSubResourceRoutes(w, r, rest) {
		return
	}

	// Fallback: single-segment package-ID routes.
	h.handlePackageIDRoutes(w, r, rest)
}

// handlePackageAssocRoutes handles associate/dissociate package routes.
// Returns true if the request was handled.
func (h *Handler) handlePackageAssocRoutes(
	w http.ResponseWriter,
	r *http.Request,
	rest string,
) bool {
	switch {
	// POST /packages/associate/{PackageID}/{DomainName} → AssociatePackage
	case strings.HasPrefix(rest, "/associate/") && r.Method == http.MethodPost:
		parts := strings.SplitN(strings.TrimPrefix(rest, "/associate/"), "/", pkgPathParts)
		if len(parts) != pkgPathParts {
			h.writeError(
				r,
				w,
				http.StatusBadRequest,
				"ValidationException",
				"invalid associate package path",
			)

			return true
		}

		h.handleAssociatePackage(w, r, parts[0], parts[1])

		return true
	// POST /packages/associateMultiple → AssociatePackages
	case rest == "/associateMultiple" && r.Method == http.MethodPost:
		h.handleAssociatePackages(w, r)

		return true
	// DELETE /packages/dissociate/{PackageID}/{DomainName} → DissociatePackage
	case strings.HasPrefix(rest, "/dissociate/") && r.Method == http.MethodDelete:
		parts := strings.SplitN(strings.TrimPrefix(rest, "/dissociate/"), "/", pkgPathParts)
		if len(parts) != pkgPathParts {
			h.writeError(
				r,
				w,
				http.StatusBadRequest,
				"ValidationException",
				"invalid dissociate package path",
			)

			return true
		}

		h.handleDissociatePackage(w, r, parts[0], parts[1])

		return true
	// POST /packages/dissociateMultiple → DissociatePackages
	case rest == "/dissociateMultiple" && r.Method == http.MethodPost:
		h.handleDissociatePackages(w, r)

		return true
	}

	return false
}

// handlePackageSubResourceRoutes handles package sub-resource routes (history, domains, scope).
// Returns true if the request was handled.
func (h *Handler) handlePackageSubResourceRoutes(
	w http.ResponseWriter,
	r *http.Request,
	rest string,
) bool {
	switch {
	// GET /packages/{packageId}/history → GetPackageVersionHistory
	case strings.HasSuffix(rest, "/history") && r.Method == http.MethodGet:
		pkgID := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/history")
		history, err := h.Backend.GetPackageVersionHistory(pkgID)
		if err != nil {
			history = []*PackageVersionHistory{}
		}
		h.writeJSON(r, w, map[string]any{"PackageVersionHistoryList": history})

		return true
	// GET /packages/{packageId}/domains → ListDomainsForPackage
	case strings.HasSuffix(rest, "/domains") && r.Method == http.MethodGet:
		pkgID := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/domains")
		domains := h.Backend.ListDomainsForPackage(pkgID)
		h.writeJSON(r, w, map[string]any{jsonKeyPkgDetailsList: domains})

		return true
	// PUT /packages/{packageId}/scope → UpdatePackageScope
	case strings.HasSuffix(rest, "/scope") && r.Method == http.MethodPut:
		pkgID := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/scope")
		body, _ := httputils.ReadBody(r)
		var req struct {
			Operation   string   `json:"Operation"`
			DomainNames []string `json:"PackageScopeOperationConfig"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		pkg, err := h.Backend.UpdatePackageScope(pkgID, req.Operation, req.DomainNames)
		var retPkgID string
		if pkg != nil {
			retPkgID = pkg.PackageID
		}
		_ = err
		h.writeJSON(r, w, map[string]any{
			jsonKeyPackageID:              retPkgID,
			"Operation":                   req.Operation,
			"PackageScopeOperationStatus": softwareUpdateCompleted,
		})

		return true
	}

	return false
}

// handlePackageRootRoutes handles /packages and /packages/ requests.
func (h *Handler) handlePackageRootRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	// POST /packages → CreatePackage
	case http.MethodPost:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			PackageSource            *packageSourceJSON            `json:"PackageSource,omitempty"`
			PackageEncryptionOptions *packageEncryptionOptionsJSON `json:"PackageEncryptionOptions,omitempty"`
			PackageName              string                        `json:"PackageName"`
			PackageType              string                        `json:"PackageType"`
			PackageDescription       string                        `json:"PackageDescription"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		var pkgSource *PackageSource
		if req.PackageSource != nil {
			pkgSource = &PackageSource{
				S3BucketName: req.PackageSource.S3BucketName,
				S3Key:        req.PackageSource.S3Key,
			}
		}
		var pkgEncOpts *PackageEncryptionOptions
		if req.PackageEncryptionOptions != nil {
			pkgEncOpts = &PackageEncryptionOptions{
				KmsKeyIdentifier:  req.PackageEncryptionOptions.KmsKeyIdentifier,
				EncryptionEnabled: req.PackageEncryptionOptions.EncryptionEnabled,
			}
		}
		pkg, createErr := h.Backend.CreatePackage(
			req.PackageName,
			req.PackageType,
			req.PackageDescription,
			pkgSource,
			pkgEncOpts,
		)
		if createErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{jsonKeyPackageDetails: pkg})
	// GET /packages → DescribePackages
	case http.MethodGet:
		var ids []string
		if q := r.URL.Query().Get("PackageID"); q != "" {
			ids = append(ids, q)
		}
		pkgs, _ := h.Backend.DescribePackages(ids)
		if pkgs == nil {
			pkgs = []*Package{}
		}
		h.writeJSON(r, w, map[string]any{"PackageDetailsList": pkgs})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handlePackageIDRoutes handles /packages/{packageId} requests.
func (h *Handler) handlePackageIDRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	pkgID := strings.TrimPrefix(rest, "/")
	if strings.Contains(pkgID, "/") {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	switch r.Method {
	// DELETE /packages/{packageId} → DeletePackage
	case http.MethodDelete:
		pkg, err := h.Backend.DeletePackage(pkgID)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{jsonKeyPackageDetails: pkg})
	// POST /packages/{packageId} → UpdatePackage
	case http.MethodPost:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			PackageDescription string `json:"PackageDescription"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		pkg, updateErr := h.Backend.UpdatePackage(pkgID, req.PackageDescription)
		if updateErr != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{jsonKeyPackageDetails: pkg})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// associatePackageOutput is the JSON response for AssociatePackage.
type associatePackageOutput struct {
	DomainPackageDetails domainPackageDetailsJSON `json:"DomainPackageDetails"`
}

// domainPackageDetailsJSON is the JSON representation of package domain details.
type domainPackageDetailsJSON struct {
	PackageID           string `json:"PackageID"`
	DomainName          string `json:"DomainName"`
	DomainPackageStatus string `json:"DomainPackageStatus"`
}

func (h *Handler) handleAssociatePackage(
	w http.ResponseWriter,
	r *http.Request,
	packageID, domainName string,
) {
	details, err := h.Backend.AssociatePackage(packageID, domainName)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) || errors.Is(err, ErrPackageNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, associatePackageOutput{
		DomainPackageDetails: domainPackageDetailsJSON{
			PackageID:           details.PackageID,
			DomainName:          details.DomainName,
			DomainPackageStatus: details.State,
		},
	})
}

// associatePackagesRequest is the JSON request body for AssociatePackages.
type associatePackagesRequest struct {
	DomainName  string            `json:"DomainName"`
	PackageList []packageForAssoc `json:"PackageList"`
}

// packageForAssoc is a package entry in AssociatePackages request.
type packageForAssoc struct {
	PackageID string `json:"PackageID"`
}

// associatePackagesOutput is the JSON response for AssociatePackages.
type associatePackagesOutput struct {
	DomainPackageDetailsList []domainPackageDetailsJSON `json:"DomainPackageDetailsList"`
}

func (h *Handler) handleAssociatePackages(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req associatePackagesRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	packageIDs := make([]string, 0, len(req.PackageList))
	for _, p := range req.PackageList {
		packageIDs = append(packageIDs, p.PackageID)
	}

	details, assocErr := h.Backend.AssociatePackages(req.DomainName, packageIDs)
	if assocErr != nil {
		if errors.Is(assocErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", assocErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", assocErr.Error())
		}

		return
	}

	outList := make([]domainPackageDetailsJSON, 0, len(details))
	for _, d := range details {
		outList = append(outList, domainPackageDetailsJSON{
			PackageID:           d.PackageID,
			DomainName:          d.DomainName,
			DomainPackageStatus: d.State,
		})
	}

	h.writeJSON(r, w, associatePackagesOutput{DomainPackageDetailsList: outList})
}
