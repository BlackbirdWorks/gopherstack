package elasticsearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// createPackageRequest is the JSON body for CreatePackage.
type createPackageRequest struct {
	PackageName        string `json:"PackageName"`
	PackageType        string `json:"PackageType"`
	PackageDescription string `json:"PackageDescription"`
}

// packageJSON is the JSON representation of an Elasticsearch package.
type packageJSON struct {
	PackageID          string `json:"PackageID"`
	PackageName        string `json:"PackageName"`
	PackageType        string `json:"PackageType"`
	PackageDescription string `json:"PackageDescription"`
	PackageStatus      string `json:"PackageStatus"`
}

// createPackageOutput is the response for CreatePackage.
type createPackageOutput struct {
	PackageDetails packageJSON `json:"PackageDetails"`
}

func (h *Handler) handleCreatePackage(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createPackageRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	pkg, createErr := h.Backend.CreatePackage(h.reqContext(r), req.PackageName, req.PackageType, req.PackageDescription)
	if createErr != nil {
		if errors.Is(createErr, ErrDomainAlreadyExists) {
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", createErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())
		}

		return
	}

	h.writeJSON(r, w, createPackageOutput{PackageDetails: toPackageJSON(pkg)})
}

func toPackageJSON(p *Package) packageJSON {
	return packageJSON{
		PackageID:          p.ID,
		PackageName:        p.Name,
		PackageType:        p.PackageType,
		PackageDescription: p.Description,
		PackageStatus:      p.Status,
	}
}

// associatePackageOutput is the response for AssociatePackage.
type associatePackageOutput struct {
	DomainPackageDetails domainPackageJSON `json:"DomainPackageDetails"`
}

type domainPackageJSON struct {
	PackageID           string `json:"PackageID"`
	PackageName         string `json:"PackageName,omitempty"`
	DomainName          string `json:"DomainName"`
	PackageType         string `json:"PackageType,omitempty"`
	DomainPackageStatus string `json:"DomainPackageStatus"`
}

// associatePackagePathParts is the expected number of path segments after /associate/.
const associatePackagePathParts = 2

func (h *Handler) handleAssociatePackage(w http.ResponseWriter, r *http.Request) {
	// Path: /2015-01-01/packages/associate/{packageID}/{domainName}
	rest := strings.TrimPrefix(r.URL.Path, elasticsearchPackages+"/associate/")
	parts := strings.SplitN(rest, "/", associatePackagePathParts)

	if len(parts) != associatePackagePathParts {
		h.writeError(
			r,
			w,
			http.StatusBadRequest,
			"ValidationException",
			"invalid path: expected /associate/{packageID}/{domainName}",
		)

		return
	}

	packageID, domainName := parts[0], parts[1]

	if assocErr := h.Backend.AssociatePackage(h.reqContext(r), packageID, domainName); assocErr != nil {
		switch {
		case errors.Is(assocErr, ErrDomainNotFound) || errors.Is(assocErr, ErrPackageNotFound):
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", assocErr.Error())
		case errors.Is(assocErr, ErrPackageAlreadyAssociated):
			h.writeError(r, w, http.StatusConflict, "ConflictException", assocErr.Error())
		default:
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", assocErr.Error())
		}

		return
	}

	var out associatePackageOutput
	out.DomainPackageDetails.PackageID = packageID
	out.DomainPackageDetails.DomainName = domainName
	out.DomainPackageDetails.DomainPackageStatus = "ACTIVE"

	h.writeJSON(r, w, &out)
}

func (h *Handler) handleDissociatePackage(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, elasticsearchPackages+"/dissociate/")
	parts := strings.SplitN(rest, "/", associatePackagePathParts)
	if len(parts) != associatePackagePathParts {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid package dissociation path")

		return
	}

	if err := h.Backend.DissociatePackage(h.reqContext(r), parts[0], parts[1]); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"DomainPackageDetails": map[string]any{
		"PackageID":           parts[0],
		"DomainName":          parts[1],
		"DomainPackageStatus": "DISSOCIATED",
	}})
}

func (h *Handler) handleDescribePackages(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PackageIDs []string `json:"PackageIDs"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	packages := h.Backend.DescribePackages(h.reqContext(r), req.PackageIDs)
	result := make([]packageJSON, 0, len(packages))
	for _, pkg := range packages {
		result = append(result, toPackageJSON(pkg))
	}

	h.writeJSON(r, w, map[string]any{"PackageDetailsList": result})
}

func (h *Handler) handleUpdatePackage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PackageID          string `json:"PackageID"`
		PackageDescription string `json:"PackageDescription"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	pkg, err := h.Backend.UpdatePackage(h.reqContext(r), req.PackageID, req.PackageDescription)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"PackageDetails": toPackageJSON(pkg)})
}

func (h *Handler) handleDeletePackage(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, elasticsearchPackages+"/")
	pkg, err := h.Backend.DeletePackage(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"PackageDetails": toPackageJSON(pkg)})
}

func (h *Handler) handleGetPackageVersionHistory(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path, elasticsearchPackages+"/", "/history")
	packages, err := h.Backend.GetPackageVersionHistory(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	history := make([]packageJSON, 0, len(packages))
	for _, pkg := range packages {
		history = append(history, toPackageJSON(pkg))
	}

	h.writeJSON(r, w, map[string]any{"PackageVersionHistoryList": history})
}

func (h *Handler) handleListDomainsForPackage(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path, elasticsearchPackages+"/", "/domains")
	domains, err := h.Backend.ListDomainsForPackage(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	result := make([]domainPackageJSON, 0, len(domains))
	for _, domainName := range domains {
		result = append(result, domainPackageJSON{
			PackageID: id, DomainName: domainName, DomainPackageStatus: statusActive,
		})
	}

	h.writeJSON(r, w, map[string]any{"DomainPackageDetailsList": result})
}

func (h *Handler) handleListPackagesForDomain(w http.ResponseWriter, r *http.Request) {
	domainName := pathID(r.URL.Path, elasticsearchDomainPackages+"/", "/packages")
	packages := h.Backend.ListPackagesForDomain(h.reqContext(r), domainName)
	result := make([]domainPackageJSON, 0, len(packages))
	for _, pkg := range packages {
		result = append(result, domainPackageJSON{
			PackageID:           pkg.ID,
			PackageName:         pkg.Name,
			PackageType:         pkg.PackageType,
			DomainName:          domainName,
			DomainPackageStatus: statusActive,
		})
	}

	h.writeJSON(r, w, map[string]any{"DomainPackageDetailsList": result})
}
