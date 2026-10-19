package gcpiam

import (
	"slices"
	"strings"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// resourceInfo is what the Query* RPCs need to know about a full resource name.
type resourceInfo struct {
	parents        []string
	serviceAccount bool
}

// parseFullResourceName accepts "//{service}.googleapis.com/{path}" and extracts the project/organization parents.
func parseFullResourceName(name string) (resourceInfo, error) {
	rest, ok := strings.CutPrefix(name, "//")
	host, path, found := strings.Cut(rest, "/")

	if !ok || !found || !strings.HasSuffix(host, ".googleapis.com") || path == "" {
		return resourceInfo{}, errInvalid(
			"Invalid full_resource_name %q; expected //SERVICE.googleapis.com/PATH.",
			name,
		)
	}

	parts := strings.Split(path, "/")
	info := resourceInfo{serviceAccount: slices.Contains(parts, "serviceAccounts")}

	for i := 0; i+1 < len(parts); i += 2 {
		if (parts[i] == rootProjects || parts[i] == rootOrgs) && parts[i+1] != "" {
			info.parents = append(info.parents, parts[i]+"/"+parts[i+1])
		}
	}

	return info, nil
}

func (r resourceInfo) grantable(name string) bool {
	if !r.serviceAccount {
		return true
	}

	switch name {
	case roleOwner, roleEditor, roleViewer:
		return true
	}

	return strings.HasPrefix(name, "roles/iam.serviceAccount")
}

// QueryGrantableRoles lists the roles grantable on a resource.
func (b *InMemoryBackend) QueryGrantableRoles(
	req *adminpb.QueryGrantableRolesRequest,
) ([]*adminpb.Role, string, error) {
	info, err := parseFullResourceName(req.GetFullResourceName())
	if err != nil {
		return nil, "", err
	}

	size, err := pageSizeOf(req.GetPageSize(), maxBigPage)
	if err != nil {
		return nil, "", err
	}

	if err = page.ValidateToken(req.GetPageToken()); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	var all []*adminpb.Role

	for _, d := range catalogDefs() {
		if info.grantable(d.name) {
			all = append(all, viewRole(d.role(true), req.GetView()))
		}
	}

	for _, parent := range info.parents {
		all = append(all, b.customRoles(parent, false, req.GetView())...)
	}

	slices.SortFunc(all, func(x, y *adminpb.Role) int { return strings.Compare(x.GetName(), y.GetName()) })

	pg := page.New(all, req.GetPageToken(), size, defaultRolePage)

	return pg.Data, pg.Next, nil
}

// QueryTestablePermissions lists the permissions that can be tested on a resource.
func (b *InMemoryBackend) QueryTestablePermissions(
	req *adminpb.QueryTestablePermissionsRequest,
) ([]*adminpb.Permission, string, error) {
	info, err := parseFullResourceName(req.GetFullResourceName())
	if err != nil {
		return nil, "", err
	}

	size, err := pageSizeOf(req.GetPageSize(), maxBigPage)
	if err != nil {
		return nil, "", err
	}

	if err = page.ValidateToken(req.GetPageToken()); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	var all []*adminpb.Permission

	for _, p := range allPermissions() {
		if info.serviceAccount && !strings.HasPrefix(p, "iam.serviceAccounts.") {
			continue
		}

		all = append(all, &adminpb.Permission{
			Name:                    p,
			Stage:                   adminpb.Permission_GA,
			CustomRolesSupportLevel: adminpb.Permission_SUPPORTED,
		})
	}

	pg := page.New(all, req.GetPageToken(), size, defaultPermPage)

	return pg.Data, pg.Next, nil
}

// QueryAuditableServices lists services that write audit logs for a resource.
func (b *InMemoryBackend) QueryAuditableServices(
	req *adminpb.QueryAuditableServicesRequest,
) (*adminpb.QueryAuditableServicesResponse, error) {
	if _, err := parseFullResourceName(req.GetFullResourceName()); err != nil {
		return nil, err
	}

	out := &adminpb.QueryAuditableServicesResponse{}
	for _, s := range auditableServices() {
		out.Services = append(out.Services, &adminpb.QueryAuditableServicesResponse_AuditableService{Name: s})
	}

	return out, nil
}
