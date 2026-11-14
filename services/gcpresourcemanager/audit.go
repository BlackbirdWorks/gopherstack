package gcpresourcemanager

import (
	"google.golang.org/protobuf/proto"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpaudit"
)

func auditPolicy() gcpaudit.Policy {
	return gcpaudit.Policy{
		ServiceName: "cloudresourcemanager.googleapis.com",
		AdminPrefixes: []string{
			"Create", "Update", "Delete", "Undelete", "Move", "SetIamPolicy",
		},
		NameFunc: func(req, _ proto.Message) string {
			if r, ok := req.(*resourcemanagerpb.CreateProjectRequest); ok && r.GetProject().GetProjectId() != "" {
				return "projects/" + r.GetProject().GetProjectId()
			}

			return ""
		},
	}
}
