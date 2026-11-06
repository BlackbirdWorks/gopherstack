package connect

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func attachedFileRoutes() []restroute.Route {
	const (
		files   = "/attached-files/{InstanceId}"
		one     = files + "/{FileId}"
		configs = "/attached-files-configurations/{InstanceId}"
	)

	return []restroute.Route{
		{Method: http.MethodPut, Path: files, Op: "StartAttachedFileUpload"},
		{Method: http.MethodPut, Path: files + "/files", Op: "CreateAttachedFile"},
		{Method: http.MethodPost, Path: files, Op: "BatchGetAttachedFileMetadata"},
		{Method: http.MethodPost, Path: one, Op: "CompleteAttachedFileUpload"},
		{Method: http.MethodGet, Path: one, Op: "GetAttachedFile"},
		{Method: http.MethodDelete, Path: one, Op: "DeleteAttachedFile"},
		{Method: http.MethodGet, Path: configs, Op: "ListAttachedFilesConfigurations"},
		{Method: http.MethodGet, Path: configs + "/{AttachmentScope}", Op: "DescribeAttachedFilesConfiguration"},
		{Method: http.MethodPost, Path: configs + "/{AttachmentScope}", Op: "UpdateAttachedFilesConfiguration"},
	}
}
