package azurefile

import "net/http"

// Operation names (metrics labels and GetSupportedOperations).
const (
	opListShares        = "ListShares"
	opGetServiceProps   = "GetFileServiceProperties"
	opSetServiceProps   = "SetFileServiceProperties"
	opCreateShare       = "CreateShare"
	opGetShareProps     = "GetShareProperties"
	opSetShareProps     = "SetShareProperties"
	opSetShareMetadata  = "SetShareMetadata"
	opDeleteShare       = "DeleteShare"
	opSnapshotShare     = "SnapshotShare"
	opRestoreShare      = "RestoreShare"
	opGetShareACL       = "GetShareACL"
	opSetShareACL       = "SetShareACL"
	opGetShareStats     = "GetShareStats"
	opLeaseShare        = "LeaseShare"
	opCreatePermission  = "CreatePermission"
	opGetPermission     = "GetPermission"
	opCreateDirectory   = "CreateDirectory"
	opDeleteDirectory   = "DeleteDirectory"
	opGetDirProps       = "GetDirectoryProperties"
	opSetDirProps       = "SetDirectoryProperties"
	opSetDirMetadata    = "SetDirectoryMetadata"
	opListFilesDirs     = "ListFilesAndDirectories"
	opRenameDirectory   = "RenameDirectory"
	opListHandles       = "ListHandles"
	opForceCloseHandles = "ForceCloseHandles"
	opCreateFile        = "CreateFile"
	opDeleteFile        = "DeleteFile"
	opGetFile           = "GetFile"
	opGetFileProps      = "GetFileProperties"
	opSetFileProps      = "SetFileProperties"
	opSetFileMetadata   = "SetFileMetadata"
	opPutRange          = "PutRange"
	opPutRangeFromURL   = "PutRangeFromURL"
	opListRanges        = "ListRanges"
	opCopyFile          = "CopyFile"
	opAbortCopyFile     = "AbortCopyFile"
	opRenameFile        = "RenameFile"
	opLeaseFile         = "LeaseFile"
)

// GetSupportedOperations lists the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opListShares, opGetServiceProps, opSetServiceProps, opCreateShare, opGetShareProps, opSetShareProps,
		opSetShareMetadata, opDeleteShare, opSnapshotShare, opRestoreShare, opGetShareACL, opSetShareACL,
		opGetShareStats, opLeaseShare, opCreatePermission, opGetPermission, opCreateDirectory, opDeleteDirectory,
		opGetDirProps, opSetDirProps, opSetDirMetadata, opListFilesDirs, opRenameDirectory, opListHandles,
		opForceCloseHandles, opCreateFile, opDeleteFile, opGetFile, opGetFileProps, opSetFileProps,
		opSetFileMetadata, opPutRange, opPutRangeFromURL, opListRanges, opCopyFile, opAbortCopyFile,
		opRenameFile, opLeaseFile,
	}
}

const (
	compList          = "list"
	compProperties    = "properties"
	compMetadata      = "metadata"
	compACL           = "acl"
	compLease         = "lease"
	compRange         = "range"
	compRename        = "rename"
	compListHandles   = "listhandles"
	compForceClose    = "forceclosehandles"
	compCopy          = "copy"
	compRangeList     = "rangelist"
	compSnapshot      = "snapshot"
	compUndelete      = "undelete"
	compStats         = "stats"
	compFilePerm      = "filepermission"
	restypeShare      = "share"
	restypeDirectory  = "directory"
	restypeService    = "service"
	headerCopySource  = "x-ms-copy-source"
	headerWrite       = "x-ms-write"
	headerLeaseAction = "x-ms-lease-action"
)

// route resolves a request to its operation name and handler.
func (h *Handler) route(rc *reqCtx) (string, opFn) {
	comp, restype := rc.q.Get("comp"), rc.q.Get("restype")

	switch {
	case rc.share == "":
		return h.routeService(rc, comp, restype)
	case restype == restypeShare && rc.path == "":
		return h.routeShare(rc, comp)
	case restype == restypeDirectory:
		return h.routeDir(rc, comp)
	case restype != "":
		return unknownOperation, nil
	}

	return h.routeFile(rc, comp)
}

func (h *Handler) routeService(rc *reqCtx, comp, restype string) (string, opFn) {
	m := rc.r.Method

	switch {
	case m == http.MethodGet && comp == compList:
		return opListShares, h.listShares
	case m == http.MethodGet && restype == restypeService && comp == compProperties:
		return opGetServiceProps, h.getServiceProps
	case m == http.MethodPut && restype == restypeService && comp == compProperties:
		return opSetServiceProps, h.setServiceProps
	}

	return unknownOperation, nil
}

func (h *Handler) routeShare(rc *reqCtx, comp string) (string, opFn) {
	switch rc.r.Method + " " + comp {
	case "PUT ":
		return opCreateShare, h.createShare
	case "GET ", "HEAD ":
		return opGetShareProps, h.getShareProps
	case "DELETE ":
		return opDeleteShare, h.deleteShare
	case "PUT " + compProperties:
		return opSetShareProps, h.setShareProps
	case "PUT " + compMetadata:
		return opSetShareMetadata, h.setShareMetadata
	case "PUT " + compSnapshot:
		return opSnapshotShare, h.snapshotShare
	case "PUT " + compUndelete:
		return opRestoreShare, h.restoreShare
	case "GET " + compACL:
		return opGetShareACL, h.getShareACL
	case "PUT " + compACL:
		return opSetShareACL, h.setShareACL
	case "GET " + compStats:
		return opGetShareStats, h.getShareStats
	case "PUT " + compLease:
		return opLeaseShare, h.leaseShare
	case "PUT " + compFilePerm:
		return opCreatePermission, h.createPermission
	case "GET " + compFilePerm:
		return opGetPermission, h.getPermission
	}

	return unknownOperation, nil
}

func (h *Handler) routeDir(rc *reqCtx, comp string) (string, opFn) {
	switch rc.r.Method + " " + comp {
	case "PUT ":
		return opCreateDirectory, h.createDir
	case "GET ", "HEAD ":
		return opGetDirProps, h.getDirProps
	case "DELETE ":
		return opDeleteDirectory, h.deleteDir
	case "GET " + compList:
		return opListFilesDirs, h.listFilesDirs
	case "PUT " + compProperties:
		return opSetDirProps, h.setDirProps
	case "PUT " + compMetadata:
		return opSetDirMetadata, h.setDirMetadata
	case "PUT " + compRename:
		return opRenameDirectory, h.renameDir
	}

	return unknownOperation, nil
}

func (h *Handler) routeFile(rc *reqCtx, comp string) (string, opFn) {
	switch rc.r.Method + " " + comp {
	case "GET " + compListHandles:
		return opListHandles, h.listHandles
	case "PUT " + compForceClose:
		return opForceCloseHandles, h.forceCloseHandles
	}

	if rc.path == "" {
		return unknownOperation, nil
	}

	if comp == "" {
		return h.routeFilePlain(rc)
	}

	return h.routeFileComp(rc, comp)
}

func (h *Handler) routeFilePlain(rc *reqCtx) (string, opFn) {
	switch rc.r.Method {
	case http.MethodPut:
		if rc.hdr(headerCopySource) != "" {
			return opCopyFile, h.copyFile
		}

		return opCreateFile, h.createFile
	case http.MethodGet:
		return opGetFile, h.getFile
	case http.MethodHead:
		return opGetFileProps, h.getFileProps
	case http.MethodDelete:
		return opDeleteFile, h.deleteFile
	}

	return unknownOperation, nil
}

func (h *Handler) routeFileComp(rc *reqCtx, comp string) (string, opFn) {
	switch rc.r.Method + " " + comp {
	case "PUT " + compProperties:
		return opSetFileProps, h.setFileProps
	case "PUT " + compMetadata:
		return opSetFileMetadata, h.setFileMetadata
	case "PUT " + compRange:
		if rc.hdr(headerCopySource) != "" {
			return opPutRangeFromURL, h.putRangeFromURL
		}

		return opPutRange, h.putRange
	case "GET " + compRangeList:
		return opListRanges, h.listRanges
	case "PUT " + compCopy:
		return opAbortCopyFile, h.abortCopy
	case "PUT " + compRename:
		return opRenameFile, h.renameFile
	case "PUT " + compLease:
		return opLeaseFile, h.leaseFile
	}

	return unknownOperation, nil
}
