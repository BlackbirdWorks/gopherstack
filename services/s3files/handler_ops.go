package s3files

import (
	"context"
	"net/http"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const keyNextToken = "nextToken"

func operationTable() map[string]dispatchFunc {
	return map[string]dispatchFunc{
		opCreateFileSystem:                (*Handler).createFileSystem,
		opGetFileSystem:                   (*Handler).getFileSystem,
		opListFileSystems:                 (*Handler).listFileSystems,
		opDeleteFileSystem:                (*Handler).deleteFileSystem,
		opPutFileSystemPolicy:             (*Handler).putFileSystemPolicy,
		opGetFileSystemPolicy:             (*Handler).getFileSystemPolicy,
		opDeleteFileSystemPolicy:          (*Handler).deleteFileSystemPolicy,
		opPutSynchronizationConfiguration: (*Handler).putSyncConfiguration,
		opGetSynchronizationConfiguration: (*Handler).getSyncConfiguration,
		opCreateMountTarget:               (*Handler).createMountTarget,
		opGetMountTarget:                  (*Handler).getMountTarget,
		opListMountTargets:                (*Handler).listMountTargets,
		opUpdateMountTarget:               (*Handler).updateMountTarget,
		opDeleteMountTarget:               (*Handler).deleteMountTarget,
		opCreateAccessPoint:               (*Handler).createAccessPoint,
		opGetAccessPoint:                  (*Handler).getAccessPoint,
		opListAccessPoints:                (*Handler).listAccessPoints,
		opDeleteAccessPoint:               (*Handler).deleteAccessPoint,
		opTagResource:                     (*Handler).tagResource,
		opUntagResource:                   (*Handler).untagResource,
		opListTagsForResource:             (*Handler).listTagsForResource,
	}
}

func createdStatus(created bool) int {
	if created {
		return http.StatusCreated
	}

	return http.StatusOK
}

func (h *Handler) createFileSystem(
	ctx context.Context, _ *http.Request, _ string, body []byte,
) (int, any, error) {
	var req createFileSystemRequest
	if err := decode(body, &req); err != nil {
		return 0, nil, err
	}

	fs, created, err := h.Backend.CreateFileSystem(ctx, CreateFileSystemInput{
		Bucket: req.Bucket, RoleARN: req.RoleARN, ClientToken: req.ClientToken, KMSKeyID: req.KMSKeyID,
		Prefix: req.Prefix, Tags: tagsFromWire(req.Tags),
	})
	if err != nil {
		return 0, nil, err
	}

	return createdStatus(created), fileSystemToWire(fs), nil
}

func (h *Handler) getFileSystem(ctx context.Context, _ *http.Request, id string, _ []byte) (int, any, error) {
	fs, err := h.Backend.GetFileSystem(ctx, id)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, fileSystemToWire(fs), nil
}

func (h *Handler) listFileSystems(ctx context.Context, r *http.Request, _ string, _ []byte) (int, any, error) {
	q := r.URL.Query()

	maxResults, err := queryInt(q, "maxResults")
	if err != nil {
		return 0, nil, err
	}

	pg, err := h.Backend.ListFileSystems(ctx, q.Get("bucket"), maxResults, q.Get("nextToken"))
	if err != nil {
		return 0, nil, err
	}

	items := make([]fileSystemSummaryWire, 0, len(pg.Data))
	for _, fs := range pg.Data {
		items = append(items, fileSystemSummaryWire{
			Bucket: fs.Bucket, CreationTime: awstime.Epoch(fs.CreatedAt), FileSystemARN: fs.ARN, FileSystemID: fs.ID,
			OwnerID: fs.OwnerID, RoleARN: fs.RoleARN, Status: fs.Status, Name: fs.Tags["Name"],
			StatusMessage: fs.StatusMessage,
		})
	}

	return http.StatusOK, map[string]any{"fileSystems": items, keyNextToken: nilIfEmpty(pg.Next)}, nil
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}

func (h *Handler) deleteFileSystem(ctx context.Context, r *http.Request, id string, _ []byte) (int, any, error) {
	force, _ := strconv.ParseBool(r.URL.Query().Get("forceDelete"))

	return http.StatusNoContent, nil, h.Backend.DeleteFileSystem(ctx, id, force)
}

func (h *Handler) putFileSystemPolicy(ctx context.Context, _ *http.Request, id string, body []byte) (int, any, error) {
	var req putPolicyRequest
	if err := decode(body, &req); err != nil {
		return 0, nil, err
	}

	if err := h.Backend.PutFileSystemPolicy(ctx, id, req.Policy); err != nil {
		return 0, nil, err
	}

	return http.StatusOK, struct{}{}, nil
}

func (h *Handler) getFileSystemPolicy(ctx context.Context, _ *http.Request, id string, _ []byte) (int, any, error) {
	fsID, policy, err := h.Backend.GetFileSystemPolicy(ctx, id)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, map[string]string{"fileSystemId": fsID, "policy": policy}, nil
}

func (h *Handler) deleteFileSystemPolicy(
	ctx context.Context, _ *http.Request, id string, _ []byte,
) (int, any, error) {
	return http.StatusNoContent, nil, h.Backend.DeleteFileSystemPolicy(ctx, id)
}

func (h *Handler) putSyncConfiguration(ctx context.Context, _ *http.Request, id string, body []byte) (int, any, error) {
	var req putSyncRequest
	if err := decode(body, &req); err != nil {
		return 0, nil, err
	}

	err := h.Backend.PutSynchronizationConfiguration(ctx, id, PutSyncInput{
		LatestVersion: req.LatestVersionNumber, ImportRules: req.ImportDataRules, ExpireRules: req.ExpirationDataRules,
	})
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, struct{}{}, nil
}

func (h *Handler) getSyncConfiguration(ctx context.Context, _ *http.Request, id string, _ []byte) (int, any, error) {
	cfg, err := h.Backend.GetSynchronizationConfiguration(ctx, id)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, map[string]any{
		"importDataRules": cfg.ImportDataRules, "expirationDataRules": cfg.ExpirationDataRules,
		"latestVersionNumber": cfg.Version,
	}, nil
}

func (h *Handler) createMountTarget(ctx context.Context, _ *http.Request, _ string, body []byte) (int, any, error) {
	var req createMountTargetRequest
	if err := decode(body, &req); err != nil {
		return 0, nil, err
	}

	mt, err := h.Backend.CreateMountTarget(ctx, CreateMountTargetInput(req))
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, mountTargetToWire(mt, true), nil
}

func (h *Handler) getMountTarget(ctx context.Context, _ *http.Request, id string, _ []byte) (int, any, error) {
	mt, err := h.Backend.GetMountTarget(ctx, id)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, mountTargetToWire(mt, true), nil
}

func (h *Handler) listMountTargets(ctx context.Context, r *http.Request, _ string, _ []byte) (int, any, error) {
	q := r.URL.Query()

	maxResults, err := queryInt(q, "maxResults")
	if err != nil {
		return 0, nil, err
	}

	pg, err := h.Backend.ListMountTargets(
		ctx,
		q.Get("fileSystemId"),
		q.Get("accessPointId"),
		maxResults,
		q.Get("nextToken"),
	)
	if err != nil {
		return 0, nil, err
	}

	items := make([]mountTargetWire, 0, len(pg.Data))
	for _, mt := range pg.Data {
		items = append(items, mountTargetToWire(mt, false))
	}

	return http.StatusOK, map[string]any{"mountTargets": items, keyNextToken: nilIfEmpty(pg.Next)}, nil
}

func (h *Handler) updateMountTarget(ctx context.Context, _ *http.Request, id string, body []byte) (int, any, error) {
	var req updateMountTargetRequest
	if err := decode(body, &req); err != nil {
		return 0, nil, err
	}

	mt, err := h.Backend.UpdateMountTarget(ctx, id, req.SecurityGroups)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, mountTargetToWire(mt, true), nil
}

func (h *Handler) deleteMountTarget(ctx context.Context, _ *http.Request, id string, _ []byte) (int, any, error) {
	return http.StatusNoContent, nil, h.Backend.DeleteMountTarget(ctx, id)
}

func (h *Handler) createAccessPoint(ctx context.Context, _ *http.Request, _ string, body []byte) (int, any, error) {
	var req createAccessPointRequest
	if err := decode(body, &req); err != nil {
		return 0, nil, err
	}

	ap, created, err := h.Backend.CreateAccessPoint(ctx, CreateAccessPointInput{
		FileSystemID: req.FileSystemID, ClientToken: req.ClientToken, PosixUser: req.PosixUser,
		RootDirectory: req.RootDirectory, Tags: tagsFromWire(req.Tags),
	})
	if err != nil {
		return 0, nil, err
	}

	return createdStatus(created), accessPointToWire(ap, true), nil
}

func (h *Handler) getAccessPoint(ctx context.Context, _ *http.Request, id string, _ []byte) (int, any, error) {
	ap, err := h.Backend.GetAccessPoint(ctx, id)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, accessPointToWire(ap, true), nil
}

func (h *Handler) listAccessPoints(ctx context.Context, r *http.Request, _ string, _ []byte) (int, any, error) {
	q := r.URL.Query()

	maxResults, err := queryInt(q, "maxResults")
	if err != nil {
		return 0, nil, err
	}

	pg, err := h.Backend.ListAccessPoints(ctx, q.Get("fileSystemId"), maxResults, q.Get("nextToken"))
	if err != nil {
		return 0, nil, err
	}

	items := make([]accessPointWire, 0, len(pg.Data))
	for _, ap := range pg.Data {
		items = append(items, accessPointToWire(ap, false))
	}

	return http.StatusOK, map[string]any{"accessPoints": items, keyNextToken: nilIfEmpty(pg.Next)}, nil
}

func (h *Handler) deleteAccessPoint(ctx context.Context, _ *http.Request, id string, _ []byte) (int, any, error) {
	return http.StatusNoContent, nil, h.Backend.DeleteAccessPoint(ctx, id)
}

func (h *Handler) tagResource(ctx context.Context, _ *http.Request, id string, body []byte) (int, any, error) {
	var req tagResourceRequest
	if err := decode(body, &req); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, nil, h.Backend.TagResource(ctx, id, tagsFromWire(req.Tags))
}

func (h *Handler) untagResource(ctx context.Context, r *http.Request, id string, _ []byte) (int, any, error) {
	return http.StatusNoContent, nil, h.Backend.UntagResource(ctx, id, r.URL.Query()["tagKeys"])
}

func (h *Handler) listTagsForResource(ctx context.Context, r *http.Request, id string, _ []byte) (int, any, error) {
	q := r.URL.Query()

	maxResults, err := queryInt(q, "MaxResults")
	if err != nil {
		return 0, nil, err
	}

	pg, err := h.Backend.ListTagsForResource(ctx, id, maxResults, q.Get("NextToken"))
	if err != nil {
		return 0, nil, err
	}

	items := make([]wireTag, 0, len(pg.Data))
	for _, kv := range pg.Data {
		items = append(items, wireTag(kv))
	}

	return http.StatusOK, map[string]any{"tags": items, keyNextToken: nilIfEmpty(pg.Next)}, nil
}
