package ocifilestorage

import (
	"maps"
	"net/http"
	"strconv"
	"strings"
)

type compartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

func (q *req) override() bool { return strings.EqualFold(q.query("isLockOverride"), "true") }

func (q *req) token() string { return q.r.Header.Get("Opc-Retry-Token") }

func (q *req) respond(v any, etag string, err error) error {
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, v, etag)
}

func (q *req) page(items any, next string, err error) error {
	if err != nil {
		return err
	}

	return q.collection(items, next)
}

func (q *req) changeCompartment(fn func(id, ifMatch, compartment string) error, param string) error {
	var in compartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	if err := fn(q.param(param), q.ifMatch(), in.CompartmentID); err != nil {
		return err
	}

	return q.ok()
}

func lockOps[W resource, V any](
	param string, tbl func(*InMemoryBackend) table[W], view func(*InMemoryBackend, *entry[W]) V,
) (opFunc, opFunc) {
	run := func(add bool) opFunc {
		return func(h *Handler, q *req) error {
			var l lock
			if err := q.decode(&l); err != nil {
				return err
			}

			b := h.Backend
			b.mu.Lock("lockChange")
			defer b.mu.Unlock()

			e, aerr := tbl(b).usable(q.param(param))
			if aerr != nil {
				return aerr
			}

			if aerr = checkEtag(e.Etag, q.ifMatch()); aerr != nil {
				return aerr
			}

			if add {
				aerr = b.addLock(e.W.bs(), l)
			} else {
				aerr = removeLock(e.W.bs(), l)
			}

			if aerr != nil {
				return aerr
			}

			e.Etag = newEtag()

			return q.json(http.StatusOK, view(b, e), e.Etag)
		}
	}

	return run(true), run(false)
}

func queryInt(q *req, k string) *int {
	if v, err := strconv.Atoi(q.query(k)); err == nil {
		return &v
	}

	return nil
}

func buildOps() map[string]opFunc {
	ops := map[string]opFunc{}
	addFS, rmFS := lockOps("fileSystemId", func(b *InMemoryBackend) table[*FileSystem] { return b.fss },
		func(b *InMemoryBackend, e *entry[*FileSystem]) FileSystem { return b.fsView(e) })
	addMT, rmMT := lockOps("mountTargetId", func(b *InMemoryBackend) table[*MountTarget] { return b.mts },
		func(b *InMemoryBackend, e *entry[*MountTarget]) MountTarget { return b.mtView(e) })
	addEx, rmEx := lockOps("exportId", func(b *InMemoryBackend) table[*Export] { return b.exps },
		func(b *InMemoryBackend, e *entry[*Export]) Export { return b.exportView(e) })
	addSn, rmSn := lockOps("snapshotId", func(b *InMemoryBackend) table[*Snapshot] { return b.snaps },
		func(b *InMemoryBackend, e *entry[*Snapshot]) Snapshot { return b.snapView(e) })
	addPo, rmPo := lockOps("filesystemSnapshotPolicyId", func(b *InMemoryBackend) table[*Policy] { return b.pols },
		func(b *InMemoryBackend, e *entry[*Policy]) Policy { return b.polView(e) })
	addRe, rmRe := lockOps("replicationId", func(b *InMemoryBackend) table[*Replication] { return b.reps },
		func(b *InMemoryBackend, e *entry[*Replication]) Replication { return b.repView(e) })
	addCo, rmCo := lockOps("outboundConnectorId", func(b *InMemoryBackend) table[*Connector] { return b.conns },
		func(b *InMemoryBackend, e *entry[*Connector]) Connector { return b.connView(e) })

	maps.Copy(ops, map[string]opFunc{
		opAddFileSystemLock:                  addFS,
		opRemoveFileSystemLock:               rmFS,
		opAddMountTargetLock:                 addMT,
		opRemoveMountTargetLock:              rmMT,
		opAddExportLock:                      addEx,
		opRemoveExportLock:                   rmEx,
		opAddSnapshotLock:                    addSn,
		opRemoveSnapshotLock:                 rmSn,
		opAddFilesystemSnapshotPolicyLock:    addPo,
		opRemoveFilesystemSnapshotPolicyLock: rmPo,
		opAddReplicationLock:                 addRe,
		opRemoveReplicationLock:              rmRe,
		opAddOutboundConnectorLock:           addCo,
		opRemoveOutboundConnectorLock:        rmCo,
	})

	for _, group := range []map[string]opFunc{
		fileSystemOps(), quotaOps(), mountTargetOps(), exportOps(), snapshotPolicyOps(), replicationOps(), connectorOps(),
	} {
		maps.Copy(ops, group)
	}

	return ops
}

func fileSystemOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateFileSystem: func(h *Handler, q *req) error {
			var in createFSIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.CreateFileSystem(in, q.token())

			return q.respond(v, etag, err)
		},
		opGetFileSystem: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.GetFileSystem(q.param("fileSystemId"))

			return q.respond(v, etag, err)
		},
		opListFileSystems: func(h *Handler, q *req) error {
			pg, err := h.Backend.ListFileSystems(FSFilter{
				Compartment: q.query(
					"compartmentId",
				),
				AD:             q.query("availabilityDomain"),
				State:          q.query("lifecycleState"),
				DisplayName:    q.query("displayName"),
				ID:             q.query("id"),
				SourceSnapshot: q.query("sourceSnapshotId"),
				Parent:         q.query("parentFileSystemId"),
				Policy:         q.query("filesystemSnapshotPolicyId"),
			}, q.listOpts())

			return q.page(pg.Data, pg.Next, err)
		},
		opUpdateFileSystem: func(h *Handler, q *req) error {
			var in updateFSIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.UpdateFileSystem(q.param("fileSystemId"), q.ifMatch(), q.override(), in)

			return q.respond(v, etag, err)
		},
		opDeleteFileSystem: func(h *Handler, q *req) error {
			err := h.Backend.DeleteFileSystem(q.param("fileSystemId"), q.ifMatch(), q.override(),
				strings.EqualFold(q.query("canDetachChildFileSystem"), "true"))
			if err != nil {
				return err
			}

			return q.noContent()
		},
		opChangeFileSystemCompartment: func(h *Handler, q *req) error {
			return q.changeCompartment(h.Backend.ChangeFileSystemCompartment, "fileSystemId")
		},
		opDetachClone: func(h *Handler, q *req) error {
			if err := h.Backend.DetachClone(q.param("fileSystemId"), q.ifMatch()); err != nil {
				return err
			}

			return q.ok()
		},
		opEstimateReplication: func(h *Handler, q *req) error {
			v, err := h.Backend.EstimateReplication(q.param("fileSystemId"), queryInt(q, "changeRateInMBps"))

			return q.respond(v, "", err)
		},
	}
}

func quotaOps() map[string]opFunc {
	return map[string]opFunc{
		opToggleQuotaRules: func(h *Handler, q *req) error {
			var in toggleQuotaIn
			if err := q.decode(&in); err != nil {
				return err
			}

			if err := h.Backend.ToggleQuotaRules(q.param("fileSystemId"), q.ifMatch(), in); err != nil {
				return err
			}

			return q.ok()
		},
		opCreateQuotaRule: func(h *Handler, q *req) error {
			var in createQuotaIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.CreateQuotaRule(q.param("fileSystemId"), in)

			return q.respond(v, etag, err)
		},
		opGetQuotaRule: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.GetQuotaRule(q.param("fileSystemId"), q.param("quotaRuleId"))

			return q.respond(v, etag, err)
		},
		opListQuotaRules: func(h *Handler, q *req) error {
			pg, err := h.Backend.ListQuotaRules(q.param("fileSystemId"), QuotaFilter{
				Type: q.query("principalType"), PrincipalID: queryInt(q, "principalId"),
				Violators: strings.EqualFold(q.query("areViolatorsOnly"), "true"),
			}, q.listOpts())

			return q.page(pg.Data, pg.Next, err)
		},
		opUpdateQuotaRule: func(h *Handler, q *req) error {
			var in updateQuotaIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.UpdateQuotaRule(q.param("fileSystemId"), q.param("quotaRuleId"), q.ifMatch(), in)

			return q.respond(v, etag, err)
		},
		opDeleteQuotaRule: func(h *Handler, q *req) error {
			if err := h.Backend.DeleteQuotaRule(
				q.param("fileSystemId"),
				q.param("quotaRuleId"),
				q.ifMatch(),
			); err != nil {
				return err
			}

			return q.noContent()
		},
	}
}

func mountTargetOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateMountTarget: func(h *Handler, q *req) error {
			var in createMTIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.CreateMountTarget(in, q.token())

			return q.respond(v, etag, err)
		},
		opGetMountTarget: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.GetMountTarget(q.param("mountTargetId"))

			return q.respond(v, etag, err)
		},
		opListMountTargets: func(h *Handler, q *req) error {
			pg, err := h.Backend.ListMountTargets(q.mtFilter(), q.listOpts())

			return q.page(pg.Data, pg.Next, err)
		},
		opUpdateMountTarget: func(h *Handler, q *req) error {
			var in updateMTIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.UpdateMountTarget(q.param("mountTargetId"), q.ifMatch(), q.override(), in)

			return q.respond(v, etag, err)
		},
		opDeleteMountTarget: func(h *Handler, q *req) error {
			if err := h.Backend.DeleteMountTarget(q.param("mountTargetId"), q.ifMatch(), q.override()); err != nil {
				return err
			}

			return q.noContent()
		},
		opChangeMountTargetCompartment: func(h *Handler, q *req) error {
			return q.changeCompartment(h.Backend.ChangeMountTargetCompartment, "mountTargetId")
		},
		opUpgradeShapeMountTarget: func(h *Handler, q *req) error {
			var in throughputIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.UpgradeShape(q.param("mountTargetId"), q.ifMatch(), in)

			return q.respond(v, etag, err)
		},
		opScheduleDowngradeShapeMountTarget: func(h *Handler, q *req) error {
			var in throughputIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.ScheduleDowngradeShape(q.param("mountTargetId"), q.ifMatch(), in)

			return q.respond(v, etag, err)
		},
		opCancelDowngradeShapeMountTarget: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.CancelDowngradeShape(q.param("mountTargetId"), q.ifMatch())

			return q.respond(v, etag, err)
		},
		opValidateKeyTabs: func(h *Handler, q *req) error {
			var in validateKeyTabsIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, err := h.Backend.ValidateKeyTabs(in)

			return q.respond(v, "", err)
		},
	}
}

func exportOps() map[string]opFunc {
	return map[string]opFunc{
		opGetExportSet: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.GetExportSet(q.param("exportSetId"))

			return q.respond(v, etag, err)
		},
		opListExportSets: func(h *Handler, q *req) error {
			pg, err := h.Backend.ListExportSets(q.mtFilter(), q.listOpts())

			return q.page(pg.Data, pg.Next, err)
		},
		opUpdateExportSet: func(h *Handler, q *req) error {
			var in updateSetIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.UpdateExportSet(q.param("exportSetId"), q.ifMatch(), in)

			return q.respond(v, etag, err)
		},
		opCreateExport: func(h *Handler, q *req) error {
			var in createExportIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.CreateExport(in, q.token())

			return q.respond(v, etag, err)
		},
		opGetExport: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.GetExport(q.param("exportId"))

			return q.respond(v, etag, err)
		},
		opListExports: func(h *Handler, q *req) error {
			pg, err := h.Backend.ListExports(ExportFilter{
				Compartment: q.query(
					"compartmentId",
				),
				ExportSet:  q.query("exportSetId"),
				FileSystem: q.query("fileSystemId"),
				State:      q.query("lifecycleState"),
				ID:         q.query("id"),
			}, q.listOpts())

			return q.page(pg.Data, pg.Next, err)
		},
		opUpdateExport: func(h *Handler, q *req) error {
			var in updateExportIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.UpdateExport(q.param("exportId"), q.ifMatch(), q.override(), in)

			return q.respond(v, etag, err)
		},
		opDeleteExport: func(h *Handler, q *req) error {
			if err := h.Backend.DeleteExport(q.param("exportId"), q.ifMatch(), q.override()); err != nil {
				return err
			}

			return q.noContent()
		},
	}
}

func (q *req) mtFilter() MTFilter {
	return MTFilter{
		Compartment: q.query("compartmentId"), AD: q.query("availabilityDomain"), State: q.query("lifecycleState"),
		DisplayName: q.query("displayName"), ID: q.query("id"), ExportSet: q.query("exportSetId"),
	}
}

func snapshotPolicyOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateSnapshot: func(h *Handler, q *req) error {
			var in createSnapshotIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.CreateSnapshot(in, q.token())

			return q.respond(v, etag, err)
		},
		opGetSnapshot: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.GetSnapshot(q.param("snapshotId"))

			return q.respond(v, etag, err)
		},
		opListSnapshots: func(h *Handler, q *req) error {
			pg, err := h.Backend.ListSnapshots(SnapshotFilter{
				FileSystem: q.query("fileSystemId"), Policy: q.query("filesystemSnapshotPolicyId"),
				Compartment: q.query("compartmentId"), State: q.query("lifecycleState"), ID: q.query("id"),
			}, q.listOpts())

			return q.page(pg.Data, pg.Next, err)
		},
		opUpdateSnapshot: func(h *Handler, q *req) error {
			var in updateSnapshotIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.UpdateSnapshot(q.param("snapshotId"), q.ifMatch(), in)

			return q.respond(v, etag, err)
		},
		opDeleteSnapshot: func(h *Handler, q *req) error {
			if err := h.Backend.DeleteSnapshot(q.param("snapshotId"), q.ifMatch(), q.override()); err != nil {
				return err
			}

			return q.noContent()
		},
		opCreateFilesystemSnapshotPolicy: func(h *Handler, q *req) error {
			var in createPolicyIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.CreatePolicy(in, q.token())

			return q.respond(v, etag, err)
		},
		opGetFilesystemSnapshotPolicy: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.GetPolicy(q.param("filesystemSnapshotPolicyId"))

			return q.respond(v, etag, err)
		},
		opListFilesystemSnapshotPolicies: func(h *Handler, q *req) error {
			pg, err := h.Backend.ListPolicies(q.mtFilter(), q.listOpts())

			return q.page(pg.Data, pg.Next, err)
		},
		opUpdateFilesystemSnapshotPolicy: func(h *Handler, q *req) error {
			var in updatePolicyIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.UpdatePolicy(q.param("filesystemSnapshotPolicyId"), q.ifMatch(), in)

			return q.respond(v, etag, err)
		},
		opDeleteFilesystemSnapshotPolicy: func(h *Handler, q *req) error {
			if err := h.Backend.DeletePolicy(
				q.param("filesystemSnapshotPolicyId"),
				q.ifMatch(),
				q.override(),
			); err != nil {
				return err
			}

			return q.noContent()
		},
		opPauseFilesystemSnapshotPolicy: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.SetPolicyPaused(q.param("filesystemSnapshotPolicyId"), q.ifMatch(), true)

			return q.respond(v, etag, err)
		},
		opUnpauseFilesystemSnapshotPolicy: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.SetPolicyPaused(q.param("filesystemSnapshotPolicyId"), q.ifMatch(), false)

			return q.respond(v, etag, err)
		},
		opChangeFilesystemSnapshotPolicyCompartment: func(h *Handler, q *req) error {
			return q.changeCompartment(h.Backend.ChangePolicyCompartment, "filesystemSnapshotPolicyId")
		},
	}
}

func replicationOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateReplication: func(h *Handler, q *req) error {
			var in createReplicationIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.CreateReplication(in, q.token())

			return q.respond(v, etag, err)
		},
		opGetReplication: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.GetReplication(q.param("replicationId"))

			return q.respond(v, etag, err)
		},
		opListReplications: func(h *Handler, q *req) error {
			pg, err := h.Backend.ListReplications(
				ReplicationFilter{MTFilter: q.mtFilter(), FileSystem: q.query("fileSystemId")},
				q.listOpts(),
			)

			return q.page(pg.Data, pg.Next, err)
		},
		opUpdateReplication: func(h *Handler, q *req) error {
			var in updateReplicationIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.UpdateReplication(q.param("replicationId"), q.ifMatch(), q.override(), in)

			return q.respond(v, etag, err)
		},
		opDeleteReplication: func(h *Handler, q *req) error {
			err := h.Backend.DeleteReplication(
				q.param("replicationId"),
				q.ifMatch(),
				q.query("deleteMode"),
				q.override(),
			)
			if err != nil {
				return err
			}

			return q.noContent()
		},
		opChangeReplicationCompartment: func(h *Handler, q *req) error {
			return q.changeCompartment(h.Backend.ChangeReplicationCompartment, "replicationId")
		},
		opGetReplicationTarget: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.GetReplicationTarget(q.param("replicationTargetId"))

			return q.respond(v, etag, err)
		},
		opListReplicationTargets: func(h *Handler, q *req) error {
			pg, err := h.Backend.ListReplicationTargets(q.mtFilter(), q.listOpts())

			return q.page(pg.Data, pg.Next, err)
		},
		opDeleteReplicationTarget: func(h *Handler, q *req) error {
			if err := h.Backend.DeleteReplicationTarget(q.param("replicationTargetId"), q.ifMatch()); err != nil {
				return err
			}

			return q.noContent()
		},
	}
}

func connectorOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateOutboundConnector: func(h *Handler, q *req) error {
			var in createConnectorIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.CreateConnector(in, q.token())

			return q.respond(v, etag, err)
		},
		opGetOutboundConnector: func(h *Handler, q *req) error {
			v, etag, err := h.Backend.GetConnector(q.param("outboundConnectorId"))

			return q.respond(v, etag, err)
		},
		opListOutboundConnectors: func(h *Handler, q *req) error {
			pg, err := h.Backend.ListConnectors(q.mtFilter(), q.listOpts())

			return q.page(pg.Data, pg.Next, err)
		},
		opUpdateOutboundConnector: func(h *Handler, q *req) error {
			var in updateConnectorIn
			if err := q.decode(&in); err != nil {
				return err
			}

			v, etag, err := h.Backend.UpdateConnector(q.param("outboundConnectorId"), q.ifMatch(), q.override(), in)

			return q.respond(v, etag, err)
		},
		opDeleteOutboundConnector: func(h *Handler, q *req) error {
			if err := h.Backend.DeleteConnector(q.param("outboundConnectorId"), q.ifMatch(), q.override()); err != nil {
				return err
			}

			return q.noContent()
		},
		opChangeOutboundConnectorCompartment: func(h *Handler, q *req) error {
			return q.changeCompartment(h.Backend.ChangeConnectorCompartment, "outboundConnectorId")
		},
	}
}
