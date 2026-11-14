package ocivault

func replicaOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateReplica: (*Handler).createVaultReplica, opDeleteReplica: (*Handler).deleteVaultReplica,
		opListReplicas: (*Handler).listVaultReplicas, opReplicationStatus: (*Handler).getReplicationStatus,
	}
}

func (h *Handler) createVaultReplica(q *req) error {
	var in CreateReplicaIn
	if err := q.decode(&in); err != nil {
		return err
	}

	return accepted(q, h.Backend.CreateVaultReplica(q.param("vaultId"), q.ifMatch(), q.retry(), in))
}

func (h *Handler) deleteVaultReplica(q *req) error {
	var in DeleteReplicaIn
	if err := q.decode(&in); err != nil {
		return err
	}

	return accepted(q, h.Backend.DeleteVaultReplica(q.param("vaultId"), q.ifMatch(), q.retry(), in))
}

func (h *Handler) listVaultReplicas(q *req) error {
	pg, err := h.Backend.ListVaultReplicas(q.param("vaultId"), q.hostPort(), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) getReplicationStatus(q *req) error {
	w, err := h.Backend.GetReplicationStatus(q.param("replicationId"))

	return reply(q, w, "", err)
}
