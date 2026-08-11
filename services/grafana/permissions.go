package grafana

import "context"

// ListPermissions returns the permission grants for a workspace, optionally
// filtered by groupID, userID, and/or userType -- mirroring ListPermissionsInput's
// groupId/userId/userType query parameters.
func (b *InMemoryBackend) ListPermissions(workspaceID, groupID, userID, userType string) ([]*Permission, error) {
	b.mu.RLock("ListPermissions")
	defer b.mu.RUnlock()

	if _, ok := b.workspaces.Get(workspaceID); !ok {
		return nil, notFoundError(resourceTypeWorkspace, workspaceID)
	}

	all := b.permissionsByWorkspace.Get(workspaceID)
	out := make([]*Permission, 0, len(all))

	for _, p := range all {
		if groupID != "" && (p.UserType != UserTypeSSOGroup || p.UserID != groupID) {
			continue
		}

		if userID != "" && (p.UserType != UserTypeSSOUser || p.UserID != userID) {
			continue
		}

		if userType != "" && p.UserType != userType {
			continue
		}

		cp := *p
		out = append(out, &cp)
	}

	return out, nil
}

// applyUpdateInstruction applies one UpdateInstruction (ADD or REVOKE) to
// workspaceID's permission grants. Callers must hold b.mu. Returns an error
// (never nil) when the instruction is malformed, in which case the caller
// records it in UpdatePermissionsOutput.Errors rather than applying it --
// this is the batch's partial-failure surface.
func (b *InMemoryBackend) applyUpdateInstruction(
	ctx context.Context, workspaceID string, instr *updateInstructionWire,
) error {
	if len(instr.Users) == 0 {
		return validationError("update instruction specifies no users")
	}

	if instr.Action == UpdateActionAdd {
		for _, u := range instr.Users {
			if err := b.validatePermissionUser(ctx, u); err != nil {
				return err
			}
		}
	}

	for _, u := range instr.Users {
		key := permissionKeyFn(&Permission{WorkspaceID: workspaceID, UserType: u.Type, UserID: u.ID})

		switch instr.Action {
		case UpdateActionAdd:
			b.permissions.Put(&Permission{
				WorkspaceID: workspaceID,
				UserID:      u.ID,
				UserType:    u.Type,
				Role:        instr.Role,
			})
		case UpdateActionRevoke:
			b.permissions.Delete(key)
		}
	}

	return nil
}

// UpdatePermissions applies a batch of ADD/REVOKE instructions to a
// workspace's permission grants. This is explicitly a partial-failure
// batch op (matching UpdatePermissionsOutput.Errors's doc comment): a
// malformed instruction is recorded in the returned error slice rather
// than aborting the whole batch.
func (b *InMemoryBackend) UpdatePermissions(
	ctx context.Context, workspaceID string, instructions []updateInstructionWire,
) ([]updateErrorWire, error) {
	b.mu.Lock("UpdatePermissions")
	defer b.mu.Unlock()

	if _, ok := b.workspaces.Get(workspaceID); !ok {
		return nil, notFoundError(resourceTypeWorkspace, workspaceID)
	}

	const validationErrorCode = 400

	var errs []updateErrorWire

	for i := range instructions {
		instr := instructions[i]
		if err := b.applyUpdateInstruction(ctx, workspaceID, &instr); err != nil {
			errs = append(errs, updateErrorWire{
				CausedBy: &instr,
				Code:     validationErrorCode,
				Message:  err.Error(),
			})
		}
	}

	return errs, nil
}
