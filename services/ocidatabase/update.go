package ocidatabase

import (
	"slices"
)

// change records what an update touches, which decides the transient state.
type change struct {
	password string
	scale    bool
	other    bool
}

// UpdateDatabase applies an UpdateAutonomousDatabase request.
func (b *InMemoryBackend) UpdateDatabase(id, ifMatch string, in updateIn) (adbWire, string, string, error) {
	b.mu.Lock("UpdateDatabase")
	defer b.mu.Unlock()

	db, aerr := b.activeDB(id)
	if aerr != nil {
		return adbWire{}, "", "", aerr
	}

	if aerr = checkEtag(db.Etag, ifMatch); aerr != nil {
		return adbWire{}, "", "", aerr
	}

	if aerr = b.updatable(db, in); aerr != nil {
		return adbWire{}, "", "", aerr
	}

	next := *db

	ch, aerr := b.applyUpdate(&next, in)
	if aerr != nil {
		return adbWire{}, "", "", aerr
	}

	if db.State == stateStopped {
		return b.updateStopped(db, &next, ch)
	}

	*db = next
	b.applyAccess(db)

	return b.finishUpdate(db, ch)
}

func (b *InMemoryBackend) updatable(db *ADB, in updateIn) *APIError {
	switch db.State {
	case stateAvailable:
		return nil
	case stateStopped:
		return b.stoppedUpdate(db, in)
	}

	return errState("The Autonomous Database " + db.ID + " is in state " + db.State + " and cannot be updated")
}

// stoppedUpdate allows only the ADMIN password of an engine database that has no container to be set.
func (b *InMemoryBackend) stoppedUpdate(db *ADB, in updateIn) *APIError {
	if in.AdminPassword == nil || !b.engineOn(db) || b.eng.live[db.ID] != nil {
		return errState("The Autonomous Database " + db.ID + " is stopped; start it before updating")
	}

	return nil
}

func (b *InMemoryBackend) updateStopped(db, next *ADB, ch change) (adbWire, string, string, error) {
	*db = *next
	b.eng.passwords[db.ID] = ch.password
	db.Etag = newEtag()

	return b.view(db), b.instant(db, "UPDATE_AUTONOMOUS_DATABASE").ID, db.Etag, nil
}

func (b *InMemoryBackend) finishUpdate(db *ADB, ch change) (adbWire, string, string, error) {
	typ := "UPDATE_AUTONOMOUS_DATABASE"

	if ch.password != "" && b.engineOn(db) && b.eng.live[db.ID] == nil {
		b.eng.passwords[db.ID] = ch.password
	}

	switch {
	case ch.scale:
		wr := b.begin(db, step{typ: typ, action: actionUpdated, state: stateScaling, target: stateAvailable})

		return b.view(db), wr.ID, db.Etag, nil
	case ch.password != "" && b.engineOn(db) && b.eng.live[db.ID] != nil:
		wr := b.begin(db, step{
			typ: typ, action: actionUpdated, state: stateUpdating, target: stateAvailable,
			work: b.passwordWork(db.ID, ch.password),
		})

		return b.view(db), wr.ID, db.Etag, nil
	case ch.other:
		wr := b.begin(db, step{typ: typ, action: actionUpdated, state: stateUpdating, target: stateAvailable})

		return b.view(db), wr.ID, db.Etag, nil
	}

	db.Etag = newEtag()

	return b.view(db), b.instant(db, typ).ID, db.Etag, nil
}

func (b *InMemoryBackend) applyUpdate(db *ADB, in updateIn) (change, *APIError) {
	var ch change

	if aerr := b.applyIdentity(db, in, &ch); aerr != nil {
		return ch, aerr
	}

	if aerr := applyShape(db, in, &ch); aerr != nil {
		return ch, aerr
	}

	if aerr := b.applyAccessUpdate(db, in, &ch); aerr != nil {
		return ch, aerr
	}

	if in.AdminPassword != nil {
		if aerr := checkAdminPassword(*in.AdminPassword); aerr != nil {
			return ch, aerr
		}

		ch.password, ch.other = *in.AdminPassword, true
	}

	if in.SecretID != nil {
		return ch, errParam("secretId is not supported by the emulator; pass adminPassword")
	}

	return ch, nil
}

// applyIdentity handles the names, tags, workload, version and license.
func (b *InMemoryBackend) applyIdentity(db *ADB, in updateIn, ch *change) *APIError {
	if in.DisplayName != nil {
		db.DisplayName = *in.DisplayName
	}

	if in.FreeformTags != nil {
		db.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		db.DefinedTags = in.DefinedTags
	}

	if in.DBName != nil && *in.DBName != db.DBName {
		if !validDBName(*in.DBName) {
			return errParam("dbName must begin with a letter and have at most 30 letters and digits")
		}

		if b.nameTaken(*in.DBName, db.ID) {
			return errConflict("An Autonomous Database named " + *in.DBName + " already exists in the tenancy")
		}

		db.DBName, ch.other = *in.DBName, true
	}

	return b.applyVersioning(db, in, ch)
}

func (b *InMemoryBackend) applyVersioning(db *ADB, in updateIn, ch *change) *APIError {
	if in.DBWorkload != nil && *in.DBWorkload != db.Workload {
		if aerr := checkWorkload(*in.DBWorkload); aerr != nil {
			return aerr
		}

		db.Workload, ch.other = *in.DBWorkload, true
	}

	if in.DBVersion != nil && *in.DBVersion != db.DBVersion {
		if aerr := checkVersion(*in.DBVersion, b.engineOn(db)); aerr != nil {
			return aerr
		}

		order := []string{versionOldest, versionMid, versionNew}
		if slices.Index(order, *in.DBVersion) < slices.Index(order, db.DBVersion) {
			return errParam("dbVersion cannot be downgraded")
		}

		db.DBVersion, ch.other = *in.DBVersion, true
	}

	if in.LicenseModel != nil && *in.LicenseModel != db.LicenseModel {
		if aerr := oneOf("licenseModel", *in.LicenseModel, licenseIncl, licenseByol); aerr != nil {
			return aerr
		}

		if db.IsFreeTier && *in.LicenseModel != licenseIncl {
			return errParam("Always Free databases use the LICENSE_INCLUDED license model")
		}

		db.LicenseModel, ch.other = *in.LicenseModel, true
	}

	return nil
}

// applyShape handles compute, storage, auto scaling and the Always Free conversion.
func applyShape(db *ADB, in updateIn, ch *change) *APIError {
	if aerr := applyTier(db, in, ch); aerr != nil {
		return aerr
	}

	resizes := in.ComputeModel != nil || in.ComputeCount != nil || in.OcpuCount != nil || in.CPUCoreCount != nil ||
		in.StorageTBs != nil || in.StorageGBs != nil || in.AutoScaling != nil || in.StorageAutoScaling != nil
	if resizes && db.IsFreeTier {
		return errParam("Always Free databases cannot be scaled")
	}

	if aerr := applyCompute(db, in, ch); aerr != nil {
		return aerr
	}

	if aerr := applyStorage(db, in, ch); aerr != nil {
		return aerr
	}

	if in.BackupDays != nil {
		db.BackupDays, ch.other = *in.BackupDays, true
	}

	return nil
}

func applyStorage(db *ADB, in updateIn, ch *change) *APIError {
	if in.StorageTBs != nil || in.StorageGBs != nil {
		gbs, aerr := resolveStorage(in.StorageTBs, in.StorageGBs, db.StorageGBs)
		if aerr != nil {
			return aerr
		}

		db.StorageGBs, ch.scale = gbs, true
	}

	if in.AutoScaling != nil {
		db.AutoScaling, ch.scale = *in.AutoScaling, true
	}

	if in.StorageAutoScaling != nil {
		db.StorageScales, ch.scale = *in.StorageAutoScaling, true
	}

	return nil
}

func applyCompute(db *ADB, in updateIn, ch *change) *APIError {
	if in.ComputeModel == nil && in.ComputeCount == nil && in.OcpuCount == nil && in.CPUCoreCount == nil {
		return nil
	}

	model, count := in.ComputeModel, in.ComputeCount

	if model != nil && count == nil && in.OcpuCount == nil && in.CPUCoreCount == nil {
		current := float64(db.ComputeCount)
		count = &current
	}

	if model == nil && (in.OcpuCount != nil || in.CPUCoreCount != nil) {
		m := modelOCPU
		model = &m
	}

	if model == nil {
		model = &db.ComputeModel
	}

	spec, aerr := resolveCompute(model, count, in.OcpuCount, in.CPUCoreCount)
	if aerr != nil {
		return aerr
	}

	db.ComputeModel, db.ComputeCount, ch.scale = spec.model, spec.count, true

	return nil
}

// applyAccessUpdate handles the ACL, mTLS and private endpoint.
func (b *InMemoryBackend) applyAccessUpdate(db *ADB, in updateIn, ch *change) *APIError {
	if in.Whitelisted != nil {
		if aerr := checkACL(*in.Whitelisted); aerr != nil {
			return aerr
		}

		db.Whitelisted, ch.other = *in.Whitelisted, true
		db.AccessControl = len(*in.Whitelisted) > 0
	}

	if in.AccessControl != nil {
		db.AccessControl, ch.other = *in.AccessControl, true
	}

	if in.Mtls != nil {
		db.Mtls, ch.other = *in.Mtls, true
	}

	if in.NsgIDs != nil {
		db.NsgIDs, ch.other = *in.NsgIDs, true
	}

	if in.SubnetID != nil || in.PrivateEndpointLabel != nil || in.PrivateEndpointIP != nil {
		subnet, label := deref(in.SubnetID, db.SubnetID), deref(in.PrivateEndpointLabel, db.EndpointLabel)
		if in.PrivateEndpointLabel != nil && *in.PrivateEndpointLabel == "" && in.SubnetID == nil {
			subnet = ""
		}

		if db.IsFreeTier && subnet != "" {
			return errParam("Always Free databases cannot use a private endpoint")
		}

		if aerr := b.placeEndpoint(db, subnet, label, deref(in.PrivateEndpointIP, "")); aerr != nil {
			return aerr
		}

		ch.other = true
	}

	return nil
}

func applyTier(db *ADB, in updateIn, ch *change) *APIError {
	if in.IsFreeTier == nil || *in.IsFreeTier == db.IsFreeTier {
		return nil
	}

	if *in.IsFreeTier {
		return errParam("a database cannot be converted to Always Free")
	}

	db.IsFreeTier, ch.other = false, true
	paidMinimum(db)

	return nil
}

// paidMinimum applies the compute and storage a paid Transaction Processing or Lakehouse database gets when an
// Always Free one is updated (Always Free Autonomous Database page, "update to paid"): 4 ECPUs and 1 TB.
func paidMinimum(db *ADB) {
	switch db.Workload {
	case workloadOLTP, workloadDW, workloadLH:
	default:
		return
	}

	if db.ComputeModel == modelECPU {
		db.ComputeCount = paidECPU
	}

	db.StorageGBs = gbPerTB
}
