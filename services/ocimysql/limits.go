package ocimysql

import (
	"net/http"
	"strconv"
)

// Service limits of https://docs.oracle.com/en-us/iaas/Content/General/Concepts/servicelimits.htm (MySQL HeatWave,
// Oracle Universal Credits column). MySQL.256 is limited to a tenth of the other shapes.
const (
	DefaultMaxManualBackups  = 1200
	DefaultMaxStorageGB      = 100000
	DefaultMaxShapeInstances = 100
	DefaultMaxParallelCopies = 5
	bigShape                 = "MySQL.256"
	bigShapeDivisor          = 10
)

type limits struct {
	manualBackups int
	storageGB     int
	shapeSystems  int
	parallelCopy  int
}

func defaultLimits() limits {
	return limits{
		manualBackups: DefaultMaxManualBackups, storageGB: DefaultMaxStorageGB,
		shapeSystems: DefaultMaxShapeInstances, parallelCopy: DefaultMaxParallelCopies,
	}
}

// Configure applies the service limits.
func (b *InMemoryBackend) Configure(s Settings) {
	b.mu.Lock("Configure")
	defer b.mu.Unlock()

	if s.MaxManualBackups > 0 {
		b.limits.manualBackups = s.MaxManualBackups
	}

	if s.MaxStorageGB > 0 {
		b.limits.storageGB = s.MaxStorageGB
	}

	if s.MaxShapeInstances > 0 {
		b.limits.shapeSystems = s.MaxShapeInstances
	}

	if s.MaxParallelCopies > 0 {
		b.limits.parallelCopy = s.MaxParallelCopies
	}
}

func errLimit(name string, limit int) *APIError {
	return newErr(http.StatusBadRequest, "LimitExceeded", "The service limit "+name+" ("+strconv.Itoa(limit)+
		") has been reached")
}

// checkSystemLimits must be called with the lock held.
func (b *InMemoryBackend) checkSystemLimits(n *DBSystem) *APIError {
	shapeMax := b.limits.shapeSystems
	if n.Shape == bigShape {
		shapeMax = max(shapeMax/bigShapeDivisor, 1)
	}

	shapes, gb := 0, n.DataGB

	for _, s := range b.systems {
		if s.State == stateDeleted || s.AD != n.AD {
			continue
		}

		gb += s.DataGB

		if s.Shape == n.Shape {
			shapes++
		}
	}

	switch {
	case shapes >= shapeMax:
		return errLimit("the "+n.Shape+" instance count", shapeMax)
	case gb > b.limits.storageGB:
		return errLimit("MySQL Database Block Storage", b.limits.storageGB)
	}

	return nil
}

// checkManualBackupLimit must be called with the lock held.
func (b *InMemoryBackend) checkManualBackupLimit() *APIError {
	n := 0

	for _, bk := range b.backups {
		if bk.CreationType == creationManual && bk.State != stateDeleted {
			n++
		}
	}

	if n >= b.limits.manualBackups {
		return errLimit("MySQL Database Manual Backup Count", b.limits.manualBackups)
	}

	return nil
}

// checkCopyLimit must be called with the lock held.
func (b *InMemoryBackend) checkCopyLimit() *APIError {
	n := 0

	for _, bk := range b.backups {
		if bk.TimeCopied != nil && bk.State == stateCreating {
			n++
		}
	}

	if n >= b.limits.parallelCopy {
		return errLimit("MySQL Maximum Parallel Cross-Region Backup Copy Count", b.limits.parallelCopy)
	}

	return nil
}
