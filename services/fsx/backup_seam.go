package fsx

// BackupFileSystem creates a user-initiated backup of a file system.
func (b *InMemoryBackend) BackupFileSystem(fileSystemID string) (*Backup, error) {
	return b.CreateBackup(&createBackupInput{FileSystemID: fileSystemID})
}

// RestoreFileSystemFromBackup creates a file system from a backup; empty subnets default to the source's.
func (b *InMemoryBackend) RestoreFileSystemFromBackup(
	backupID, kmsKeyID string, subnetIDs, securityGroupIDs []string,
) (*FileSystem, error) {
	if len(subnetIDs) == 0 {
		backups, _, err := b.DescribeBackups([]string{backupID}, nil, 0, "")
		if err != nil {
			return nil, err
		}

		if len(backups) > 0 && backups[0].FileSystem != nil {
			subnetIDs = backups[0].FileSystem.SubnetIDs
		}
	}

	return b.CreateFileSystemFromBackup(&createFileSystemFromBackupInput{
		BackupID: backupID, KmsKeyID: kmsKeyID, SubnetIDs: subnetIDs, SecurityGroupIDs: securityGroupIDs,
	})
}

// FileSystemExists reports whether a file system with the given ID exists.
func (b *InMemoryBackend) FileSystemExists(fileSystemID string) bool {
	b.mu.RLock("FileSystemExists")
	defer b.mu.RUnlock()

	return b.fileSystems.Has(fileSystemID)
}
