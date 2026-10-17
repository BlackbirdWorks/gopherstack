package main

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// wireBackupResources connects Backup to the services it reads from and restores into.
func wireBackupResources(byName map[string]service.Registerable) {
	wireBackupS3(byName["Backup"], byName["S3"])
	wireBackupDynamoDB(byName["Backup"], byName["DynamoDB"])
	wireBackupEC2(byName["Backup"], byName["EC2"])
}

// wireBackupDynamoDB lets Backup snapshot DynamoDB tables at backup time and recreate them on restore.
func wireBackupDynamoDB(backupReg, ddbReg service.Registerable) {
	backupH, ok := backupReg.(*backupbackend.Handler)
	if !ok {
		return
	}

	ddbH, ddbOk := ddbReg.(*ddbbackend.DynamoDBHandler)
	if !ddbOk {
		return
	}

	if ddbBk, bkOk := ddbH.Backend.(*ddbbackend.InMemoryDB); bkOk {
		backupH.Backend.SetDynamoDBBackend(ddbBk)
	}
}

// wireBackupEC2 lets Backup recreate EBS volumes on restore, in Backup's own region.
func wireBackupEC2(backupReg, ec2Reg service.Registerable) {
	backupH, ok := backupReg.(*backupbackend.Handler)
	if !ok {
		return
	}

	ec2H, ec2Ok := ec2Reg.(*ec2backend.Handler)
	if !ec2Ok {
		return
	}

	for _, bk := range ec2H.RegionBackends() {
		ec2Bk, bkOk := bk.(*ec2backend.InMemoryBackend)
		if bkOk && ec2Bk.Region == backupH.Backend.Region() {
			backupH.Backend.SetVolumeService(
				ebsVolumeAdapter{bk: ec2Bk}, backupH.Backend.Region(), backupH.Backend.AccountID(),
			)

			return
		}
	}
}

type ebsVolumeAdapter struct{ bk *ec2backend.InMemoryBackend }

func (a ebsVolumeAdapter) DescribeVolume(id string) (backupbackend.VolumeInfo, bool) {
	vols := a.bk.DescribeVolumes([]string{id})
	if len(vols) == 0 {
		return backupbackend.VolumeInfo{}, false
	}

	v := vols[0]

	return backupbackend.VolumeInfo{AZ: v.AZ, VolumeType: v.VolumeType, Size: v.Size}, true
}

func (a ebsVolumeAdapter) CreateVolume(info backupbackend.VolumeInfo) (string, error) {
	vol, err := a.bk.CreateVolume(info.AZ, info.VolumeType, info.Size, "")
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return vol.ID, nil
}
