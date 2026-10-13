package main

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// wireCloudFrontKVSImport lets CreateKeyValueStore import its initial data from an S3 object.
func wireCloudFrontKVSImport(cfReg, s3Reg service.Registerable) {
	cfH, ok := cfReg.(*cloudfrontbackend.Handler)
	if !ok {
		return
	}

	s3H, ok := s3Reg.(*s3backend.S3Handler)
	if !ok {
		return
	}

	cfH.SetKVSImportReader(sfnbackend.NewS3Integration(s3H.Backend))
}
