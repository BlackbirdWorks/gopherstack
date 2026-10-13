package main

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	omicsbackend "github.com/blackbirdworks/gopherstack/services/omics"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// wireOmicsS3 lets Omics read s3UriSettings, ReadmeUri and ContainerRegistryMapUri from S3.
func wireOmicsS3(omicsReg, s3Reg service.Registerable) {
	omicsH, ok := omicsReg.(*omicsbackend.Handler)
	if !ok {
		return
	}

	s3H, ok := s3Reg.(*s3backend.S3Handler)
	if !ok {
		return
	}

	omicsH.SetS3Reader(sfnbackend.NewS3Integration(s3H.Backend))
}
