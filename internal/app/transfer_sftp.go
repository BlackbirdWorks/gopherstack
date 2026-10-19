package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	transferbackend "github.com/blackbirdworks/gopherstack/services/transfer"
)

// wireTransferSFTP makes ONLINE Transfer SFTP servers accept real SSH/SFTP connections backed by S3.
func wireTransferSFTP(enabled bool, host string, byName map[string]service.Registerable) {
	if !enabled {
		return
	}

	xferH, ok := byName["Transfer"].(*transferbackend.Handler)
	if !ok {
		return
	}

	cfg := transferbackend.SFTPConfig{Host: host}

	if s3H, s3Ok := byName["S3"].(*s3backend.S3Handler); s3Ok {
		if bk, bkOk := s3H.Backend.(*s3backend.InMemoryBackend); bkOk {
			cfg.Store = bk
		}
	}

	if cfg.Store == nil {
		return
	}

	if lH, lOk := byName["Lambda"].(*lambdabackend.Handler); lOk {
		if bk, bkOk := lH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
			cfg.Lambda = bk
		}
	}

	xferH.EnableSFTP(cfg)
}
