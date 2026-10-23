package mwaa_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mwaa"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

type siblingConfig struct{ s3 *s3backend.S3Handler }

func (c siblingConfig) GetS3Handler() service.Registerable { return c.s3 }

// newS3Sibling returns an app config exposing an emulated S3 holding bucket.
func newS3Sibling(t *testing.T, bucket string, objects map[string]string) siblingConfig {
	t.Helper()

	be := s3backend.NewInMemoryBackend(nil)
	_, err := be.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	for k, v := range objects {
		_, err = be.PutObject(t.Context(), &awss3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(k), Body: strings.NewReader(v),
		})
		require.NoError(t, err)
	}

	return siblingConfig{s3: s3backend.NewHandler(be)}
}

func TestAirflowEngine_SyncFromEmulatedS3(t *testing.T) {
	t.Parallel()

	rig := newAirflowRig(t)
	rig.b.SetAppConfig(newS3Sibling(t, "dag-bucket", map[string]string{
		"dags/from_s3.py": "# from emulated s3",
		"other/skip.py":   "# outside prefix",
	}))
	// Rebuild the engine with the default (S3-backed) object source.
	rig.b.EnableAirflow(mwaa.AirflowConfig{
		Runtime: rig.rt, HTTP: rig.ht, PollInterval: 5 * time.Millisecond, SyncInterval: 10 * time.Millisecond,
	})
	rig.createAvailable(t, "s3env")

	spec := rig.rt.startedSpecs()[0]
	dags, _, _ := strings.Cut(spec.Mounts[0], ":")

	require.FileExists(t, dags+"/from_s3.py")
	require.NoFileExists(t, dags+"/skip.py")
}
