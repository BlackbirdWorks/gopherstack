package gcpartifactregistry

import (
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpstreamBasePublicRepositories(t *testing.T) {
	t.Parallel()

	type (
		aptBase = artifactregistrypb.RemoteRepositoryConfig_AptRepository_PublicRepository_RepositoryBase
		yumBase = artifactregistrypb.RemoteRepositoryConfig_YumRepository_PublicRepository_RepositoryBase
	)

	apt := func(base aptBase, path string) *artifactregistrypb.RemoteRepositoryConfig {
		return &artifactregistrypb.RemoteRepositoryConfig{
			RemoteSource: &artifactregistrypb.RemoteRepositoryConfig_AptRepository_{
				AptRepository: &artifactregistrypb.RemoteRepositoryConfig_AptRepository{
					Upstream: &artifactregistrypb.RemoteRepositoryConfig_AptRepository_PublicRepository_{
						PublicRepository: &artifactregistrypb.RemoteRepositoryConfig_AptRepository_PublicRepository{
							RepositoryBase: base, RepositoryPath: path,
						},
					},
				},
			},
		}
	}

	yum := func(base yumBase, path string) *artifactregistrypb.RemoteRepositoryConfig {
		return &artifactregistrypb.RemoteRepositoryConfig{
			RemoteSource: &artifactregistrypb.RemoteRepositoryConfig_YumRepository_{
				YumRepository: &artifactregistrypb.RemoteRepositoryConfig_YumRepository{
					Upstream: &artifactregistrypb.RemoteRepositoryConfig_YumRepository_PublicRepository_{
						PublicRepository: &artifactregistrypb.RemoteRepositoryConfig_YumRepository_PublicRepository{
							RepositoryBase: base, RepositoryPath: path,
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name string
		cfg  *artifactregistrypb.RemoteRepositoryConfig
		want string
	}{
		{
			"debian",
			apt(artifactregistrypb.RemoteRepositoryConfig_AptRepository_PublicRepository_DEBIAN, "debian/dists/stable"),
			"http://deb.debian.org/debian/dists/stable",
		},
		{
			"ubuntu",
			apt(artifactregistrypb.RemoteRepositoryConfig_AptRepository_PublicRepository_UBUNTU, "ubuntu/dists/noble"),
			"http://archive.ubuntu.com/ubuntu/dists/noble",
		},
		{
			"debian snapshot",
			apt(artifactregistrypb.RemoteRepositoryConfig_AptRepository_PublicRepository_DEBIAN_SNAPSHOT, "archive/x"),
			"https://snapshot.debian.org/archive/x",
		},
		{
			"centos stream",
			yum(artifactregistrypb.RemoteRepositoryConfig_YumRepository_PublicRepository_CENTOS_STREAM,
				"9-stream/BaseOS/x86_64/os"),
			"https://mirror.stream.centos.org/9-stream/BaseOS/x86_64/os",
		},
		{
			"rocky",
			yum(artifactregistrypb.RemoteRepositoryConfig_YumRepository_PublicRepository_ROCKY, "9/BaseOS/x86_64/os"),
			"http://dl.rockylinux.org/9/BaseOS/x86_64/os",
		},
		{
			"epel",
			yum(artifactregistrypb.RemoteRepositoryConfig_YumRepository_PublicRepository_EPEL, "9/Everything/x86_64"),
			"https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := upstreamBase(tt.cfg)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
