package ecrpublic

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func repoKeyFn(r *Repository) string { return r.RepositoryName }

// imageTableKey returns the store.Table primary key for an image: repository
// name and digest joined by "@", matching the OCI image-reference convention.
// "@" never appears in a repository name or a "sha256:..." digest.
func imageTableKey(repositoryName, digest string) string { return repositoryName + "@" + digest }

func imageKeyFn(img *Image) string { return imageTableKey(img.RepositoryName, img.ImageDigest) }

func imageRepoIndexKeyFn(img *Image) string { return img.RepositoryName }

// registerAllTables registers every backend resource table exactly once.
// Must be called during construction only -- store.Register panics on a
// duplicate name.
func registerAllTables(b *InMemoryBackend) {
	b.repos = store.Register(b.registry, "repos", store.New(repoKeyFn))

	imagesT := store.Register(b.registry, "images", store.New(imageKeyFn))
	b.images = imagesT
	b.imagesByRepo = imagesT.AddIndex("repo", imageRepoIndexKeyFn)
}
