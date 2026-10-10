package workmail

// DirectoryDeleter deletes the Directory Service directory behind an organization.
type DirectoryDeleter interface {
	// DeleteDirectory removes the directory; a directory that does not exist is not an error.
	DeleteDirectory(region, directoryID string) error
}

// SetDirectoryDeleter wires the accessor used by DeleteOrganization with DeleteDirectory set.
func (b *InMemoryBackend) SetDirectoryDeleter(d DirectoryDeleter) {
	b.mu.Lock("SetDirectoryDeleter")
	defer b.mu.Unlock()

	b.directories = d
}
