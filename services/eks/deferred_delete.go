package eks

// deferDeletes reports whether deletions dwell in DELETING before the resource disappears; only an
// explicit lifecycle delay turns this on, so default deletes stay immediate.
func (b *InMemoryBackend) deferDeletes() bool {
	return b.lifecycleDelay.Load() > 0
}

func (b *InMemoryBackend) scheduleRemoval(component string, remove func()) {
	b.scheduleStatusFlip(component, 0, remove)
}

func (b *InMemoryBackend) beginClusterDeleteLocked(name string) (*Cluster, error) {
	c, err := b.checkClusterDeletableLocked(name)
	if err != nil {
		return nil, err
	}

	if c.Status != statusDeleting {
		c.Status = statusDeleting

		b.scheduleRemoval("ClusterDelete", func() {
			cur, ok := b.clusters.Get(name)
			if !ok || cur.Status != statusDeleting {
				return
			}

			if _, delErr := b.deleteClusterLocked(name); delErr != nil {
				cur.Status = statusActive

				return
			}

			if b.clusterEng != nil {
				b.clusterEng.reap([]*liveCluster{b.detachClusterLocked(name)})
			}
		})
	}

	return c.clone(), nil
}

func (b *InMemoryBackend) beginNodegroupDeleteLocked(ng *Nodegroup) *Nodegroup {
	if ng.Status != statusDeleting {
		ng.Status = statusDeleting
		key := nodegroupKey(ng.ClusterName, ng.NodegroupName)

		b.scheduleRemoval("NodegroupDelete", func() {
			if cur, ok := b.nodegroups.Get(key); ok && cur.Status == statusDeleting {
				if cur.Tags != nil {
					cur.Tags.Close()
				}

				b.nodegroups.Delete(key)
			}
		})
	}

	return deepCopyNodegroup(ng)
}

func (b *InMemoryBackend) beginFargateDeleteLocked(profile *FargateProfile) *FargateProfile {
	if profile.Status != statusDeleting {
		profile.Status = statusDeleting
		key := fargateProfileKey(profile.ClusterName, profile.FargateProfileName)

		b.scheduleRemoval("FargateProfileDelete", func() {
			if cur, ok := b.fargateProfiles.Get(key); ok && cur.Status == statusDeleting {
				if cur.Tags != nil {
					cur.Tags.Close()
				}

				b.fargateProfiles.Delete(key)
			}
		})
	}

	return deepCopyFargateProfile(profile)
}

func (b *InMemoryBackend) beginAddonDeleteLocked(addon *Addon) *Addon {
	if addon.Status != statusDeleting {
		addon.Status = statusDeleting
		key := addonKey(addon.ClusterName, addon.AddonName)

		b.scheduleRemoval("AddonDelete", func() {
			if cur, ok := b.addons.Get(key); ok && cur.Status == statusDeleting {
				if cur.Tags != nil {
					cur.Tags.Close()
				}

				b.addons.Delete(key)
			}
		})
	}

	cp := *addon

	return &cp
}
