package gcpstorage

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"
)

const (
	janitorInterval = time.Minute
	day             = 24 * time.Hour
	actionDelete    = "Delete"
	actionSetClass  = "SetStorageClass"
	dateLayout      = "2006-01-02"
)

type lifecycleCondition struct {
	NumNewerVersions        *int64       `json:"numNewerVersions"`
	SizeAboveBytes          *json.Number `json:"sizeAboveBytes"`
	SizeBelowBytes          *json.Number `json:"sizeBelowBytes"`
	IsLive                  *bool        `json:"isLive"`
	DaysSinceCustomTime     *int64       `json:"daysSinceCustomTime"`
	DaysSinceNoncurrentTime *int64       `json:"daysSinceNoncurrentTime"`
	Age                     *int64       `json:"age"`
	CustomTimeBefore        string       `json:"customTimeBefore"`
	NoncurrentTimeBefore    string       `json:"noncurrentTimeBefore"`
	CreatedBefore           string       `json:"createdBefore"`
	MatchesSuffix           []string     `json:"matchesSuffix"`
	MatchesStorageClass     []string     `json:"matchesStorageClass"`
	MatchesPrefix           []string     `json:"matchesPrefix"`
}

type lifecycleRule struct {
	Action struct {
		Type         string `json:"type"`
		StorageClass string `json:"storageClass"`
	} `json:"action"`
	Condition lifecycleCondition `json:"condition"`
}

func lifecycleRules(bk *Bucket) []lifecycleRule {
	raw, ok := bk.Config["lifecycle"]
	if !ok {
		return nil
	}

	var doc struct {
		Rule []lifecycleRule `json:"rule"`
	}

	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}

	return doc.Rule
}

func beforeDate(t time.Time, date string) bool {
	d, err := time.Parse(dateLayout, date)

	return err == nil && !t.IsZero() && t.Before(d)
}

func daysSince(t, now time.Time, n int64) bool {
	return !t.IsZero() && now.Sub(t) >= time.Duration(n)*day
}

func sizeCompare(limit *json.Number, size int64, cmp func(size, limit int64) bool) bool {
	if limit == nil {
		return false
	}

	n, err := limit.Int64()

	return err == nil && cmp(size, n)
}

func matchesAny(s string, subs []string, match func(string, string) bool) bool {
	return slices.ContainsFunc(subs, func(p string) bool { return match(s, p) })
}

// matches reports whether o satisfies every condition set on c; newer counts the versions after o, live included.
func (c lifecycleCondition) matches(o *Object, newer int, now time.Time) bool {
	live := o.Deleted.IsZero()

	checks := []struct {
		set bool
		ok  bool
	}{
		{c.Age != nil, c.Age != nil && now.Sub(o.Created) >= time.Duration(*c.Age)*day},
		{c.IsLive != nil, c.IsLive != nil && *c.IsLive == live},
		{c.NumNewerVersions != nil, c.NumNewerVersions != nil && int64(newer) >= *c.NumNewerVersions},
		{c.CreatedBefore != "", beforeDate(o.Created, c.CreatedBefore)},
		{c.CustomTimeBefore != "", beforeDate(o.CustomTime, c.CustomTimeBefore)},
		{
			c.DaysSinceCustomTime != nil,
			c.DaysSinceCustomTime != nil && daysSince(o.CustomTime, now, *c.DaysSinceCustomTime),
		},
		{
			c.DaysSinceNoncurrentTime != nil,
			!live && c.DaysSinceNoncurrentTime != nil && daysSince(o.Deleted, now, *c.DaysSinceNoncurrentTime),
		},
		{c.NoncurrentTimeBefore != "", !live && beforeDate(o.Deleted, c.NoncurrentTimeBefore)},
		{len(c.MatchesPrefix) > 0, matchesAny(o.Name, c.MatchesPrefix, strings.HasPrefix)},
		{len(c.MatchesSuffix) > 0, matchesAny(o.Name, c.MatchesSuffix, strings.HasSuffix)},
		{len(c.MatchesStorageClass) > 0, slices.Contains(c.MatchesStorageClass, o.StorageClass)},
		{
			c.SizeAboveBytes != nil,
			sizeCompare(c.SizeAboveBytes, o.Size(), func(size, limit int64) bool { return size > limit }),
		},
		{
			c.SizeBelowBytes != nil,
			sizeCompare(c.SizeBelowBytes, o.Size(), func(size, limit int64) bool { return size < limit }),
		},
	}

	for _, ck := range checks {
		if ck.set && !ck.ok {
			return false
		}
	}

	return true
}

const (
	rankStandard = iota
	rankNearline
	rankColdline
	rankArchive
)

func classRank(class string) (int, bool) {
	switch class {
	case "STANDARD", "MULTI_REGIONAL", "REGIONAL", "DURABLE_REDUCED_AVAILABILITY":
		return rankStandard, true
	case "NEARLINE":
		return rankNearline, true
	case "COLDLINE":
		return rankColdline, true
	case "ARCHIVE":
		return rankArchive, true
	default:
		return 0, false
	}
}

func classTransitionAllowed(from, to string) bool {
	rf, okF := classRank(from)
	rt, okT := classRank(to)

	return okF && okT && rt > rf
}

// lifecycleAction picks the single action to run on o: Delete beats SetStorageClass, deepest class wins.
func lifecycleAction(rules []lifecycleRule, o *Object, newer int, now time.Time) (string, string) {
	target := ""

	for _, r := range rules {
		if !r.Condition.matches(o, newer, now) {
			continue
		}

		switch r.Action.Type {
		case actionDelete:
			return actionDelete, ""
		case actionSetClass:
			if classTransitionAllowed(o.StorageClass, r.Action.StorageClass) &&
				rankOf(r.Action.StorageClass) > rankOf(target) {
				target = r.Action.StorageClass
			}
		}
	}

	if target != "" {
		return actionSetClass, target
	}

	return "", ""
}

// RunLifecycle applies every bucket's lifecycle rules once and purges expired soft-deleted objects.
func (b *InMemoryBackend) RunLifecycle() {
	defer b.flushEvents()

	b.mu.Lock("RunLifecycle")
	defer b.mu.Unlock()

	now := b.now().UTC()

	for _, bk := range b.buckets {
		b.sweepBucketLocked(bk, now)
		bk.SoftDeleted = slices.DeleteFunc(
			bk.SoftDeleted,
			func(o *Object) bool { return !now.Before(o.HardDeleteTime) },
		)
	}
}

func (b *InMemoryBackend) sweepBucketLocked(bk *Bucket, now time.Time) {
	rules := lifecycleRules(bk)
	if len(rules) == 0 {
		return
	}

	for _, versions := range bk.Objects {
		for i, o := range slices.Clone(versions) {
			act, class := lifecycleAction(rules, o, len(versions)-1-i, now)

			switch act {
			case actionDelete:
				b.lifecycleDeleteLocked(bk, o, now)
			case actionSetClass:
				o.StorageClass, o.ClassUpdated, o.Updated = class, now, now
			}
		}
	}
}

func (b *InMemoryBackend) lifecycleDeleteLocked(bk *Bucket, o *Object, now time.Time) {
	if b.guardMutation(bk, o) != nil {
		return
	}

	if o.Deleted.IsZero() && bk.Versioning {
		o.Deleted = now
		b.queueLocked(ObjectArchived, o)

		return
	}

	b.removeVersionLocked(bk, o)
}

// RunJanitor sweeps lifecycle rules every interval until ctx ends.
func (b *InMemoryBackend) RunJanitor(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.RunLifecycle()
		}
	}
}

func rankOf(class string) int {
	r, _ := classRank(class)

	return r
}
