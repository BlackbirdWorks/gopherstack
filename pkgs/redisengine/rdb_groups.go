package redisengine

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/alicebob/miniredis/v2"
)

const maxPendingScan = math.MaxInt32

// attachStreamState adds the consumer groups and the last generated ID of a stream key.
func attachStreamState(cl *localClient, kd *keyDump) error {
	if cl == nil {
		return nil
	}

	if err := cl.selectDB(kd.DB); err != nil {
		return err
	}

	info, err := cl.do("XINFO", "STREAM", kd.Key)
	if err != nil {
		return err
	}

	kd.StreamLastID = replyString(replyMap(info)["last-generated-id"])

	raw, err := cl.do("XINFO", "GROUPS", kd.Key)
	if err != nil {
		return err
	}

	groups, _ := raw.([]any)
	now := time.Now().UnixMilli()

	for _, g := range groups {
		m := replyMap(g)
		name := replyString(m["name"])

		consumers, cErr := readConsumers(cl, kd.Key, name, now)
		if cErr != nil {
			return cErr
		}

		pending, pErr := readPending(cl, kd.Key, name, now)
		if pErr != nil {
			return pErr
		}

		kd.Groups = append(kd.Groups, streamGroupDump{
			Name: name, LastID: replyString(m["last-delivered-id"]), Consumers: consumers, Pending: pending,
		})
	}

	return nil
}

func readConsumers(cl *localClient, key, group string, now int64) ([]streamConsumerDump, error) {
	raw, err := cl.do("XINFO", "CONSUMERS", key, group)
	if err != nil {
		return nil, err
	}

	items, _ := raw.([]any)
	out := make([]streamConsumerDump, 0, len(items))

	for _, it := range items {
		m := replyMap(it)
		c := streamConsumerDump{Name: replyString(m["name"]), SeenMS: now, ActiveMS: int64(streamNoTime)}

		if idle := replyInt(m["idle"]); idle >= 0 {
			c.SeenMS = now - int64(idle)
		}

		if inactive := replyInt(m["inactive"]); inactive >= 0 {
			c.ActiveMS = now - int64(inactive)
		}

		out = append(out, c)
	}

	return out, nil
}

func readPending(cl *localClient, key, group string, now int64) ([]streamPendingDump, error) {
	raw, err := cl.do("XPENDING", key, group, "-", "+", strconv.Itoa(maxPendingScan))
	if err != nil {
		return nil, err
	}

	items, _ := raw.([]any)
	out := make([]streamPendingDump, 0, len(items))

	for _, it := range items {
		f, _ := it.([]any)
		if len(f) < 4 { //nolint:mnd // id, consumer, idle, deliveries
			continue
		}

		out = append(out, streamPendingDump{
			ID:         replyString(f[0]),
			Consumer:   replyString(f[1]),
			DeliveryMS: now - int64(replyInt(f[2])),
			Count:      replyInt(f[3]),
		})
	}

	return out, nil
}

// restoreStreamState recreates what a plain XADD replay cannot: the last generated ID, groups and pending entries.
func restoreStreamState(cl *localClient, mr *miniredis.Miniredis, kd *keyDump) error {
	if cl == nil || (len(kd.Groups) == 0 && kd.StreamLastID == "") {
		return nil
	}

	if err := cl.selectDB(kd.DB); err != nil {
		return err
	}

	if err := restoreLastID(cl, mr, kd); err != nil {
		return err
	}

	for i := range kd.Groups {
		if err := restoreGroup(cl, kd, &kd.Groups[i]); err != nil {
			return fmt.Errorf("group %q: %w", kd.Groups[i].Name, err)
		}
	}

	return nil
}

func restoreLastID(cl *localClient, mr *miniredis.Miniredis, kd *keyDump) error {
	want, ok := parseStreamID(kd.StreamLastID)
	if !ok || (want == streamID{}) {
		return nil
	}

	var have streamID

	if n := len(kd.Stream); n > 0 {
		have, _ = parseStreamID(kd.Stream[n-1].ID)
	}

	if !greaterID(want, have) {
		return nil
	}

	id := want.String()
	if _, err := mr.DB(kd.DB).XAdd(kd.Key, id, []string{"_", "_"}); err != nil {
		return err
	}

	_, err := cl.do("XDEL", kd.Key, id)

	return err
}

func restoreGroup(cl *localClient, kd *keyDump, g *streamGroupDump) error {
	if len(g.Pending) == 0 {
		if _, err := cl.do("XGROUP", "CREATE", kd.Key, g.Name, g.LastID); err != nil {
			return err
		}

		return createConsumers(cl, kd.Key, g, nil)
	}

	if _, err := cl.do("XGROUP", "CREATE", kd.Key, g.Name, "0"); err != nil {
		return err
	}

	owners := make(map[string]string, len(g.Pending))
	for _, p := range g.Pending {
		owners[p.ID] = p.Consumer
	}

	if err := replayDelivered(cl, kd, g, owners); err != nil {
		return err
	}

	if err := createConsumers(cl, kd.Key, g, owners); err != nil {
		return err
	}

	return restorePendingStats(cl, kd.Key, g)
}

func replayDelivered(cl *localClient, kd *keyDump, g *streamGroupDump, owners map[string]string) error {
	last, _ := parseStreamID(g.LastID)
	filler := g.Pending[0].Consumer
	if len(g.Consumers) > 0 {
		filler = g.Consumers[0].Name
	}

	var run []string

	flush := func() error {
		if len(run) == 0 {
			return nil
		}

		defer func() { run = run[:0] }()

		return readAndAck(cl, kd.Key, g.Name, filler, run)
	}

	for _, e := range kd.Stream {
		id, _ := parseStreamID(e.ID)
		if greaterID(id, last) {
			break
		}

		owner, pending := owners[e.ID]
		if !pending {
			run = append(run, e.ID)

			continue
		}

		if err := flush(); err != nil {
			return err
		}

		if _, err := cl.do(cmdXReadGroup, "GROUP", g.Name, owner, "COUNT", "1", "STREAMS", kd.Key, ">"); err != nil {
			return err
		}
	}

	return flush()
}

func readAndAck(cl *localClient, key, group, consumer string, ids []string) error {
	args := []string{cmdXReadGroup, "GROUP", group, consumer, "COUNT", strconv.Itoa(len(ids)), "STREAMS", key, ">"}
	if _, err := cl.do(args...); err != nil {
		return err
	}

	_, err := cl.do(append([]string{"XACK", key, group}, ids...)...)

	return err
}

func createConsumers(cl *localClient, key string, g *streamGroupDump, owners map[string]string) error {
	has := map[string]struct{}{}
	for _, o := range owners {
		has[o] = struct{}{}
	}

	for _, c := range g.Consumers {
		if _, ok := has[c.Name]; ok {
			continue
		}

		if _, err := cl.do("XGROUP", "CREATECONSUMER", key, g.Name, c.Name); err != nil {
			return err
		}
	}

	return nil
}

func restorePendingStats(cl *localClient, key string, g *streamGroupDump) error {
	now := time.Now().UnixMilli()
	pending := append([]streamPendingDump(nil), g.Pending...)

	for _, p := range pending {
		idle := max(now-p.DeliveryMS, 0)
		args := []string{
			"XCLAIM", key, g.Name, p.Consumer, "0", p.ID,
			"IDLE", strconv.FormatInt(idle, 10), "RETRYCOUNT", strconv.Itoa(max(p.Count, 1)), "JUSTID",
		}

		if _, err := cl.do(args...); err != nil {
			return err
		}
	}

	return nil
}
