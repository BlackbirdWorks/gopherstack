package azureeventhubs

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
)

var errBadAddress = errors.New("azureeventhubs: invalid AMQP address")

type addrKind int

const (
	addrCBS addrKind = iota + 1
	addrMgmt
	addrSend
	addrReceive
)

// amqpAddr is a parsed link address. Partition is -1 when unspecified.
type amqpAddr struct {
	hub       string
	group     string
	kind      addrKind
	partition int
}

// parseAMQPAddr understands "$cbs", "<hub>/$management", "<hub>",
// "<hub>/Partitions/<n>", "<hub>/Publishers/<name>" and
// "<hub>/ConsumerGroups/<cg>/Partitions/<n>".
func parseAMQPAddr(raw string) (amqpAddr, error) {
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		raw = u.Path
	}

	segs := strings.Split(strings.Trim(raw, "/"), "/")
	a := amqpAddr{partition: -1}

	switch len(segs) {
	case 1:
		return a.single(segs[0])
	case segsHubSuffix:
		return a.hubSuffix(segs)
	case segsHubNamed:
		return a.hubNamed(segs)
	case segsReceive:
		if strings.EqualFold(segs[1], "consumergroups") && strings.EqualFold(segs[3], "partitions") {
			a.group = segs[2]

			return a.partitioned(segs[0], segs[4], addrReceive)
		}
	}

	return amqpAddr{}, errBadAddress
}

const (
	segsHubSuffix = 2
	segsHubNamed  = 3
	segsReceive   = 5
)

func (a amqpAddr) single(seg string) (amqpAddr, error) {
	switch {
	case strings.EqualFold(seg, "$cbs"):
		a.kind = addrCBS
	case strings.EqualFold(seg, "$management"):
		a.kind = addrMgmt
	case seg != "":
		a.kind, a.hub = addrSend, seg
	default:
		return amqpAddr{}, errBadAddress
	}

	return a, nil
}

func (a amqpAddr) hubSuffix(segs []string) (amqpAddr, error) {
	if !strings.EqualFold(segs[1], "$management") {
		return amqpAddr{}, errBadAddress
	}

	a.kind, a.hub = addrMgmt, segs[0]

	return a, nil
}

func (a amqpAddr) hubNamed(segs []string) (amqpAddr, error) {
	switch {
	case strings.EqualFold(segs[1], "publishers"):
		a.kind, a.hub = addrSend, segs[0]

		return a, nil
	case strings.EqualFold(segs[1], "partitions"):
		return a.partitioned(segs[0], segs[2], addrSend)
	}

	return amqpAddr{}, errBadAddress
}

func (a amqpAddr) partitioned(hub, pid string, kind addrKind) (amqpAddr, error) {
	n, err := strconv.Atoi(pid)
	if err != nil || n < 0 || n >= MaxPartitionCount {
		return amqpAddr{}, errBadAddress
	}

	a.hub, a.partition, a.kind = hub, n, kind

	return a, nil
}
