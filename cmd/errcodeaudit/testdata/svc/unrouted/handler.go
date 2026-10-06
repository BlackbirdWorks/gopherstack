package unrouted

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/fakesvc"
)

var _ = fakesvc.ServiceID

type store interface {
	lookupItem() error
}

var (
	ErrOrphanThing = errors.New("OrphanThingException")
	ErrChainThing  = errors.New("ChainThingException")
	ErrRoutedThing = errors.New("RoutedThingException")
)

type backend struct{}

func (b *backend) lookupItem() error { return ErrRoutedThing }

func (b *backend) orphanLookup() error { return ErrOrphanThing }

func (b *backend) chainOuter() error { return b.chainInner() }

func (b *backend) chainInner() error { return ErrChainThing }

func serve(s store) error { return s.lookupItem() }

var _ = serve
