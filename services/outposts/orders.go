package outposts

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// buildOrderLineItems validates and constructs the LineItems for a new
// order, resolving each CatalogItemId against the static catalog seed (an
// Order referencing a nonexistent CatalogItemId must fail, not silently
// succeed -- see PARITY.md's "Site/Order/Outpost relationship" note).
func buildOrderLineItems(reqs []lineItemRequestWire) ([]LineItem, error) {
	items := make([]LineItem, 0, len(reqs))

	for _, r := range reqs {
		if r.CatalogItemId == "" {
			return nil, validationError("LineItems[].CatalogItemId is required")
		}

		if _, ok := findCatalogItem(r.CatalogItemId); !ok {
			return nil, notFoundError(resourceCatalogItem, r.CatalogItemId)
		}

		if r.Quantity <= 0 {
			return nil, validationError("LineItems[].Quantity must be positive")
		}

		items = append(items, LineItem{
			LineItemID:    newLineItemID(),
			CatalogItemID: r.CatalogItemId,
			Quantity:      r.Quantity,
			Status:        LineItemStatusPreparing,
		})
	}

	return items, nil
}

// CreateOrder creates a new order against an existing Outpost, optionally
// consuming a Quote, and schedules its async single-hop completion (see
// scheduleOrderCompletion) -- OrderType is always "OUTPOST": CreateOrderInput
// has no OrderType member, so "REPLACEMENT" can never be produced by this
// API surface (see models.go's Order doc comment).
func (b *InMemoryBackend) CreateOrder(req *createOrderRequest) (*Order, error) {
	if req.OutpostIdentifier == "" {
		return nil, validationError("OutpostIdentifier is required")
	}

	if !isValidPaymentOption(req.PaymentOption) {
		return nil, validationError("invalid PaymentOption: " + req.PaymentOption)
	}

	if req.PaymentTerm != "" && !isValidPaymentTerm(req.PaymentTerm) {
		return nil, validationError("invalid PaymentTerm: " + req.PaymentTerm)
	}

	items, err := buildOrderLineItems(req.LineItems)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateOrder")
	defer b.mu.Unlock()

	o, ok := b.resolveOutpostLocked(req.OutpostIdentifier)
	if !ok {
		return nil, notFoundError(resourceOutpost, req.OutpostIdentifier)
	}

	var quote *Quote

	if req.QuoteIdentifier != "" {
		q, quoteOK := b.resolveQuoteLocked(req.QuoteIdentifier)
		if !quoteOK {
			return nil, notFoundError(resourceQuote, req.QuoteIdentifier)
		}

		if q.Status != QuoteStatusCreated {
			return nil, conflictError("quote is not available for ordering: " + q.Status)
		}

		if req.QuoteOptionIdentifier != "" && req.QuoteOptionIdentifier != q.QuoteOptionID {
			return nil, validationError("unknown QuoteOptionIdentifier: " + req.QuoteOptionIdentifier)
		}

		quote = q
	}

	id := newOrderID()
	now := time.Now().UTC()

	order := &Order{
		ID:                    id,
		OutpostID:             o.ID,
		OrderType:             "OUTPOST",
		Status:                OrderStatusPreparing,
		PaymentOption:         req.PaymentOption,
		PaymentTerm:           req.PaymentTerm,
		QuoteIdentifier:       req.QuoteIdentifier,
		QuoteOptionIdentifier: req.QuoteOptionIdentifier,
		LineItems:             items,
		OrderSubmissionDate:   now,
	}

	b.orders.Put(order)

	if quote != nil {
		quote.Status = QuoteStatusOrderSubmitted
		quote.SubmittedOrderID = id
	}

	b.scheduleOrderCompletion(id)

	return order.clone(), nil
}

// clone returns a deep copy of o, so the returned Order shares no mutable
// memory with the backend's stored copy -- LineItems is a slice, and
// scheduleOrderCompletion mutates its elements in place
// (o.LineItems[i].Status = ...) after the order transitions, which would
// otherwise race with any earlier caller still holding (and reading) a
// "copy" whose LineItems slice header aliases the same backing array.
func (o *Order) clone() *Order {
	cp := *o
	cp.LineItems = append([]LineItem(nil), o.LineItems...)

	return &cp
}

// scheduleOrderCompletion schedules order id's async multi-hop transition
// through the real SDK-declared, non-deprecated OrderStatus timeline:
// PREPARING -> IN_PROGRESS -> DELIVERED -> COMPLETED, one hop per chained
// b.work.After call (mirroring services/mgn's exportimport.go
// scheduleExportLocked, which chains Pending -> Started -> Succeeded the
// same way). Each hop's LineItems move in lockstep -- an invented but
// documented rollup rule (see consts.go), since the SDK does not encode
// one. A hop that finds the order not in the status it expects (already
// cancelled, or restored from a snapshot mid-flight with no pending timer)
// silently stops advancing the chain rather than forcing a transition.
func (b *InMemoryBackend) scheduleOrderCompletion(id string) {
	b.work.After("OrderInProgress", orderTransitionDelay, func() {
		b.mu.Lock("OrderInProgress-async")
		advanced := b.advanceOrderStatusLocked(id, OrderStatusPreparing, OrderStatusInProgress, LineItemStatusBuilding)
		b.mu.Unlock()

		if !advanced {
			return
		}

		b.scheduleOrderDelivery(id)
	})
}

func (b *InMemoryBackend) scheduleOrderDelivery(id string) {
	b.work.After("OrderDelivered", orderTransitionDelay, func() {
		b.mu.Lock("OrderDelivered-async")
		advanced := b.advanceOrderStatusLocked(id, OrderStatusInProgress, OrderStatusDelivered, LineItemStatusDelivered)
		b.mu.Unlock()

		if !advanced {
			return
		}

		b.scheduleOrderCompletionFinal(id)
	})
}

func (b *InMemoryBackend) scheduleOrderCompletionFinal(id string) {
	b.work.After("OrderCompleted", orderTransitionDelay, func() {
		b.mu.Lock("OrderCompleted-async")
		defer b.mu.Unlock()

		o, ok := b.orders.Get(id)
		if !ok || o.Status != OrderStatusDelivered {
			return
		}

		o.Status = OrderStatusCompleted
		o.OrderFulfilledDate = time.Now().UTC()

		for i := range o.LineItems {
			o.LineItems[i].Status = LineItemStatusInstalled
		}

		b.recordOriginalSubscriptionLocked(o)
	})
}

// advanceOrderStatusLocked moves order id from fromStatus to toStatus and
// sets every LineItem's Status to lineItemStatus, but only if the order is
// still in fromStatus (a concurrent CancelOrder, or a restart with no
// pending timer, means it isn't) -- reports whether it advanced. Callers
// must hold b.mu.
func (b *InMemoryBackend) advanceOrderStatusLocked(id, fromStatus, toStatus, lineItemStatus string) bool {
	o, ok := b.orders.Get(id)
	if !ok || o.Status != fromStatus {
		return false
	}

	o.Status = toStatus
	for i := range o.LineItems {
		o.LineItems[i].Status = lineItemStatus
	}

	return true
}

// recordOriginalSubscriptionLocked appends an ORIGINAL Subscription to the
// order's Outpost once the order completes, so GetOutpostBillingInformation
// has real accumulated state to report even before any CreateRenewal call,
// and sets the Outpost's ContractEndDate from the order's own PaymentTerm
// (termYears, shared with CreateRenewal's identical computation in
// pricing.go/renewals.go) so OUTPOST_RENEWAL_REQUIRED_ERROR has real state
// to evaluate even before any renewal is ever created. Callers must hold
// b.mu.
func (b *InMemoryBackend) recordOriginalSubscriptionLocked(o *Order) {
	outpost, ok := b.outposts.Get(o.OutpostID)
	if !ok {
		return
	}

	endDate := o.OrderFulfilledDate.AddDate(termYears(o.PaymentTerm), 0, 0)
	outpost.ContractEndDate = endDate

	outpost.Subscriptions = append(outpost.Subscriptions, Subscription{
		SubscriptionID:     newSubscriptionID(),
		SubscriptionType:   SubscriptionTypeOriginal,
		SubscriptionStatus: SubscriptionStatusActive,
		Currency:           currencyUSD,
		BeginDate:          o.OrderFulfilledDate,
		EndDate:            endDate,
		OrderIDs:           []string{o.ID},
	})
}

// GetOrder returns a copy of the order with the given ID (Orders have no
// ARN form -- GetOrderInput.OrderId is "The ID of the order", not
// "ID or ARN").
func (b *InMemoryBackend) GetOrder(id string) (*Order, error) {
	b.mu.RLock("GetOrder")
	defer b.mu.RUnlock()

	o, ok := b.orders.Get(id)
	if !ok {
		return nil, notFoundError(resourceOrder, id)
	}

	return o.clone(), nil
}

// CancelOrder cancels the order with the given ID. Rejected
// (ConflictException) once the order has reached DELIVERED or a terminal
// state -- an order still PREPARING or IN_PROGRESS remains cancellable
// (a documented generalization of the original PREPARING-only rule, now
// that this backend actually transitions through IN_PROGRESS -- see
// scheduleOrderCompletion).
func (b *InMemoryBackend) CancelOrder(id string) error {
	b.mu.Lock("CancelOrder")
	defer b.mu.Unlock()

	o, ok := b.orders.Get(id)
	if !ok {
		return notFoundError(resourceOrder, id)
	}

	if o.Status != OrderStatusPreparing && o.Status != OrderStatusInProgress {
		return conflictErrorWithResource(ResourceTypeOrder, id, "order is not cancellable in status: "+o.Status)
	}

	o.Status = OrderStatusCancelled

	for i := range o.LineItems {
		o.LineItems[i].Status = LineItemStatusCancelled
	}

	return nil
}

// ListOrders returns a page of order summaries, optionally filtered by
// outpostIdentifierFilter (an Outpost ID or ARN).
func (b *InMemoryBackend) ListOrders(outpostIdentifierFilter, token string, limit int) page.Page[*Order] {
	b.mu.RLock("ListOrders")
	defer b.mu.RUnlock()

	var all []*Order

	if outpostIdentifierFilter != "" {
		if o, ok := b.resolveOutpostLocked(outpostIdentifierFilter); ok {
			all = append(all, b.ordersByOutpost.Get(o.ID)...)
			sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
		}
	} else {
		all = b.orders.Snapshot()
	}

	// Clone before returning: these are the live, backend-owned pointers, and
	// scheduleOrderCompletion mutates Status/LineItems on them in place after
	// this call returns and the lock is released -- see clone's doc comment.
	cloned := make([]*Order, len(all))
	for i, o := range all {
		cloned[i] = o.clone()
	}

	return page.New(cloned, token, limit, defaultPageLimit)
}
