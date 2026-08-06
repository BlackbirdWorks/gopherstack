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

// scheduleOrderCompletion schedules a single-hop async transition of order
// id from PREPARING directly to COMPLETED, mirroring services/grafana's
// scheduleWorkspaceTransition. Real Order/LineItem status rollup rules are
// not encoded anywhere in the SDK (see PARITY.md's hardest-things #1); this
// backend picks the simplest defensible model -- one hop, no intermediate
// IN_PROGRESS/DELIVERED stop -- rather than inventing an unverifiable
// multi-stage timeline.
func (b *InMemoryBackend) scheduleOrderCompletion(id string) {
	b.work.After("OrderCompletion", orderTransitionDelay, func() {
		b.mu.Lock("OrderCompletion-async")
		defer b.mu.Unlock()

		o, ok := b.orders.Get(id)
		if !ok || o.Status != OrderStatusPreparing {
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

// recordOriginalSubscriptionLocked appends an ORIGINAL Subscription to the
// order's Outpost once the order completes, so GetOutpostBillingInformation
// has real accumulated state to report even before any CreateRenewal call.
// Callers must hold b.mu.
func (b *InMemoryBackend) recordOriginalSubscriptionLocked(o *Order) {
	outpost, ok := b.outposts.Get(o.OutpostID)
	if !ok {
		return
	}

	outpost.Subscriptions = append(outpost.Subscriptions, Subscription{
		SubscriptionID:     newSubscriptionID(),
		SubscriptionType:   SubscriptionTypeOriginal,
		SubscriptionStatus: SubscriptionStatusActive,
		Currency:           currencyUSD,
		BeginDate:          o.OrderFulfilledDate,
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
// (ConflictException) once the order has already reached a terminal state.
func (b *InMemoryBackend) CancelOrder(id string) error {
	b.mu.Lock("CancelOrder")
	defer b.mu.Unlock()

	o, ok := b.orders.Get(id)
	if !ok {
		return notFoundError(resourceOrder, id)
	}

	if o.Status != OrderStatusPreparing {
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
