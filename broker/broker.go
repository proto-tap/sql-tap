package broker

import (
	"sync"
	"sync/atomic"

	"github.com/proto-tap/sql-tap/proxy"
)

// Broker implements a non-blocking fan-out pub/sub for proxy events.
// Slow subscribers drop events to avoid blocking the publisher; each subscription counts its drops.
type Broker struct {
	mu          sync.RWMutex
	subscribers map[int]subscriber
	nextID      int
	bufSize     int
}

type subscriber struct {
	ch      chan proxy.Event
	dropped *atomic.Uint64
	drops   chan struct{}
}

// Subscription receives published events.
type Subscription struct {
	// Events is closed by Close.
	Events <-chan proxy.Event
	// Drops receives a signal after Dropped has increased. Signals coalesce, so read Dropped on receipt.
	// Reporting from here, rather than alongside the next event, covers a drop with no event after it.
	Drops   <-chan struct{}
	dropped *atomic.Uint64
	close   func()
}

// Dropped returns how many events this subscription has dropped because its buffer was full.
func (s Subscription) Dropped() uint64 {
	return s.dropped.Load()
}

// Close unsubscribes. It is idempotent.
func (s Subscription) Close() {
	s.close()
}

func New(bufSize int) *Broker {
	return &Broker{
		subscribers: make(map[int]subscriber),
		bufSize:     bufSize,
	}
}

// Subscribe registers a new subscription.
func (b *Broker) Subscribe() Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := b.nextID
	b.nextID++

	sub := subscriber{
		ch:      make(chan proxy.Event, b.bufSize),
		dropped: new(atomic.Uint64),
		drops:   make(chan struct{}, 1),
	}
	b.subscribers[id] = sub

	return Subscription{
		Events:  sub.ch,
		Drops:   sub.drops,
		dropped: sub.dropped,
		close: func() {
			b.mu.Lock()
			defer b.mu.Unlock()

			if _, ok := b.subscribers[id]; ok {
				delete(b.subscribers, id)
				close(sub.ch)
			}
		},
	}
}

// Publish sends an event to all subscribers.
// If a subscriber's buffer is full, the event is dropped for that subscriber and counted.
func (b *Broker) Publish(ev proxy.Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, sub := range b.subscribers {
		select {
		case sub.ch <- ev:
		default:
			sub.dropped.Add(1)
			select {
			case sub.drops <- struct{}{}:
			default: // a signal is already pending
			}
		}
	}
}

// SubscriberCount returns the number of active subscribers.
func (b *Broker) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.subscribers)
}
