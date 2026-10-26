package broker

import (
	"sync"

	"github.com/proto-tap/sql-tap/proxy"
)

// Broker implements a non-blocking fan-out pub/sub for proxy events.
// Slow subscribers silently drop events to avoid blocking the publisher.
type Broker struct {
	mu          sync.RWMutex
	subscribers map[int]chan proxy.Event
	nextID      int
	bufSize     int
}

func New(bufSize int) *Broker {
	return &Broker{
		subscribers: make(map[int]chan proxy.Event),
		bufSize:     bufSize,
	}
}

// Subscribe returns a channel that receives published events
// and an unsubscribe function. The unsubscribe function is idempotent.
func (b *Broker) Subscribe() (<-chan proxy.Event, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := b.nextID
	b.nextID++

	ch := make(chan proxy.Event, b.bufSize)
	b.subscribers[id] = ch

	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		if _, ok := b.subscribers[id]; ok {
			delete(b.subscribers, id)
			close(ch)
		}
	}
}

// Publish sends an event to all subscribers.
// If a subscriber's buffer is full, the event is dropped for that subscriber.
func (b *Broker) Publish(ev proxy.Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, ch := range b.subscribers {
		select {
		case ch <- ev:
		default:
			// buffer full; drop event for this subscriber
		}
	}
}

// SubscriberCount returns the number of active subscribers.
func (b *Broker) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.subscribers)
}
