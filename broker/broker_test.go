package broker_test

import (
	"sync"
	"testing"
	"time"

	"github.com/proto-tap/sql-tap/broker"
	"github.com/proto-tap/sql-tap/proxy"
)

func TestBroker_PublishSubscribe(t *testing.T) {
	t.Parallel()

	b := broker.New(8)
	sub := b.Subscribe()
	defer sub.Close()

	ev := proxy.Event{ID: "1", Op: proxy.OpQuery, Query: "SELECT 1"}
	b.Publish(ev)

	select {
	case got := <-sub.Events:
		if got.ID != "1" || got.Query != "SELECT 1" {
			t.Fatalf("unexpected event: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestBroker_MultipleSubscribers(t *testing.T) {
	t.Parallel()

	b := broker.New(8)

	sub1 := b.Subscribe()
	defer sub1.Close()
	sub2 := b.Subscribe()
	defer sub2.Close()

	if b.SubscriberCount() != 2 {
		t.Fatalf("expected 2 subscribers, got %d", b.SubscriberCount())
	}

	ev := proxy.Event{ID: "1"}
	b.Publish(ev)

	for _, ch := range []<-chan proxy.Event{sub1.Events, sub2.Events} {
		select {
		case got := <-ch:
			if got.ID != "1" {
				t.Fatalf("unexpected event: %+v", got)
			}
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for event")
		}
	}
}

func TestBroker_Unsubscribe(t *testing.T) {
	t.Parallel()

	b := broker.New(8)
	sub := b.Subscribe()

	if b.SubscriberCount() != 1 {
		t.Fatalf("expected 1 subscriber, got %d", b.SubscriberCount())
	}

	sub.Close()

	if b.SubscriberCount() != 0 {
		t.Fatalf("expected 0 subscribers, got %d", b.SubscriberCount())
	}

	// idempotent
	sub.Close()
	if b.SubscriberCount() != 0 {
		t.Fatalf("expected 0 subscribers after double unsub, got %d", b.SubscriberCount())
	}
}

func TestBroker_SlowSubscriberDropsEvents(t *testing.T) {
	t.Parallel()

	b := broker.New(1) // buffer size 1
	sub := b.Subscribe()
	defer sub.Close()
	other := b.Subscribe()
	defer other.Close()

	// Fill the buffer.
	b.Publish(proxy.Event{ID: "1"})
	<-other.Events // other keeps up, so only sub drops
	// These should be dropped, not block.
	b.Publish(proxy.Event{ID: "2"})
	b.Publish(proxy.Event{ID: "3"})

	if got := sub.Dropped(); got != 2 {
		t.Errorf("Dropped() = %d, want 2", got)
	}
	if got := other.Dropped(); got != 1 {
		t.Errorf("other.Dropped() = %d, want 1 (its buffer holds event 2)", got)
	}

	select {
	case got := <-sub.Events:
		if got.ID != "1" {
			t.Fatalf("expected event 1, got %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	select {
	case ev := <-sub.Events:
		t.Fatalf("expected no more events, got %+v", ev)
	default:
	}
}

func TestBroker_DropsSignalsAfterCounting(t *testing.T) {
	t.Parallel()

	b := broker.New(1)
	sub := b.Subscribe()
	defer sub.Close()

	b.Publish(proxy.Event{ID: "1"}) // fills the buffer
	select {
	case <-sub.Drops:
		t.Fatal("Drops signaled without a drop")
	default:
	}

	b.Publish(proxy.Event{ID: "2"})
	b.Publish(proxy.Event{ID: "3"})
	select {
	case <-sub.Drops:
		if got := sub.Dropped(); got != 2 {
			t.Errorf("Dropped() = %d on signal, want 2", got)
		}
	default:
		t.Fatal("Drops not signaled after a drop")
	}
}

func TestBroker_ConcurrentPublish(t *testing.T) {
	t.Parallel()

	b := broker.New(256)
	sub := b.Subscribe()
	defer sub.Close()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			b.Publish(proxy.Event{ID: string(rune('A' + i%26))})
		}()
	}
	wg.Wait()

	count := 0
	for {
		select {
		case <-sub.Events:
			count++
		default:
			if count != n {
				t.Fatalf("expected %d events, got %d", n, count)
			}
			return
		}
	}
}

func TestSubscription_ZeroValue(t *testing.T) {
	t.Parallel()

	var sub broker.Subscription
	sub.Close()
	if got := sub.Dropped(); got != 0 {
		t.Errorf("Dropped() = %d, want 0", got)
	}
}
