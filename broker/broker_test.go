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
	ch, unsub := b.Subscribe()
	defer unsub()

	ev := proxy.Event{ID: "1", Op: proxy.OpQuery, Query: "SELECT 1"}
	b.Publish(ev)

	select {
	case got := <-ch:
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

	ch1, unsub1 := b.Subscribe()
	defer unsub1()
	ch2, unsub2 := b.Subscribe()
	defer unsub2()

	if b.SubscriberCount() != 2 {
		t.Fatalf("expected 2 subscribers, got %d", b.SubscriberCount())
	}

	ev := proxy.Event{ID: "1"}
	b.Publish(ev)

	for _, ch := range []<-chan proxy.Event{ch1, ch2} {
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
	_, unsub := b.Subscribe()

	if b.SubscriberCount() != 1 {
		t.Fatalf("expected 1 subscriber, got %d", b.SubscriberCount())
	}

	unsub()

	if b.SubscriberCount() != 0 {
		t.Fatalf("expected 0 subscribers, got %d", b.SubscriberCount())
	}

	// idempotent
	unsub()
	if b.SubscriberCount() != 0 {
		t.Fatalf("expected 0 subscribers after double unsub, got %d", b.SubscriberCount())
	}
}

func TestBroker_SlowSubscriberDropsEvents(t *testing.T) {
	t.Parallel()

	b := broker.New(1) // buffer size 1
	ch, unsub := b.Subscribe()
	defer unsub()

	// Fill the buffer.
	b.Publish(proxy.Event{ID: "1"})
	// This should be dropped, not block.
	b.Publish(proxy.Event{ID: "2"})

	select {
	case got := <-ch:
		if got.ID != "1" {
			t.Fatalf("expected event 1, got %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	select {
	case ev := <-ch:
		t.Fatalf("expected no more events, got %+v", ev)
	default:
	}
}

func TestBroker_ConcurrentPublish(t *testing.T) {
	t.Parallel()

	b := broker.New(256)
	ch, unsub := b.Subscribe()
	defer unsub()

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
		case <-ch:
			count++
		default:
			if count != n {
				t.Fatalf("expected %d events, got %d", n, count)
			}
			return
		}
	}
}
