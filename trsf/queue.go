package trsf

import "sync"

type trigger struct {
	notify chan struct{}
}

func newTrigger() *trigger {
	return &trigger{notify: make(chan struct{}, 1)}
}

type withTriggerQueue[T any] struct {
	m     sync.Mutex
	set   map[*T]struct{}
	queue []*T
	*trigger
}

func newWithTriggerQueue[T any]() *withTriggerQueue[T] {
	return &withTriggerQueue[T]{
		trigger: newTrigger(),
	}
}

func (q *withTriggerQueue[T]) Len() int {
	q.m.Lock()
	defer q.m.Unlock()
	return len(q.queue)
}

func (q *withTriggerQueue[T]) Push(s *T) {
	q.m.Lock()
	defer q.m.Unlock()
	if q.set == nil {
		q.set = make(map[*T]struct{})
	}
	if _, exists := q.set[s]; exists {
		return
	}
	q.set[s] = struct{}{}
	q.queue = append(q.queue, s)
	q.trigger.Notify()
}

func (q *trigger) Notify() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *trigger) Notification() <-chan struct{} {
	return q.notify
}

func (q *withTriggerQueue[T]) Pop() *T {
	q.m.Lock()
	defer q.m.Unlock()
	if len(q.queue) == 0 {
		return nil
	}
	s := q.queue[0]
	q.queue = q.queue[1:]
	delete(q.set, s)
	if len(q.queue) != 0 {
		q.trigger.Notify() // notify again if there are more items
	}
	return s
}
