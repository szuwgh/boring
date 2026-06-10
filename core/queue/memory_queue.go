package queue

import "errors"

var (
	ErrQueueFull   = errors.New("queue is full")
	ErrQueueClosed = errors.New("queue is closed")
)

type MemoryQueue struct {
	data chan []byte
	size int
}

func NewMemoryQueue(size int) *MemoryQueue {
	return &MemoryQueue{
		data: make(chan []byte, size),
		size: size,
	}
}

// Enqueue is non-blocking. Returns ErrQueueFull if the buffer is full.
func (q *MemoryQueue) Enqueue(data []byte) error {
	select {
	case q.data <- data:
		return nil
	default:
		return ErrQueueFull
	}
}

// Dequeue blocks until data is available. Returns ErrQueueClosed after
// all buffered items have been drained and the channel is closed.
func (q *MemoryQueue) Dequeue() ([]byte, error) {
	val, ok := <-q.data
	if !ok {
		return nil, ErrQueueClosed
	}
	return val, nil
}

func (q *MemoryQueue) Size() int {
	return len(q.data)
}

func (q *MemoryQueue) Close() error {
	close(q.data)
	return nil
}
