package queue

type Queue interface {
	Enqueue(data []byte) error
	Dequeue() ([]byte, error)
	Size() int
	Close() error
}
