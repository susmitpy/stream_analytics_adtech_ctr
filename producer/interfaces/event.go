package interfaces

type Event interface {
	Key() []byte
	Topic() string
	Value() ([]byte, error)
}