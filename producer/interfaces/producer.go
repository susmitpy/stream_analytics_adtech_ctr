package interfaces

import "context"

type Producer interface {
	Write(ctx context.Context, e Event) error
	Close() []error
}