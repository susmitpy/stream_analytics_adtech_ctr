package models

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/susmitpy/stream_analytics_ctr/producer/interfaces"
)

type KafkaProducer struct {
	writers map[string]*kafka.Writer
}

// Interface: Producer
func (kp *KafkaProducer) Write(ctx context.Context, e interfaces.Event) error {
	val, err := e.Value()
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:  e.Key(),
		Value: val,
	}

	return kp.writers[e.Topic()].WriteMessages(ctx, msg)
}

func (kp *KafkaProducer) Close() []error {
	var errs []error
	for _, writer := range kp.writers {
		if err := writer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func NewKafkaProducer(brokers []string, topics ...string) *KafkaProducer {
	writers := make(map[string]*kafka.Writer)
	for _, topic := range topics {
		writers[topic] = &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			RequiredAcks: kafka.RequireAll,
			Balancer: &kafka.Murmur2Balancer{},
		}
	}
	return &KafkaProducer{
		writers: writers,
	}

}