package kafka

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	events "github.com/wire-speed-events/gen/pb"
	"github.com/wire-speed-events/internal/models"
	"google.golang.org/protobuf/proto"
)

type EventProducer struct {
	writer *kafka.Writer
}

func NewProducer(cfg Config) *EventProducer {
	mechanism := plain.Mechanism{
		Username: cfg.Username,
		Password: cfg.Password,
	}

	return &EventProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(cfg.Brokers...),
			Topic:    cfg.Topic,
			Balancer: &kafka.LeastBytes{},
			Transport: &kafka.Transport{
				SASL: mechanism,
			},
		},
	}
}

func (p *EventProducer) Publish(event *models.Event) error {
	protoEvent := &events.Event{
		Id:        event.ID,
		Type:      event.Type,
		Payload:   event.Payload,
		Timestamp: event.Timestamp.UnixNano(),
	}

	payload, err := proto.Marshal(protoEvent)
	if err != nil {
		return err
	}

	log.Printf("Publishing binary protobuf: len=%d bytes, hex=%x", len(payload), payload)

	return p.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(event.ID),
			Value: payload,
		},
	)
}

func (p *EventProducer) Close() error {
	return p.writer.Close()
}
