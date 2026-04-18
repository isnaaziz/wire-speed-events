package service

import (
	"github.com/wire-speed-events/internal/domain"
	"github.com/wire-speed-events/internal/models"
)

type Orchestrator struct {
	repo      domain.EventRepository
	publisher domain.EventPublisher
}

func NewOrchestrator(repo domain.EventRepository, publisher domain.EventPublisher) *Orchestrator {
	return &Orchestrator{
		repo:      repo,
		publisher: publisher,
	}
}

func (o *Orchestrator) HandleEvent(event *models.Event) error {
	if err := o.repo.Save(event); err != nil {
		return err
	}

	return o.publisher.Publish(event)
}
