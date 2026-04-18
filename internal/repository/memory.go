package repository

import (
	"log"

	"github.com/wire-speed-events/internal/models"
)

type MemoryEventRepository struct {
	events map[string]*models.Event
}

func NewMemoryRepository() *MemoryEventRepository {
	return &MemoryEventRepository{
		events: make(map[string]*models.Event),
	}
}

func (r *MemoryEventRepository) Save(event *models.Event) error {
	r.events[event.ID] = event
	log.Printf("Repository saved: [ID=%s, Type=%s]", event.ID, event.Type)
	return nil
}
