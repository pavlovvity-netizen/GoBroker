package main

import (
	"sync"

	pb "GoBroker/proto"
)

type Client struct {
	ID      string
	Symbols map[string]bool
	Send    chan *pb.Quote
}

type Hub struct {
	clients map[string]*Client
	mu      sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[string]*Client),
	}
}

func (h *Hub) Register(client *Client) {
	h.mu.Lock()
	h.clients[client.ID] = client
	h.mu.Unlock()
}

func (h *Hub) Unregister(clientID string) {
	h.mu.Lock()
	if client, ok := h.clients[clientID]; ok {
		close(client.Send)
		delete(h.clients, clientID)
	}
	h.mu.Unlock()
}

func (h *Hub) Broadcast(quote *pb.Quote) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, client := range h.clients {
		if client.Symbols[quote.Symbol] {
			select {
			case client.Send <- quote:
			default:
			}
		}
	}
}
