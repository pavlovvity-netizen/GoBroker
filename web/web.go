package web

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	pb "GoBroker/proto"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	Conn    *websocket.Conn
	Symbols map[string]bool
}

type WebServer struct {
	clients   map[*websocket.Conn]*Client
	broadcast chan *pb.Quote
	mutex     sync.Mutex
}

func NewWebServer() *WebServer {
	return &WebServer{
		clients:   make(map[*websocket.Conn]*Client),
		broadcast: make(chan *pb.Quote, 100),
	}
}

func (s *WebServer) Broadcast(quote *pb.Quote) {
	s.broadcast <- quote
}

func (s *WebServer) Start(port string) {
	http.Handle("/", http.FileServer(http.Dir("./web/static")))
	http.HandleFunc("/ws", s.handleConnections)

	go s.handleMessages()

	log.Printf("[Web] Дашборд запущен на http://localhost%s", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal("[Web] Ошибка запуска веб-сервера:", err)
	}
}

func (s *WebServer) handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("[Web] Ошибка Upgrade:", err)
		return
	}
	defer ws.Close()

	client := &Client{
		Conn:    ws,
		Symbols: make(map[string]bool),
	}

	s.mutex.Lock()
	s.clients[ws] = client
	s.mutex.Unlock()

	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			s.mutex.Lock()
			delete(s.clients, ws)
			s.mutex.Unlock()
			break
		}

		var req struct {
			Action  string   `json:"action"`
			Symbols []string `json:"symbols"`
		}

		if err := json.Unmarshal(message, &req); err == nil {
			s.mutex.Lock()
			for _, sym := range req.Symbols {
				switch req.Action {
				case "subscribe", "":
					client.Symbols[sym] = true
				case "unsubscribe":
					delete(client.Symbols, sym)
				}
			}
			s.mutex.Unlock()
			log.Printf("[Web] Подписки клиента обновлены: %v", client.Symbols)
		}
	}
}

func (s *WebServer) handleMessages() {
	for quote := range s.broadcast {
		msg, err := json.Marshal(quote)
		if err != nil {
			continue
		}

		s.mutex.Lock()
		for ws, client := range s.clients {
			if len(client.Symbols) == 0 || client.Symbols[quote.Symbol] {
				if err := ws.WriteMessage(websocket.TextMessage, msg); err != nil {
					ws.Close()
					delete(s.clients, ws)
				}
			}
		}
		s.mutex.Unlock()
	}
}
