package main

import (
	"fmt"
	"log"
	"time"

	moex "GoBroker/MOEX"
	binance "GoBroker/binance"
	cbr "GoBroker/cbr"
	pb "GoBroker/proto"
	"GoBroker/web"
)

type server struct {
	pb.UnimplementedMarketDataServiceServer
	hub *Hub
}

func (s *server) SubscribeQuotes(req *pb.SubscribeRequest, stream pb.MarketDataService_SubscribeQuotesServer) error {
	symbolsMap := make(map[string]bool)
	for _, sym := range req.GetSymbols() {
		symbolsMap[sym] = true
	}

	clientID := fmt.Sprintf("client-%d", time.Now().UnixNano())
	client := &Client{
		ID:      clientID,
		Symbols: symbolsMap,
		Send:    make(chan *pb.Quote, 256),
	}

	s.hub.Register(client)
	log.Printf("Клиент %s подключился с подписками %v", clientID, req.GetSymbols())

	defer func() {
		s.hub.Unregister(clientID)
		log.Printf("Клиент %s отключился", clientID)
	}()

	for quote := range client.Send {
		if err := stream.Send(quote); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	webServer := web.NewWebServer()
	go webServer.Start(":8080")
	// hub := NewHub()

	go cbr.FetchQuotes(webServer)
	go moex.FetchQuotes(webServer)
	go binance.FetchQuotes(webServer)

	select {}

	// lis, err := net.Listen("tcp", ":50051")
	// if err != nil {
	// 	log.Fatalf("Не удалось открыть порт :50051: %v", err)
	// }

	// grpcServer := grpc.NewServer()
	// s := &server{hub: hub}

	// pb.RegisterMarketDataServiceServer(grpcServer, s)

	// log.Println("gRPC GoBroker сервер запушен на порту :50051")
	// if err := grpcServer.Serve(lis); err != nil {
	// 	log.Fatalf("Ошибка работы gRPC сервера: %v", err)
	// }
}
