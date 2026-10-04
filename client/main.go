package main

import (
	"context"
	"io"
	"log"

	pb "GoBroker/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Ошибка подключения: %v", err)
	}
	defer conn.Close()

	client := pb.NewMarketDataServiceClient(conn)

	req := &pb.SubscribeRequest{
		Symbols: []string{
			"AUD/RUB",
			"GBP/RUB",
			"SBERP/RUB",
			"GAZP/RUB",
			"BTC/USDT",
			"ETH/USDT",
		},
	}

	stream, err := client.SubscribeQuotes(context.Background(), req)
	if err != nil {
		log.Fatalf("Ошибка подписки: %v", err)
	}

	log.Println("Успешно подписались! Ожидаем котировки...")

	for {
		quote, err := stream.Recv()
		if err == io.EOF {
			log.Println("Поток закрыт сервером")
			break
		}
		if err != nil {
			log.Fatalf("Ошибка получения котировок %v", err)
		}

		log.Printf(" [ПОТОК] %s: %.2f (Timestamp: %d)", quote.GetSymbol(), quote.GetPrice(), quote.GetTimestamp())
	}
}
