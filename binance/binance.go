package binance

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	pb "GoBroker/proto"
)

type BinanceValute struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

type Broadcaster interface {
	Broadcast(quote *pb.Quote)
}

func FetchQuotes(b Broadcaster) {
	client := &http.Client{Timeout: 10 * time.Second}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	url := "https://api.binance.com/api/v3/ticker/price"

	for range ticker.C {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Printf("Ошибка создания запроса: %v", err)
			continue
		}

		req.Header.Set("User-Agent", "GoBroker-App/1.0")

		res, err := client.Do(req)
		if err != nil {
			log.Printf("Ошибка получения котировок: %v", err)
			continue
		}

		var binanceRes []BinanceValute
		err = json.NewDecoder(res.Body).Decode(&binanceRes)

		res.Body.Close()

		if err != nil {
			log.Printf("Ошибка декодирования JSON: %v", err)
			continue
		}

		for _, valute := range binanceRes {
			price, err := strconv.ParseFloat(valute.Price, 64)
			if err != nil {
				continue
			}

			symbol := valute.Symbol
			if strings.HasSuffix(symbol, "USDT") {
				symbol = strings.TrimSuffix(symbol, "USDT") + "/USDT"
			}

			quote := &pb.Quote{
				Symbol:    symbol,
				Price:     price,
				Timestamp: time.Now().Unix(),
				Source:    "binance",
			}

			b.Broadcast(quote)
		}
	}
}
