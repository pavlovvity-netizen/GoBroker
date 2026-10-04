package moex

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	pb "GoBroker/proto"
)

type MOEXResponse struct {
	MarketData struct {
		Columns []string        `json:"columns"`
		Data    [][]interface{} `json:"data"`
	} `json:"marketdata"`
}

type Broadcaster interface {
	Broadcast(quote *pb.Quote)
}

func FetchQuotes(b Broadcaster) {
	client := &http.Client{Timeout: 10 * time.Second}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	url := "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json?iss.only=marketdata"

	for range ticker.C {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Printf("Ошибка создания запроса: %v", err)
			continue
		}

		req.Header.Set("User-Agent", "GoBroker-App/1.0")

		res, err := client.Do(req)
		if err != nil {
			log.Printf("Ошибка запроса к MOEX: %v", err)
			continue
		}

		var MOEXres MOEXResponse
		err = json.NewDecoder(res.Body).Decode(&MOEXres)

		res.Body.Close()

		if err != nil {
			log.Printf("Ошибка декодирования ответа: %v", err)
			continue
		}

		secIdx, lastIdx := -1, -1

		for i, col := range MOEXres.MarketData.Columns {
			if col == "SECID" {
				secIdx = i
			}
			if col == "LAST" {
				lastIdx = i
			}
		}

		if secIdx == -1 || lastIdx == -1 {
			log.Printf("Ошибка: не найдены колонки SECID или LAST в ответе MOEX")
			continue
		}

		for _, row := range MOEXres.MarketData.Data {
			if len(row) <= lastIdx || row[secIdx] == nil || row[lastIdx] == nil {
				continue
			}

			secID, ok := row[secIdx].(string)
			if !ok {
				continue
			}

			var price float64
			switch v := row[lastIdx].(type) {
			case float64:
				price = v
			case string:
				parsed, err := strconv.ParseFloat(v, 64)
				if err != nil {
					continue
				}
				price = parsed
			default:
				continue
			}

			quote := &pb.Quote{
				Symbol:    secID + "/RUB",
				Price:     price,
				Timestamp: time.Now().Unix(),
				Source:    "moex",
			}

			b.Broadcast(quote)
		}
	}
}
