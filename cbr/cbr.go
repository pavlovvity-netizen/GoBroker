package cbr

import (
	"bytes"
	"encoding/xml"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	pb "GoBroker/proto"

	"golang.org/x/text/encoding/charmap"
)

type CBRValute struct {
	CharCode string  `xml:"CharCode"`
	Value    string  `xml:"Value"`
	Nominal  float64 `xml:"Nominal"`
}

type CBRResponse struct {
	Valutes []CBRValute `xml:"Valute"`
}

type Broadcaster interface {
	Broadcast(quote *pb.Quote)
}

func FetchQuotes(b Broadcaster) {
	client := &http.Client{Timeout: 10 * time.Second}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		req, err := http.NewRequest("GET", "https://www.cbr.ru/scripts/XML_daily.asp", nil)
		if err != nil {
			log.Printf("Ошибка создания запроса: %v", err)
			continue
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

		res, err := client.Do(req)
		if err != nil {
			log.Printf("Ошибка запроса к ЦБ РФ: %v", err)
			continue
		}

		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			log.Printf("Ошибка чтения ответа: %v", err)
			continue
		}

		// 1. Конвертируем байты из Windows-1251 в UTF-8
		utf8Body, err := charmap.Windows1251.NewDecoder().Bytes(body)
		if err != nil {
			log.Printf("Ошибка конвертации кодировки: %v", err)
			continue
		}

		// 2. Меняем объявление кодировки в заголовке XML на utf-8,
		// чтобы xml.Unmarshal не ругался на отсутствие CharsetReader
		utf8Body = bytes.Replace(utf8Body, []byte(`encoding="windows-1251"`), []byte(`encoding="utf-8"`), 1)
		utf8Body = bytes.Replace(utf8Body, []byte(`encoding="WINDOWS-1251"`), []byte(`encoding="utf-8"`), 1)

		// 3. Парсим XML
		var cbrRes CBRResponse
		if err := xml.Unmarshal(utf8Body, &cbrRes); err != nil {
			log.Printf("Ошибка парсинга XML: %v", err)
			continue
		}

		// 4. Рассылаем котировки
		for _, valute := range cbrRes.Valutes {
			priceStr := strings.ReplaceAll(valute.Value, ",", ".")
			price, err := strconv.ParseFloat(priceStr, 64)
			if err != nil {
				continue
			}

			finalPrice := price / valute.Nominal

			quote := &pb.Quote{
				Symbol:    valute.CharCode + "/RUB",
				Price:     finalPrice,
				Timestamp: time.Now().Unix(),
				Source:    "cbr",
			}

			b.Broadcast(quote)
		}
	}
}
