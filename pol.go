package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Структуры данных согласно вашему запросу
type Transaction struct {
	Amount          int    `json:"amount"`
	Currency        string `json:"currency"`
	TransactionType string `json:"transaction_type"`
}

type Payer struct {
	AccountID int `json:"account_id"`
}

type Payee struct {
	MerchantID   int    `json:"merchant_id"`
	MerchantName string `json:"merchant_name"`
	Country      string `json:"country"`
}

type Context struct {
	Channel   string `json:"channel"`
	DeviceID  string `json:"device_id"`
	IP        string `json:"ip"`
	UserAgent string `json:"user-agent"`
}

type Event struct {
	EventID     string      `json:"event_id"`
	EventTime   string      `json:"event_time"`
	Direction   string      `json:"direction"`
	Transaction Transaction `json:"transaction"`
	Payer       Payer       `json:"payer"`
	Payee       Payee       `json:"payee"`
	Context     Context     `json:"context"`
}

// Генераторы случайных данных
var (
	merchantNames = []string{
		"Postfix", "SwiftPay", "GlobalTrade", "FinTech Corp", "EcoBank",
		"DigitalWallet", "PayFlow", "CashMaster", "TradeZone", "QuickPay",
		"StarMerchant", "WorldPay", "SecureTrade", "PrimeBank", "ElitePay",
	}
	countries = []string{
		"UK", "USA", "DE", "FR", "IT", "ES", "NL", "BE", "CH", "AT",
		"PL", "CZ", "HU", "RO", "BG", "GR", "PT", "SE", "NO", "DK",
		"FI", "IE", "LU", "MT", "CY", "LV", "LT", "EE", "SK", "SI",
	}
	channels = []string{"mobile_app", "web", "pos", "atm", "api"}
	userAgents = []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
		"Mozilla/5.0 (Linux; Android 13; SM-G991B) Chrome/119.0.6045.163",
		"opera/34.341.10",
		"curl/7.68.0",
		"python-requests/2.28.1",
		"Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/121.0",
		"PostmanRuntime/7.32.3",
		"okhttp/4.11.0",
	}
	currencies = []string{"GBP", "USD", "EUR", "JPY", "CHF", "AUD", "CAD", "CNY"}
	transactionTypes = []string{"authorization", "capture", "sale", "refund", "void"}
	directions = []string{"inbound", "outbound"}
)

func generateRandomEvent(id int) Event {
	// Генерация случайного event_id (8 символов)
	eventID := generateRandomString(8)
	
	// Случайная сумма от 100 до 1000000
	amount := rand.Intn(1000000-100) + 100
	
	// Случайный account_id от 1000 до 9999
	accountID := rand.Intn(9000) + 1000
	
	// Случайный merchant_id от 100000 до 999999
	merchantID := rand.Intn(900000) + 100000
	
	return Event{
		EventID:   eventID,
		EventTime: time.Now().Format("2006-01-02 15:04:05"),
		Direction: directions[rand.Intn(len(directions))],
		Transaction: Transaction{
			Amount:          amount,
			Currency:        currencies[rand.Intn(len(currencies))],
			TransactionType: transactionTypes[rand.Intn(len(transactionTypes))],
		},
		Payer: Payer{
			AccountID: accountID,
		},
		Payee: Payee{
			MerchantID:   merchantID,
			MerchantName: merchantNames[rand.Intn(len(merchantNames))],
			Country:      countries[rand.Intn(len(countries))],
		},
		Context: Context{
			Channel:   channels[rand.Intn(len(channels))],
			DeviceID:  generateRandomString(6),
			IP:        generateRandomIP(),
			UserAgent: userAgents[rand.Intn(len(userAgents))],
		},
	}
}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func generateRandomIP() string {
	return fmt.Sprintf("%d.%d.%d.%d",
		rand.Intn(256),
		rand.Intn(256),
		rand.Intn(256),
		rand.Intn(256),
	)
}

func main() {
	url := "http://localhost:8082/payment/add" // Замените на ваш URL
	
	const totalRequests = 10000
	workers := runtime.NumCPU() * 2
	
	fmt.Printf("🚀 Начинаем отправку %d запросов\n", totalRequests)
	fmt.Printf("📍 URL: %s\n", url)
	fmt.Printf("⚡ Параллельных воркеров: %d\n", workers)
	fmt.Println(strings.Repeat("=", 60))
	
	startTime := time.Now()
	
	// Настройка HTTP клиента
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			MaxConnsPerHost:     100,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  false,
		},
	}
	
	var (
		successCount   int32
		errorCount     int32
		statusCounts   = make(map[int]*int32)
		mu             sync.Mutex
		wg             sync.WaitGroup
	)
	
	// Канал для задач
	tasks := make(chan int, totalRequests)
	
	// Запуск воркеров
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for idx := range tasks {
				// Генерируем случайные данные для запроса
				event := generateRandomEvent(idx + 1)
				
				jsonData, err := json.Marshal(event)
				if err != nil {
					atomic.AddInt32(&errorCount, 1)
					fmt.Printf("❌ Worker %d: Ошибка сериализации #%d: %v\n", workerID, idx+1, err)
					continue
				}
				
				// Создаем запрос
				req, err := http.NewRequest("POST", url, bytes.NewReader(jsonData))
				if err != nil {
					atomic.AddInt32(&errorCount, 1)
					fmt.Printf("❌ Worker %d: Ошибка создания запроса #%d: %v\n", workerID, idx+1, err)
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Accept", "application/json")
				
				// Отправляем запрос
				resp, err := client.Do(req)
				if err != nil {
					atomic.AddInt32(&errorCount, 1)
					fmt.Printf("❌ Worker %d: Ошибка запроса #%d: %v\n", workerID, idx+1, err)
					continue
				}
				
				// Читаем и закрываем тело ответа
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				
				// Считаем статусы
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					atomic.AddInt32(&successCount, 1)
				} else {
					atomic.AddInt32(&errorCount, 1)
				}
				
				// Сбор статистики по статусам
				mu.Lock()
				if _, ok := statusCounts[resp.StatusCode]; !ok {
					statusCounts[resp.StatusCode] = new(int32)
				}
				atomic.AddInt32(statusCounts[resp.StatusCode], 1)
				mu.Unlock()
				
				// Вывод прогресса каждые 1000 запросов
				processed := int(atomic.LoadInt32(&successCount) + atomic.LoadInt32(&errorCount))
				if processed%1000 == 0 && processed > 0 {
					fmt.Printf("📊 Прогресс: %d/%d запросов (%.1f%%)\n", 
						processed, totalRequests, float64(processed)/float64(totalRequests)*100)
				}
			}
		}(w)
	}
	
	// Отправка задач
	for i := 0; i < totalRequests; i++ {
		tasks <- i
	}
	close(tasks)
	
	// Ожидание завершения всех воркеров
	wg.Wait()
	
	elapsed := time.Since(startTime)
	successCountFinal := atomic.LoadInt32(&successCount)
	errorCountFinal := atomic.LoadInt32(&errorCount)
	
	// Вывод результатов
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📈 РЕЗУЛЬТАТЫ:")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("✅ Успешных запросов:  %d (%.1f%%)\n", 
		successCountFinal, float64(successCountFinal)/float64(totalRequests)*100)
	fmt.Printf("❌ Ошибок:            %d (%.1f%%)\n", 
		errorCountFinal, float64(errorCountFinal)/float64(totalRequests)*100)
	fmt.Printf("⏱️  Общее время:       %.2f секунд\n", elapsed.Seconds())
	fmt.Printf("⚡ Запросов в секунду: %.1f\n", float64(totalRequests)/elapsed.Seconds())
	fmt.Printf("📊 Среднее время:      %.3f секунд\n", elapsed.Seconds()/float64(totalRequests))
	
	// Статистика по HTTP статусам
	fmt.Println("\n📊 HTTP Статусы:")
	for status, count := range statusCounts {
		cnt := atomic.LoadInt32(count)
		fmt.Printf("   %d: %d (%.1f%%)\n", 
			status, cnt, float64(cnt)/float64(totalRequests)*100)
	}
	fmt.Println(strings.Repeat("=", 60))
	
	// Пример первого и последнего запроса для проверки
	fmt.Println("\n📝 Пример сгенерированных данных (первый запрос):")
	firstEvent := generateRandomEvent(1)
	firstJSON, _ := json.MarshalIndent(firstEvent, "", "  ")
	fmt.Println(string(firstJSON))
}
