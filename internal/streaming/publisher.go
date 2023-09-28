package streaming

import (
	"encoding/json"
	"log"
	"os"
	"time"
	"wb-test-task/internal/db"

	"github.com/nats-io/stan.go"
)

type Publisher struct {
	sc   *stan.Conn
	name string
}

func NewPublisher(conn *stan.Conn) *Publisher {
	return &Publisher{
		name: "Publisher",
		sc:   conn,
	}
}

func (p *Publisher) Publish() {
	delivery := db.Delivery{
		Name:    "John Doe",
		Phone:   "1234567890",
		Zip:     "12345",
		City:    "New York",
		Address: "123 Main St",
		Region:  "NY",
		Email:   "johndoe@example.com",
	}

	payment := db.Payment{
		Transaction:  "ABCDEF",
		RequestId:    "123456789",
		Currency:     "USD",
		Provider:     "PayPal",
		Amount:       100,
		PaymentDt:    1630000000,
		Bank:         "Bank of America",
		DeliveryCost: 10,
		GoodsTotal:   90,
		CustomFee:    5,
	}

	items := []db.Item{
		{
			ChrtID:      1,
			TrackNumber: "ABC123",
			Price:       50,
			Rid:         "abc123",
			Name:        "Item 1",
			Sale:        0,
			Size:        "M",
			TotalPrice:  50,
			NmID:        1,
			Brand:       "Brand 1",
			Status:      1,
		},
		{
			ChrtID:      2,
			TrackNumber: "DEF456",
			Price:       30,
			Rid:         "def456",
			Name:        "Item 2",
			Sale:        0,
			Size:        "L",
			TotalPrice:  30,
			NmID:        2,
			Brand:       "Brand 2",
			Status:      1,
		},
		{
			ChrtID:      3,
			TrackNumber: "GHI789",
			Price:       20,
			Rid:         "ghi789",
			Name:        "Item 3",
			Sale:        0,
			Size:        "S",
			TotalPrice:  20,
			NmID:        3,
			Brand:       "Brand 3",
			Status:      1,
		},
	}

	order := db.Order{
		OrderUID:          "123456",
		TrackNumber:       "ABC123",
		Entry:             "Test",
		Delivery:          delivery,
		Payment:           payment,
		Items:             items,
		Locale:            "en_US",
		InternalSignature: "abcdef123456",
		CustomerID:        "987654321",
		DeliveryService:   "UPS",
		Shardkey:          "shard1",
		SmID:              123,
		DateCreated:       time.Now(),
		OofShard:          "shard2",
	}

	orderData, err := json.Marshal(order)

	if err != nil {
		log.Printf("%s: json.Marshal error: %v\n", p.name, err)
	}

	ackHandler := func(ackedNuid string, err error) {
		if err != nil {
			log.Printf("%s: error publishing msg id %s: %v\n", p.name, ackedNuid, err.Error())
		} else {
			log.Printf("%s: received ack for msg id: %s\n", p.name, ackedNuid)
		}
	}

	log.Printf("%s: publishing data ...\n", p.name)

	nuid, err := (*p.sc).PublishAsync(os.Getenv("NATS_SUBJECT"), orderData, ackHandler)

	if err != nil {
		log.Printf("%s: error publishing msg %s: %v\n", p.name, nuid, err.Error())
	}
}
