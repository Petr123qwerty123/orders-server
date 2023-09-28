package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
)

type DB struct {
	pool *pgxpool.Pool
	csh  *Cache
	name string
}

func NewDB() *DB {
	db := DB{}
	db.Init()
	return &db
}

func (db *DB) SetCacheInstance(csh *Cache) {
	db.csh = csh
}

func (db *DB) GetCacheState(bufSize int) (map[int64]Order, []int64, int, error) {
	buffer := make(map[int64]Order, bufSize)
	queue := make([]int64, bufSize)
	var queueInd int

	query := fmt.Sprintf("SELECT order_id FROM cache WHERE app_key = '%s' ORDER BY id DESC LIMIT %d", os.Getenv("APP_KEY"), bufSize)
	rows, err := db.pool.Query(context.Background(), query)

	if err != nil {
		log.Printf("%v: unable to get order_id from database: %v\n", db.name, err)
	}

	defer rows.Close()

	var oid int64
	for rows.Next() {
		if err := rows.Scan(&oid); err != nil {
			log.Printf("%v: unable to get oid from database row: %v\n", db.name, err)
			return buffer, queue, queueInd, errors.New("unable to get oid from database row")
		}

		queue[queueInd] = oid
		queueInd++

		o, err := db.GetOrderByID(oid)

		if err != nil {
			log.Printf("%v: unable to get order from database: %v\n", db.name, err)
			continue
		}

		buffer[oid] = o
	}

	if queueInd == 0 {
		return buffer, queue, queueInd, errors.New("cache is empty")
	}

	for i := 0; i < queueInd/2; i++ {
		queue[i], queue[queueInd-i-1] = queue[queueInd-i-1], queue[i]
	}

	return buffer, queue, queueInd, nil
}

func (db *DB) GetOrderByID(oid int64) (Order, error) {
	var o Order
	var deliveryIdFk int64
	var paymentIdFk int64

	err := db.pool.QueryRow(context.Background(), `SELECT order_uid, track_number, entry, delivery_id, payment_id, locale, 
       internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard FROM orders WHERE id = $1`, oid).Scan(&o.OrderUID,
		&o.TrackNumber, &o.Entry, &deliveryIdFk, &paymentIdFk, &o.Locale, &o.InternalSignature, &o.CustomerID, &o.DeliveryService, &o.Shardkey,
		&o.SmID, &o.DateCreated, &o.OofShard)
	if err != nil {
		return o, errors.New("unable to get order from database")
	}

	err = db.pool.QueryRow(context.Background(), `SELECT transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost,
	goods_total, custom_fee FROM payments WHERE id = $1`, paymentIdFk).Scan(&o.Payment.Transaction, &o.Payment.RequestId, &o.Payment.Currency,
		&o.Payment.Provider, &o.Payment.Amount, &o.Payment.PaymentDt, &o.Payment.Bank, &o.Payment.DeliveryCost, &o.Payment.GoodsTotal, &o.Payment.CustomFee)
	if err != nil {
		log.Printf("%v: unable to get payment from database: %v\n", db.name, err)
		return o, errors.New("unable to get payment from database")
	}

	err = db.pool.QueryRow(context.Background(), `SELECT name, phone, zip, city, address, region, email FROM deliveries WHERE id = $1`, deliveryIdFk).Scan(&o.Delivery.Name,
		&o.Delivery.Phone, &o.Delivery.Zip, &o.Delivery.City, &o.Delivery.Address, &o.Delivery.Region, &o.Delivery.Email)
	if err != nil {
		log.Printf("%v: unable to get payment from database: %v\n", db.name, err)
		return o, errors.New("unable to get delivery from database")
	}

	rowsItems, err := db.pool.Query(context.Background(), `SELECT item_id FROM order_items WHERE order_id = $1`, oid)
	if err != nil {
		return o, errors.New("unable to get items id list from database")
	}
	defer rowsItems.Close()

	var itemID int64
	for rowsItems.Next() {
		var item Item
		if err := rowsItems.Scan(&itemID); err != nil {
			return o, errors.New("unable to get itemID from database row")
		}

		err = db.pool.QueryRow(context.Background(), `SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, 
    brand, status FROM items WHERE id = $1`, itemID).Scan(&item.ChrtID, &item.TrackNumber, &item.Price, &item.Rid, &item.Name, &item.Sale, &item.Size,
			&item.TotalPrice, &item.NmID, &item.Brand, &item.Status)
		if err != nil {
			return o, errors.New("unable to get item from database")
		}
		o.Items = append(o.Items, item)
	}
	return o, nil
}

func (db *DB) AddOrder(o Order) (int64, error) {
	var lastInsertId int64
	var itemsIds []int64

	tx, err := db.pool.Begin(context.Background())
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())

	for _, item := range o.Items {
		err := tx.QueryRow(context.Background(), `INSERT INTO items (chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id`, item.ChrtID, item.TrackNumber, item.Price, item.Rid, item.Name, item.Sale, item.Size,
			item.TotalPrice, item.NmID, item.Brand, item.Status).Scan(&lastInsertId)
		if err != nil {
			log.Printf("%v: unable to insert data (items): %v\n", db.name, err)
			return -1, err
		}
		itemsIds = append(itemsIds, lastInsertId)
	}

	err = tx.QueryRow(context.Background(), `INSERT INTO payments (transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost,
		 goods_total, custom_fee) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id`, o.Payment.Transaction, o.Payment.RequestId, o.Payment.Currency, o.Payment.Provider,
		o.Payment.Amount, o.Payment.PaymentDt, o.Payment.Bank, o.Payment.DeliveryCost, o.Payment.GoodsTotal, o.Payment.CustomFee).Scan(&lastInsertId)
	if err != nil {
		log.Printf("%v: unable to insert data (payment): %v\n", db.name, err)
		return -1, err
	}
	paymentIdFk := lastInsertId

	err = tx.QueryRow(context.Background(), `INSERT INTO deliveries (name, phone, zip, city, address, region, email) values ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		o.Delivery.Name, o.Delivery.Phone, o.Delivery.Zip, o.Delivery.City, o.Delivery.Address, o.Delivery.Region, o.Delivery.Email).Scan(&lastInsertId)
	if err != nil {
		log.Printf("%v: unable to insert data (delivery): %v\n", db.name, err)
		return -1, err
	}
	deliveryIdFk := lastInsertId

	err = tx.QueryRow(context.Background(), `INSERT INTO orders (order_uid, track_number, entry, delivery_id, payment_id, locale, internal_signature, 
		customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING id`,
		o.OrderUID, o.TrackNumber, o.Entry, deliveryIdFk, paymentIdFk, o.Locale, o.InternalSignature, o.CustomerID, o.DeliveryService,
		o.Shardkey, o.SmID, o.DateCreated, o.OofShard).Scan(&lastInsertId)
	if err != nil {
		log.Printf("%v: unable to insert data (orders): %v\n", db.name, err)
		return -1, err
	}
	orderIdFk := lastInsertId

	for _, itemId := range itemsIds {
		_, err := tx.Exec(context.Background(), `INSERT INTO order_items (order_id, item_id) values ($1, $2)`,
			orderIdFk, itemId)
		if err != nil {
			log.Printf("%v: unable to insert data (order_items): %v\n", db.name, err)
			return -1, err
		}
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return 0, err
	}

	log.Printf("%v: Order successfull added to DB\n", db.name)

	db.csh.SetOrder(orderIdFk, o)
	return orderIdFk, nil
}

func (db *DB) SendOrderIDToCache(oid int64) {
	db.pool.QueryRow(context.Background(), `INSERT INTO cache (order_id, app_key) VALUES ($1, $2)`, oid, os.Getenv("APP_KEY"))
	log.Printf("%v: OrderID successfull added to Cache (DB)\n", db.name)
}

func (db *DB) ClearCache() {
	_, err := db.pool.Exec(context.Background(), `DELETE FROM cache WHERE app_key = $1`, os.Getenv("APP_KEY"))
	if err != nil {
		log.Printf("%v: clear cache error: %s\n", db.name, err)
	}
	log.Printf("%v: cache successfull cleared from database\n", db.name)
}
