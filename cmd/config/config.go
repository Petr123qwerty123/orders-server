package config

import (
	"os"
)

func SetupConfig() {
	// Database settings
	os.Setenv("DB_USERNAME", "postgres")
	os.Setenv("DB_PASSWORD", "admin")
	os.Setenv("DB_HOST", "0.0.0.0:5432")
	os.Setenv("DB_NAME", "wb")

	os.Setenv("DB_POOL_MAXCONN", "5")
	os.Setenv("DB_POOL_MAXCONN_LIFETIME", "300")

	// NATS-Streaming settings
	os.Setenv("NATS_HOSTS", "0.0.0.0:4222")
	os.Setenv("NATS_CLUSTER_ID", "test-cluster")
	os.Setenv("NATS_CLIENT_ID", "clientId")
	os.Setenv("NATS_SUBJECT", "test-subject")
	os.Setenv("NATS_DURABLE_NAME", "test-durable-name")
	os.Setenv("NATS_ACK_WAIT_SECONDS", "30")

	// Cache settings
	os.Setenv("CACHE_SIZE", "10")
	os.Setenv("APP_KEY", "WB-1")
}
