package main

import (
	"fmt"
	"os"
	"os/signal"
	"wb-test-task/api"
	"wb-test-task/cmd/config"
	"wb-test-task/internal/db"
	"wb-test-task/internal/streaming"
)

func main() {
	config.SetupConfig()
	dbObject := db.NewDB()
	csh := db.NewCache(dbObject)
	sh := streaming.NewStreamingHandler(dbObject)

	myApi := api.NewApi(csh)

	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for range signalChan {
			fmt.Printf("\nReceived an interrupt, unsubscribing and closing connection...\n\n")
			csh.Finish()
			sh.Finish()
			myApi.Finish()

			cleanupDone <- true
		}
	}()
	<-cleanupDone
}
