package main

import (
	"BaleBroker/gapi/pb"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"sync/atomic"
	"time"
)

func main() {
	conn, err := grpc.NewClient("localhost:9090", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	var requestPerSec int32 = 10

	go func() {
		for {
			fmt.Print("Enter the number of requests per second: ")
			var input int
			_, err := fmt.Scanln(&input)
			if err != nil {
				log.Printf("Failed to read input: %v", err)
				continue
			}
			atomic.StoreInt32(&requestPerSec, int32(input))
		}
	}()

	for {
		for {
			currentRPS := atomic.LoadInt32(&requestPerSec)
			var i int32
			for i = 0; i < currentRPS; i++ {
				go func() {
					client := pb.NewBrokerClient(conn)

					req := &pb.PublishRequest{
						Subject:           "ali",
						Body:              []byte("hello"),
						ExpirationSeconds: 60,
					}
					_, err := client.Publish(context.Background(), req)
					if err != nil {
						log.Printf("Error while calling Publish: %v", err)
					}
				}()
			}
			time.Sleep(time.Second)
		}
	}
}
