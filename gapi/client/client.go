package main

import (
	"BaleBroker/gapi/pb"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	brokers     []pb.BrokerClient
	connections []*grpc.ClientConn
	mu          sync.Mutex
}

func (c *Client) GetRandomClient() pb.BrokerClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.brokers[rand.Intn(len(c.brokers))]
}

func (c *Client) Close() {
	for _, conn := range c.connections {
		_ = conn.Close()
	}
}

func (c *Client) AddBroker(conn *grpc.ClientConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.brokers = append(c.brokers, pb.NewBrokerClient(conn))
	c.connections = append(c.connections, conn)
}

func main() {
	client := Client{
		brokers:     make([]pb.BrokerClient, 0),
		connections: make([]*grpc.ClientConn, 0),
	}

	for i := 0; i < 100; i++ {
		conn, err := grpc.NewClient("localhost:9090", grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Did not connect: %v", err)
		}
		client.AddBroker(conn)
	}

	defer client.Close()

	var goCount int32 = 1000

	go func() {
		for {
			fmt.Print("Enter the number of requests per second: ")
			var input int
			_, err := fmt.Scanln(&input)
			if err != nil {
				log.Printf("Failed to read input: %v", err)
				continue
			}
			atomic.StoreInt32(&goCount, int32(input))
		}
	}()

	for {
		currentRPS := atomic.LoadInt32(&goCount)
		var i int32
		for i = 0; i < currentRPS; i++ {
			go func() {
				c := client.GetRandomClient()
				req := &pb.PublishRequest{
					Subject:           "ali",
					Body:              []byte("hello"),
					ExpirationSeconds: 60,
				}
				_, err := c.Publish(context.Background(), req)
				if err != nil {
					log.Printf("Error while calling Publish: %v", err)
				}
			}()
		}
		time.Sleep(time.Second)
	}
}
