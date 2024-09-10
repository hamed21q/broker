package main

import (
	"BaleBroker/gapi/pb"
	"context"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"sync"
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
		conn, err := grpc.NewClient("localhost:8080", grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Did not connect: %v", err)
		}
		client.AddBroker(conn)
	}

	defer client.Close()

	var goCount = 15000
	var wg sync.WaitGroup
	wg.Add(goCount)
	for i := 0; i < goCount; i++ {
		go func() {
			for {
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
				time.Sleep(time.Second / 20)
			}
		}()
	}
	wg.Wait()
}
