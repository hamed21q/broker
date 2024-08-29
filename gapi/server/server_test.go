package server

import (
	"BaleBroker/broker"
	db "BaleBroker/db/memory"
	"BaleBroker/gapi/pb"
	"BaleBroker/pkg"
	"context"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"net"
	"testing"
	"time"
)

var baleBroker *broker.BaleBroker

func server(ctx context.Context) (pb.BrokerClient, func()) {
	memoryDb := db.NewMemoryDB()
	identifier := pkg.NewSequentialIdentifier()
	baleBroker = broker.NewBaleBroker(memoryDb, identifier)
	brokerServer := NewServer(baleBroker)

	buffer := 101024 * 1024
	lis := bufconn.Listen(buffer)

	baseServer := grpc.NewServer()
	pb.RegisterBrokerServer(baseServer, brokerServer)
	go func() {
		if err := baseServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()

	conn, err := grpc.DialContext(ctx, "",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("error connecting to server: %v", err)
	}

	closer := func() {
		err := lis.Close()
		if err != nil {
			log.Printf("error closing listener: %v", err)
		}
		baseServer.Stop()
	}

	client := pb.NewBrokerClient(conn)

	return client, closer
}

func TestPublish(t *testing.T) {
	ctx := context.Background()

	client, closer := server(ctx)
	defer closer()

	type expectation struct {
		out     *pb.PublishResponse
		errCode codes.Code
	}

	tests := map[string]struct {
		in       *pb.PublishRequest
		expected expectation
		prepare  func(t *testing.T)
	}{
		"Must_Success": {
			in: &pb.PublishRequest{
				Body:    []byte("hello hamed"),
				Subject: "bale",
			},
			expected: expectation{
				out:     &pb.PublishResponse{Id: 1},
				errCode: 0,
			},
			prepare: func(t *testing.T) {

			},
		},
		"Broker_Closed": {
			in: &pb.PublishRequest{
				Body:    []byte("hello hamed"),
				Subject: "bale",
			},
			expected: expectation{
				out:     nil,
				errCode: codes.DeadlineExceeded,
			},
			prepare: func(t *testing.T) {
				err := baleBroker.Close()
				require.NoError(t, err)
			},
		},
	}

	for scenario, tc := range tests {
		t.Run(scenario, func(t *testing.T) {
			tc.prepare(t)
			out, err := client.Publish(ctx, tc.in)
			if err != nil {
				actualStatus, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, actualStatus.Code(), tc.expected.errCode)
			} else {
				require.Equal(t, tc.expected.out.Id, out.Id)
			}
		})
	}
}

func TestSubscribe(t *testing.T) {
	ctx := context.Background()

	client, closer := server(ctx)
	defer closer()

	type expectation struct {
		out     []*pb.MessageResponse
		errCode codes.Code
	}

	tests := map[string]struct {
		in       *pb.SubscribeRequest
		expected expectation
		prepare  func(t *testing.T)
	}{
		"Must_Success": {
			in: &pb.SubscribeRequest{
				Subject: "bale",
			},
			expected: expectation{
				out: []*pb.MessageResponse{
					{
						Body: []byte("hello"),
					},
				},
				errCode: 0,
			},
			prepare: func(t *testing.T) {
				_, err := client.Publish(ctx, &pb.PublishRequest{Body: []byte("hello"), Subject: "bale"})
				require.NoError(t, err)
			},
		},
	}

	for scenario, tc := range tests {
		t.Run(scenario, func(t *testing.T) {
			out, err := client.Subscribe(ctx, tc.in)
			tc.prepare(t)

			var outs []*pb.MessageResponse

			for i := 0; i < 1; i++ {
				o, err := out.Recv()
				require.NoError(t, err)
				outs = append(outs, o)
			}

			if err != nil {
				actualStatus, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, actualStatus.Code(), tc.expected.errCode)
			} else {
				for _, o := range outs {
					require.Equal(t, string(o.GetBody()), "hello")
				}
			}
		})
	}
}

func TestFetch(t *testing.T) {
	ctx := context.Background()

	client, closer := server(ctx)
	defer closer()
	msg, _ := client.Publish(ctx, &pb.PublishRequest{Body: []byte("hello"), Subject: "bale", ExpirationSeconds: 1})

	type expectation struct {
		out     *pb.MessageResponse
		errCode codes.Code
	}

	tests := map[string]struct {
		in       *pb.FetchRequest
		expected expectation
		prepare  func(t *testing.T)
	}{
		"Must_Success": {
			in: &pb.FetchRequest{
				Id:      msg.Id,
				Subject: "bale",
			},
			expected: expectation{
				out:     &pb.MessageResponse{Body: []byte("hello")},
				errCode: 0,
			},
			prepare: func(t *testing.T) {

			},
		},
		"Invalid_ID": {
			in: &pb.FetchRequest{
				Id:      10,
				Subject: "bale",
			},
			expected: expectation{
				out:     nil,
				errCode: codes.NotFound,
			},
			prepare: func(t *testing.T) {

			},
		},
		"Expired_ID": {
			in: &pb.FetchRequest{
				Id:      msg.Id,
				Subject: "bale",
			},
			expected: expectation{
				out:     nil,
				errCode: codes.Unavailable,
			},
			prepare: func(t *testing.T) {
				time.Sleep(2 * time.Second)
			},
		},
		"Broker_Closed": {
			in: &pb.FetchRequest{
				Id:      11,
				Subject: "bale",
			},
			expected: expectation{
				out:     nil,
				errCode: codes.DeadlineExceeded,
			},
			prepare: func(t *testing.T) {
				err := baleBroker.Close()
				require.NoError(t, err)
			},
		},
	}

	for scenario, tc := range tests {
		t.Run(scenario, func(t *testing.T) {
			tc.prepare(t)
			out, err := client.Fetch(ctx, tc.in)
			if err != nil {
				actualStatus, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, actualStatus.Code(), tc.expected.errCode)
			} else {
				require.Equal(t, tc.expected.out.Body, out.Body)
			}
		})
	}
}
