package server

import (
	"BaleBroker/broker"
	pb "BaleBroker/gapi/pb"
	"BaleBroker/pkg"
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type Server struct {
	pb.UnimplementedBrokerServer
	broker *broker.BaleBroker
}

func NewServer(b *broker.BaleBroker) *Server {
	return &Server{
		broker: b,
	}
}

func (server *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	msg := pkg.Message{
		Body:       string(req.GetBody()),
		Expiration: time.Duration(req.GetExpirationSeconds()) * time.Second,
	}
	id, err := server.broker.Publish(ctx, req.GetSubject(), msg)
	if err != nil {
		switch err {
		case broker.ErrUnavailable:
			return nil, status.Error(codes.DeadlineExceeded, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &pb.PublishResponse{Id: int64(id)}, nil
}

func (server *Server) Subscribe(req *pb.SubscribeRequest, res grpc.ServerStreamingServer[pb.MessageResponse]) error {
	ch, err := server.broker.Subscribe(context.Background(), req.GetSubject())
	if err != nil {
		return err
	}
	for {
		err := res.Send(&pb.MessageResponse{Body: []byte((<-ch).Body)})
		if err != nil {
			switch err {
			case broker.ErrUnavailable:
				return status.Error(codes.DeadlineExceeded, err.Error())
			default:
				return status.Error(codes.Internal, err.Error())
			}
		}
	}
}

func (server *Server) Fetch(ctx context.Context, req *pb.FetchRequest) (*pb.MessageResponse, error) {
	msg, err := server.broker.Fetch(ctx, req.GetSubject(), int(req.GetId()))
	if err != nil {
		switch err {
		case broker.ErrUnavailable:
			return nil, status.Error(codes.DeadlineExceeded, err.Error())
		case broker.ErrInvalidID:
			return nil, status.Error(codes.NotFound, err.Error())
		case broker.ErrExpiredID:
			return nil, status.Error(codes.Unavailable, err.Error())
		}
	}

	return &pb.MessageResponse{Body: []byte(msg.Body)}, nil
}
