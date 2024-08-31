package server

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
)

func UnaryLoggerInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	resp, err := handler(ctx, req)

	fmt.Printf(
		"Method: %s Request: %v Response: %v \n",
		info.FullMethod,
		req,
		resp,
	)

	return resp, err
}
