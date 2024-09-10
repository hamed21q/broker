package server

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
)

func UnaryLoggerInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	resp, err := handler(ctx, req)

	if err != nil {
		log.Println("ERROR: grpc call failed:", err)
	} else {
		fmt.Printf(
			"SUCCESS: Method: %s Request: %v Response: %v \n",
			info.FullMethod,
			req,
			resp,
		)
	}

	return resp, err
}
