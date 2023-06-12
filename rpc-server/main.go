package main

import (
	"context"
	"fmt"
	"log"

	rpc "github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc/imservice"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	etcd "github.com/kitex-contrib/registry-etcd"
)

var (
	redisDatabase = &RedisDatabaseClient{}
)

func main() {
	ctx := context.Background()

	// Initialize the server redis database client
	err := redisDatabase.InitializeClient(ctx, "redis:6379", "")

	if err != nil {
		errMsg := fmt.Sprintf("failed to init Redis client, err: %v", err)
		log.Fatal(errMsg)
	}

	r, err := etcd.NewEtcdRegistry([]string{"etcd:2379"}) // r should not be reused.
	if err != nil {
		log.Fatal(err)
		log.Fatal(err)
	}

	// Create the server
	server := rpc.NewServer(new(IMServiceImpl), server.WithRegistry(r), server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{
		ServiceName: "demo.rpc.server",
	}))

	err = server.Run()
	if err != nil {
		log.Println(err.Error())
	}
}
