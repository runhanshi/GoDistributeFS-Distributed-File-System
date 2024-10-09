package storage_server

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"project-dfs/pb"
	"strconv"
	"strings"
	"sync"
)

type StorageServer struct {
	LocalAddress          string
	Alias                 string
	NamingServerAddress   string
	PublicHostname        string
	storageAddressesMutex sync.Mutex
	storageAddresses      map[string]string // key:value = serverAlias:serverAddress
	namingClient          pb.NamingClient
	storageClients        map[string]pb.StorageClient
}

func (server *StorageServer) SetMap(newKey string, newValue string) {
	server.storageAddressesMutex.Lock()
	defer server.storageAddressesMutex.Unlock()
	server.storageAddresses[newKey] = newValue
}

func (server *StorageServer) GetNamingClient() pb.NamingClient {
	if server.namingClient == nil {
		conn, err := grpc.Dial(server.NamingServerAddress, grpc.WithInsecure())
		if err != nil {
			println("Error while getting naming client:", err)
			return nil
		}
		server.namingClient = pb.NewNamingClient(conn)
	}
	return server.namingClient
}

func (server *StorageServer) GetStorageClient(address string) pb.StorageClient {
	client, ok := server.storageClients[address]
	if !ok {
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			println("Error while getting storage client:", err)
			return nil
		}
		client = pb.NewStorageClient(conn)
		server.storageClients[address] = client
	}
	return client
}

func initStorageServer() *StorageServer {
	// Obtain local address from environment
	localAddress := os.Getenv("ADDRESS")
	if localAddress == "" {
		localAddress = "0.0.0.0:5678"
		fmt.Println("ADDRESS variable not specified; falling back to", localAddress)
	}

	// Obtain naming address from environment
	namingServerAddress := os.Getenv("NAMING_SERVER_ADDRESS")
	if namingServerAddress == "" {
		namingServerAddress = "localhost:5678"
		fmt.Println("NAMING_SERVER_ADDRESS variable not specified; falling back to", namingServerAddress)
	}

	// Obtain public hostname from environment
	publicHostname := os.Getenv("PUBLIC_HOSTNAME")
	if publicHostname == "" {
		fmt.Println("PUBLIC_HOSTNAME variable not specified; aborting")
		os.Exit(1)
	}

	// Obtain alias from environment
	alias := os.Getenv("ALIAS")
	if alias == "" {
		alias = "storage"
		fmt.Println("ALIAS variable not specified; falling back to", alias)
	}

	return &StorageServer{
		LocalAddress:          localAddress,
		Alias:                 alias,
		NamingServerAddress:   namingServerAddress,
		PublicHostname:        publicHostname,
		storageAddressesMutex: sync.Mutex{},
		storageAddresses:      make(map[string]string),
		storageClients:        map[string]pb.StorageClient{},
	}
}

func CheckError(err error) {
	if err != nil {
		println("error serving gRPC storage server:", err.Error())
		os.Exit(1)
	}
}

func Run() {
	server := initStorageServer()

	fmt.Printf("Initialized storage metadata: %+v\n", server)

	fmt.Println("Connecting to naming server at", server.NamingServerAddress)
	conn, err := grpc.Dial(server.NamingServerAddress, grpc.WithInsecure())
	CheckError(err)

	port, _ := strconv.Atoi(server.LocalAddress[strings.LastIndex(server.LocalAddress, ":")+1:])

	newServer := pb.NewNamingClient(conn)
	response, err := newServer.Register(context.Background(), &pb.RegRequest{
		ServerAlias:    server.Alias,
		Port:           uint32(port),
		PublicHostname: server.PublicHostname,
	})
	CheckError(err)
	log.Printf("Response from naming server: %s", response.GetStatus())

	if response.GetStatus().String() == "ACCEPT" {
		// listen to connections
		listener, err := net.Listen("tcp", server.LocalAddress)
		CheckError(err)

		fmt.Println("Starting sync of " + server.Alias + "...")
		server.Sync("")
		fmt.Println("Sync completed.")

		println("Listening on " + server.LocalAddress)
		storageController := NewStorageServiceController(server)
		grpcServer := grpc.NewServer()
		pb.RegisterStorageServer(grpcServer, storageController)
		err = grpcServer.Serve(listener)
		CheckError(err)
	}
}

func (server *StorageServer) Sync(path string) {
	fmt.Println("Syncing directory", path)

	response, err := server.GetNamingClient().ListDirectory(context.Background(), &pb.ListDirectoryRequest{Path: path})
	if err != nil {
		println("Error syncing path", path, ":", err.Error())
		return
	}

	fmt.Println(response.Contents)

	for _, content := range response.Contents {
		if content.Mode == pb.NodeMode_DIRECTORY {
			// Recursively sync directory
			server.Sync(path + "/" + content.Name)
		} else {
			filePath := path + "/" + content.Name
			os.MkdirAll(StoragePath+path, 0777)

			discovered, err := server.GetNamingClient().Discover(context.Background(), &pb.DiscoverRequest{
				Path:               filePath,
				ExcludeStorageName: server.Alias,
			})
			if err != nil {
				println("Error discovery during sync:", err.Error())
				continue
			}

			// Skip syncing if this is the only storage server holding this file
			if len(discovered.StorageInfo) < 1 {
				fmt.Println("Not enough storage servers for", filePath)
				continue
			}

			fmt.Println("Syncing file", filePath)

			addr := discovered.StorageInfo[0].PublicAddress
			storageClient := server.GetStorageClient(addr)

			offset := int64(0)
			for true {
				read, err := storageClient.ReadFile(context.Background(), &pb.ReadFileArgs{
					Path:   filePath,
					Offset: offset,
					Count:  4096,
				})
				if err != nil {
					println("Error read during sync:", err.Error())
					break
				}

				fmt.Println("Writing", read.Buffer)

				fd, _ := os.OpenFile(StoragePath+filePath, os.O_WRONLY, os.ModePerm)
				fd.WriteAt(read.Buffer, offset)
				offset += int64(read.Count)
				fd.Close()

				if read.Count == 0 {
					break
				}
			}
		}
	}
}
