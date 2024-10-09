package naming_server

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc/peer"
	utils "project-dfs"
	"project-dfs/pb"
	"strconv"
	"strings"
	"syscall"
)

type NamingServerController struct {
	pb.UnimplementedNamingServer
	Server *NamingServer
}

// returns the pointer to the implementation
func NewNamingServiceController(server *NamingServer) *NamingServerController {
	return &NamingServerController{
		Server: server,
	}
}

// update address map on the NAMING Server
func (ctlr *NamingServerController) Register(ctx context.Context, request *pb.RegRequest) (*pb.RegResponse, error) {
	fmt.Println("Register:", request)

	otherPeer, ok := peer.FromContext(ctx)
	if !ok {
		println("other peer not found")
		return &pb.RegResponse{Status: pb.Status_DECLINE}, errors.New("other peer not found")
	}

	// add a new Server to the list of known Storage Servers
	peerAddress := otherPeer.Addr.String()
	// Remove local port
	peerAddress = peerAddress[:strings.LastIndex(peerAddress, ":")]
	// Add remote port
	peerAddress += ":" + strconv.Itoa(int(request.Port))

	ctlr.Server.SetAddressMap(request.ServerAlias, &StorageServerInfo{
		privateAddress: peerAddress,
		publicAddress:  request.PublicHostname + ":" + strconv.Itoa(int(request.Port)),
	})

	return &pb.RegResponse{Status: pb.Status_ACCEPT}, nil
}

// key is the file's path
// element is StorageInfo struct
func (ctlr *NamingServerController) Discover(ctx context.Context, request *pb.DiscoverRequest) (response *pb.DiscoverResponse, err error) {
	fmt.Println("Discover:", request)
	storages := make([]*pb.DiscoveredStorage, 0)

	// if path == "" return ALL storage servers
	if request.Path == "" {
		for alias, info := range ctlr.Server.StorageAddresses {
			storages = append(storages, &pb.DiscoveredStorage{
				Alias:         alias,
				Address:       info.privateAddress,
				PublicAddress: info.publicAddress,
			})
		}
		return &pb.DiscoverResponse{StorageInfo: storages}, nil
	}

	node, ok := ctlr.Server.FindNode(request.Path)
	if !ok {
		fmt.Println("Node not found! Returning empty list")
		return &pb.DiscoverResponse{
			StorageInfo: []*pb.DiscoveredStorage{},
		}, nil
	}

	for _, storage := range node.Storages {
		if request.GetExcludeStorageName() == storage.Alias {
			continue
		}

		info := ctlr.Server.StorageAddresses[storage.Alias]
		storages = append(storages, &pb.DiscoveredStorage{
			Alias:         storage.Alias,
			Address:       info.privateAddress,
			PublicAddress: info.publicAddress,
		})
	}

	fmt.Println("Returning storages:", storages)
	return &pb.DiscoverResponse{StorageInfo: storages}, nil
}

// ---

func (ctlr *NamingServerController) CreateFile(ctx context.Context, request *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	fmt.Println("CreateFile:", request)

	// client sends path
	// traverse index tree and find node parent for the path
	// add child with file name
	// find 2 random storages
	// contact them to create the file

	//ok, dir := utils.DoesDirectoryExist(request.Path)
	//if !ok {
	//	_ = os.MkdirAll(dir, 0777)
	//}

	node := ctlr.Server.CreateNodeIfNotExists(request.Path, true)
	servers := ctlr.Server.Get2RandomStorageServers()
	for _, s := range servers {
		fmt.Println("Sending create file request to storage server", s.Alias)
		server := ctlr.Server.GetStorageServer(s.Address)
		response, err := server.CreateFile(ctx, &pb.CreateFileArgs{Path: request.Path})
		if err != nil {
			println("Error creating file:", err.Error())
			continue
		}
		if response.ErrorStatus.Code != 0 {
			println("Error during file creation:", response.ErrorStatus.Description)
			continue
		}
		node.Storages = append(node.Storages, &StorageInfo{Alias: s.Alias})
		fmt.Println("Storage", s.Alias, "added to node", node.Name)
	}

	return &pb.CreateFileResponse{ErrorStatus: &pb.ErrorStatus{
		Code:        0,
		Description: "",
	}}, nil
}

func (ctlr *NamingServerController) Move(ctx context.Context, request *pb.MoveRequest) (*pb.MoveResponse, error) {
	fmt.Println("Move:", request)

	// client sends paths: old and new
	// traverse index tree and find node

	// find storages with the file
	// contact them to move the file

	oldParent, ok := ctlr.Server.FindNode(utils.DirPart(request.Path))
	if !ok {
		return &pb.MoveResponse{ErrorStatus: &pb.ErrorStatus{
			Code:        1,
			Description: "Old parent node does not exist",
		}}, nil
	}
	oldName := utils.NamePart(request.Path)
	newName := utils.NamePart(request.NewPath)
	node := oldParent.GetChild(oldName)

	oldParent.RemoveChild(oldName)

	node.Name = newName

	newParentPath := utils.DirPart(request.NewPath)
	newParent := ctlr.Server.CreateNodeIfNotExists(newParentPath, false)
	newParent.AddChild(node)

	for _, storage := range node.Storages {
		info := ctlr.Server.StorageAddresses[storage.Alias]
		ss := ctlr.Server.GetStorageServer(info.privateAddress)
		_, _ = ss.Move(ctx, &pb.MoveArgs{
			Path:    request.Path,
			NewPath: request.NewPath,
		})
	}

	return &pb.MoveResponse{ErrorStatus: &pb.ErrorStatus{
		Code:        0,
		Description: "",
	}}, nil
}

func (ctlr *NamingServerController) DeleteFile(ctx context.Context, request *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	fmt.Println("DeleteFile:", request)

	// client sends path
	// traverse index tree and find node parent for the path
	// delete child with file name
	// find storages with the file
	// contact them to delete the file

	parentPath := utils.DirPart(request.Path)
	parent, ok := ctlr.Server.FindNode(parentPath)
	if !ok {
		return &pb.DeleteResponse{
			ErrorStatus: &pb.ErrorStatus{
				Code:        uint32(syscall.ENOENT),
				Description: "No parent directory found",
			},
		}, nil
	}
	parent.RemoveChild(utils.NamePart(request.Path))

	for _, info := range ctlr.Server.StorageAddresses {
		server := ctlr.Server.GetStorageServer(info.privateAddress)
		server.Remove(ctx, &pb.RemoveArgs{Path: request.Path})
	}

	return &pb.DeleteResponse{ErrorStatus: &pb.ErrorStatus{
		Code:        0,
		Description: "",
	}}, nil
}

func (ctlr *NamingServerController) DeleteDirectory(ctx context.Context, request *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	fmt.Println("DeleteDirectory:", request)

	// client sends path
	// traverse index tree and find node parent for the path
	// delete child with directory name
	// find storages with the directory
	// contact them to delete the directory

	parentPath := utils.DirPart(request.Path)
	parent, ok := ctlr.Server.FindNode(parentPath)
	if !ok {
		return &pb.DeleteResponse{
			ErrorStatus: &pb.ErrorStatus{
				Code:        uint32(syscall.ENOENT),
				Description: "No parent directory found",
			},
		}, nil
	}
	parent.RemoveChild(utils.NamePart(request.Path))

	for _, info := range ctlr.Server.StorageAddresses {
		server := ctlr.Server.GetStorageServer(info.privateAddress)
		server.Remove(ctx, &pb.RemoveArgs{Path: request.Path})
	}

	return &pb.DeleteResponse{ErrorStatus: &pb.ErrorStatus{
		Code:        0,
		Description: "",
	}}, nil
}

func (ctlr *NamingServerController) MakeDirectory(ctx context.Context, request *pb.MakeDirectoryRequest) (*pb.MakeDirectoryResponse, error) {
	fmt.Println("MakeDirectory:", request)

	// client sends path
	// traverse index tree and find node parent for the path
	// add child with file name
	// find 2 random storages
	// contact them to make the directory

	ctlr.Server.CreateNodeIfNotExists(request.Path, false)
	return &pb.MakeDirectoryResponse{ErrorStatus: &pb.ErrorStatus{
		Code:        0,
		Description: "",
	}}, nil
}

func (ctlr *NamingServerController) ListDirectory(ctx context.Context, request *pb.ListDirectoryRequest) (*pb.ListDirectoryResponse, error) {
	fmt.Println("ListDirectory:", request)

	// client sends path
	// traverse index tree and find node
	// return all children of the node

	node, ok := ctlr.Server.FindNode(request.Path)
	if !ok {
		return &pb.ListDirectoryResponse{
			ErrorStatus: &pb.ErrorStatus{
				Code:        uint32(syscall.ENOENT),
				Description: "No such directory",
			},
			Contents: nil,
		}, nil
	}

	var res []*pb.Node

	for _, child := range node.Children {
		mode := pb.NodeMode_REGULAR_FILE
		if child.Type == DIR {
			mode = pb.NodeMode_DIRECTORY
		}

		res = append(res, &pb.Node{
			Mode: mode,
			Name: child.Name,
		})
	}

	fmt.Println("Returning", res)

	return &pb.ListDirectoryResponse{
		ErrorStatus: &pb.ErrorStatus{
			Code:        0,
			Description: "",
		},
		Contents: res,
	}, nil
}

func (ctlr *NamingServerController) Copy(ctx context.Context, request *pb.CopyRequest) (*pb.CopyResponse, error) {
	panic("no copy operation")
}
