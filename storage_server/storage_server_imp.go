package storage_server

import (
	"context"
	"io"
	"os"
	utils "project-dfs"
	"project-dfs/pb"
	"syscall"
)

const (
	StoragePath = "storage"
)

type StorageServiceController struct {
	pb.UnimplementedStorageServer
	Server *StorageServer
}

func NewStorageServiceController(server *StorageServer) *StorageServiceController {
	return &StorageServiceController{
		Server: server,
	}
}

// ---

func getFreeSpace() int64 {
	var stat syscall.Statfs_t
	wd, _ := os.Getwd()
	_ = syscall.Statfs(wd, &stat)
	return int64(stat.Bavail * uint64(stat.Bsize))
}

func (ctlr *StorageServiceController) Initialize(ctx context.Context, args *pb.InitializeArgs) (*pb.InitializeResult, error) {
	/* Initialize the client storage on a new system,
	remove any existing file in the dfs root directory and return available size.*/

	_ = os.RemoveAll(StoragePath)
	return &pb.InitializeResult{
		ErrorStatus: &pb.ErrorStatus{
			Code:        0,
			Description: "OK",
		},
		AvailableSize: getFreeSpace(),
	}, nil
}

func (ctlr *StorageServiceController) CreateFile(ctx context.Context, args *pb.CreateFileArgs) (*pb.CreateFileResult, error) {
	// create a new empty file
	path := StoragePath + args.Path
	exists, directoryPath := utils.DoesDirectoryExist(path)

	if !exists {
		errDir := os.MkdirAll(directoryPath, 0777)
		if errDir != nil {
			return &pb.CreateFileResult{ErrorStatus: &pb.ErrorStatus{
				Code:        1,
				Description: errDir.Error(),
			}}, nil
		}
	}

	_, err := os.Create(path)
	if err != nil {
		return &pb.CreateFileResult{ErrorStatus: &pb.ErrorStatus{
			Code:        1,
			Description: err.Error(),
		}}, nil
	}

	return &pb.CreateFileResult{ErrorStatus: &pb.ErrorStatus{
		Code:        0,
		Description: "OK",
	}}, nil
}

func (ctlr *StorageServiceController) ReadFile(ctx context.Context, args *pb.ReadFileArgs) (response *pb.ReadFileResult, err error) {
	// download a file from the DFS to the Client side

	path := StoragePath + args.Path
	fd, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return &pb.ReadFileResult{ErrorStatus: &pb.ErrorStatus{
			Code:        1,
			Description: err.Error(),
		},
			Buffer: make([]byte, 0),
			Count:  0}, nil
	}

	buf := make([]byte, args.Count)
	n, err := fd.ReadAt(buf, args.Offset)
	if n <= 0 {
		return &pb.ReadFileResult{
			ErrorStatus: &pb.ErrorStatus{
				Code:        0,
				Description: err.Error(),
			},
			Buffer: make([]byte, 0),
			Count:  0,
		}, nil
	}

	fd.Close()
	response = &pb.ReadFileResult{
		ErrorStatus: &pb.ErrorStatus{
			Code:        0,
			Description: "OK",
		},
		Buffer: buf[0:n],
		Count:  int32(n),
	}
	return response, nil
}

func (ctlr *StorageServiceController) WriteFile(ctx context.Context, args *pb.WriteFileArgs) (*pb.WriteFileResult, error) {

	path := StoragePath + args.Path
	fd, err := os.OpenFile(path, os.O_WRONLY, os.ModePerm)
	if err != nil {
		return &pb.WriteFileResult{ErrorStatus: &pb.ErrorStatus{
			Code:        1,
			Description: err.Error(),
		}}, nil
	}

	buf := args.Buffer
	_, err = fd.WriteAt(buf, args.Offset)
	if err != nil {
		return &pb.WriteFileResult{ErrorStatus: &pb.ErrorStatus{
			Code:        1,
			Description: err.Error(),
		}}, nil
	}

	fd.Close()

	if !args.IsChainCall {
		response, err := ctlr.Server.GetNamingClient().Discover(ctx, &pb.DiscoverRequest{
			Path: args.Path,
		})
		if err != nil {
			println("Error while replicating write call:", err.Error())
		} else {
			for _, s := range response.StorageInfo {
				if s.Alias == ctlr.Server.Alias {
					continue
				}
				client := ctlr.Server.GetStorageClient(s.Address)
				if client == nil {
					println("aborting write replication")
					break
				}
				client.WriteFile(ctx, &pb.WriteFileArgs{
					Path:        args.Path,
					Offset:      args.Offset,
					Buffer:      args.Buffer,
					IsChainCall: true,
				})
			}
		}
	}

	return &pb.WriteFileResult{ErrorStatus: &pb.ErrorStatus{
		Code:        0,
		Description: "OK",
	}}, nil
}

func (ctlr *StorageServiceController) Remove(ctx context.Context, args *pb.RemoveArgs) (*pb.RemoveResult, error) {
	// allow to delete any file from DFS
	// allow to delete directory.
	// If the directory contains files the system asks for confirmation

	path := StoragePath + args.Path
	err := os.RemoveAll(path)
	if err != nil {
		return &pb.RemoveResult{ErrorStatus: &pb.ErrorStatus{
			Code:        1,
			Description: err.Error(),
		}}, nil
	}

	return &pb.RemoveResult{ErrorStatus: &pb.ErrorStatus{
		Code:        0,
		Description: "OK",
	}}, nil
}

func (ctlr *StorageServiceController) GetFileInfo(ctx context.Context, args *pb.GetFileInfoArgs) (*pb.GetFileInfoResult, error) {
	// provide information about the file (any useful information - size, node id, etc.)

	path := StoragePath + args.Path
	fileInfo, err := os.Lstat(path)
	if err != nil {
		return &pb.GetFileInfoResult{ErrorStatus: &pb.ErrorStatus{
			Code:        1,
			Description: err.Error(),
		},
			FileSize: 0}, nil
	}

	return &pb.GetFileInfoResult{ErrorStatus: &pb.ErrorStatus{
		Code:        0,
		Description: "OK",
	},
		FileSize: uint64(fileInfo.Size())}, nil
}

func (ctlr *StorageServiceController) Copy(ctx context.Context, args *pb.CopyArgs) (*pb.CopyResult, error) {

	path := StoragePath + args.Path
	newPath := StoragePath + args.NewPath
	exists, directoryPath := utils.DoesDirectoryExist(path)
	existsNew, directoryNewPath := utils.DoesDirectoryExist(newPath)

	if !exists {
		errDir := os.MkdirAll(directoryPath, 0777)
		if errDir != nil {
			return &pb.CopyResult{ErrorStatus: &pb.ErrorStatus{
				Code:        1,
				Description: errDir.Error(),
			}}, nil
		}
	}

	if !existsNew {
		errDir := os.MkdirAll(directoryNewPath, 0777)
		if errDir != nil {
			return &pb.CopyResult{ErrorStatus: &pb.ErrorStatus{
				Code:        1,
				Description: errDir.Error(),
			}}, nil
		}
	}

	src, err := os.Open(path)
	if err != nil {
		return &pb.CopyResult{ErrorStatus: &pb.ErrorStatus{
			Code:        1,
			Description: err.Error(),
		}}, nil
	}

	dest, err := os.Create(newPath)
	if err != nil {
		return &pb.CopyResult{ErrorStatus: &pb.ErrorStatus{
			Code:        1,
			Description: err.Error(),
		}}, nil
	}

	_, err = io.Copy(dest, src)
	if err != nil {
		return &pb.CopyResult{ErrorStatus: &pb.ErrorStatus{
			Code:        1,
			Description: err.Error(),
		}}, nil
	}

	return &pb.CopyResult{ErrorStatus: &pb.ErrorStatus{
		Code:        0,
		Description: "OK",
	}}, nil
}

func (ctlr *StorageServiceController) Move(ctx context.Context, args *pb.MoveArgs) (*pb.MoveResult, error) {

	// update IndexTree: send request to naming server
	// add a new service into naming_server_imp for handling such a request

	path := StoragePath + args.Path
	newPath := StoragePath + args.NewPath
	exists, directoryPath := utils.DoesDirectoryExist(path)
	existsNew, directoryNewPath := utils.DoesDirectoryExist(newPath)

	if !exists {
		errDir := os.MkdirAll(directoryPath, 0777)
		if errDir != nil {
			return &pb.MoveResult{ErrorStatus: &pb.ErrorStatus{
				Code:        1,
				Description: errDir.Error(),
			}}, nil
		}
	}

	if !existsNew {
		errDir := os.MkdirAll(directoryNewPath, 0777)
		if errDir != nil {
			return &pb.MoveResult{ErrorStatus: &pb.ErrorStatus{
				Code:        1,
				Description: errDir.Error(),
			}}, nil
		}
	}

	err := os.Rename(path, newPath)
	if err != nil {
		return &pb.MoveResult{ErrorStatus: &pb.ErrorStatus{
			Code:        1,
			Description: err.Error(),
		}}, nil
	}

	return &pb.MoveResult{ErrorStatus: &pb.ErrorStatus{
		Code:        0,
		Description: "OK",
	}}, nil
}

//func (ctlr *StorageServiceController) ReadDirectory(ctx context.Context, args *pb.ReadDirectoryArgs) (*pb.ReadDirectoryResult, error) {
//	// return list of files, which are stored in the directory
//
//	path := StoragePath + args.Path
//	fd, err := os.Open(path)
//	if err != nil {
//		return &pb.ReadDirectoryResult{ErrorStatus: &pb.ErrorStatus{
//			Code:        1,
//			Description: err.Error(),
//		},
//			Contents: make([]*pb.Node, 0)}, nil
//	}
//
//	fileInfo, err := fd.Readdir(0)
//	if err != nil {
//		return &pb.ReadDirectoryResult{ErrorStatus: &pb.ErrorStatus{
//			Code:        1,
//			Description: err.Error(),
//		},
//			Contents: make([]*pb.Node, 0)}, nil
//	}
//
//	fileInfoEntries := make([]*pb.Node, len(fileInfo))
//	var mode pb.NodeMode
//	for _, entry := range fileInfo {
//		if entry.IsDir() {
//			mode = pb.NodeMode_DIRECTORY
//		} else {
//			mode = pb.NodeMode_REGULAR_FILE
//		}
//		fileInfoEntries = append(fileInfoEntries, &pb.Node{
//			Mode: mode,
//			Name: entry.Name(),
//		})
//	}
//
//	return &pb.ReadDirectoryResult{ErrorStatus: &pb.ErrorStatus{
//		Code:        0,
//		Description: "OK",
//	},
//		Contents: fileInfoEntries}, nil
//}

//func (ctlr *StorageServiceController) MakeDirectory(ctx context.Context, args *pb.MakeDirectoryArgs) (*pb.MakeDirectoryResult, error) {
//
//	path := StoragePath + args.Path
//	err := os.MkdirAll(path, 0777)
//	if err != nil {
//		return &pb.MakeDirectoryResult{ErrorStatus: &pb.ErrorStatus{
//			Code:        1,
//			Description: err.Error(),
//		}}, nil
//	}
//
//	return &pb.MakeDirectoryResult{ErrorStatus: &pb.ErrorStatus{
//		Code:        0,
//		Description: "OK",
//	}}, nil
//}
