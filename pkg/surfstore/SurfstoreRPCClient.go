package surfstore

import (
	context "context"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddr string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	// log.Println("Block:", string(block.BlockData))
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	log.Println(blockStoreAddr)
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// log.Println("BlockStore client initiated")

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		log.Println("Error occurred while putting: ", err.Error())
		return err
	}
	*succ = true

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	BlockHashesIn := &BlockHashes{Hashes: blockHashesIn}
	blockHashes, err := c.HasBlocks(ctx, BlockHashesIn)

	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = blockHashes.Hashes
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the serverm
	metaStoreAddr := surfClient.MetaStoreAddr
	conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	m := NewMetaStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fileInfoMap, err := m.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*serverFileInfoMap = fileInfoMap.FileInfoMap
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// connect to the serverm
	metaStoreAddr := surfClient.MetaStoreAddr
	conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	m := NewMetaStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	version, err := m.UpdateFile(ctx, fileMetaData)
	if err != nil {
		conn.Close()
		return err
	}
	*latestVersion = version.Version
	return err
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// connect to the serverm
	metaStoreAddr := surfClient.MetaStoreAddr
	conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	m := NewMetaStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = m.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	// log.Println("BlockStoreAddr is123 :", blockStoreAddrObj.Addr)
	// *blockStoreAddr = blockStoreAddrObj.Addr
	// log.Println("Blockstoree addr pointer is: ", blockStoreAddr, *blockStoreAddr)
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	panic("todo")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	panic("todo")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	panic("todo")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddr: hostPort,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}
