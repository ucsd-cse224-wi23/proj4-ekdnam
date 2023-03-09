package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fname := fileMetaData.Filename
	clientVersion := fileMetaData.Version
	var version *Version = new(Version)
	val, ok := m.FileMetaMap[fname]
	if !ok {
		m.FileMetaMap[fname] = fileMetaData
		*version = Version{Version: clientVersion}
		return version, nil
	} else {
		serverVersion := val.Version
		if clientVersion == serverVersion+1 {
			m.FileMetaMap[fname] = fileMetaData
			*version = Version{Version: clientVersion}
			return version, nil
		} else {
			*version = Version{Version: -1}
			return version, fmt.Errorf("file version mismatch")
		}
	}
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	var bsMap map[string]*BlockHashes
	for _, hash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(hash)
		bsMap[server].Hashes = append(bsMap[server].Hashes, hash)
	}
	blockStoreMap := &BlockStoreMap{BlockStoreMap: bsMap}
	return blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	blockStoreAddrs := &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}
	return blockStoreAddrs, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string, consistentHashRing *ConsistentHashRing) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: consistentHashRing,
	}
}
