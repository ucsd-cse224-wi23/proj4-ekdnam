package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	hashes := []string{}
	for hash := range c.ServerMap {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	responsisbleServer := c.ServerMap[hashes[0]]
	// blockHash := c.Hash(blockId)
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsisbleServer = c.ServerMap[hashes[i]]
			break
		}
	}
	return responsisbleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	consistentHashRing := new(ConsistentHashRing)
	for _, server := range serverAddrs {
		serverHash := consistentHashRing.Hash(server)
		consistentHashRing.ServerMap[serverHash] = server
	}
	return consistentHashRing
}
