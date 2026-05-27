package broker

import (
	"hash/fnv"
	"sort"

	"github.com/san4b0t/mini-kafka/internal/config"
)

// ClusterManager handles deterministic topic ownership routing.
type ClusterManager struct {
	selfID string
	nodes  []config.NodeConfig
}

func NewClusterManager(cfg *config.Config) *ClusterManager {
	nodes := make([]config.NodeConfig, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		nodes = append(nodes, n)
	}
	// Sort by ID to ensure consistent hashing across all brokers
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	return &ClusterManager{
		selfID: cfg.BrokerID,
		nodes:  nodes,
	}
}

// GetOwner determines which broker node owns the given topic using hash modulo.
func (c *ClusterManager) GetOwner(topic string) config.NodeConfig {
	if len(c.nodes) == 0 {
		return config.NodeConfig{}
	}
	h := fnv.New32a()
	h.Write([]byte(topic))
	idx := h.Sum32() % uint32(len(c.nodes))
	return c.nodes[idx]
}

func (c *ClusterManager) IsOwner(topic string) bool {
	owner := c.GetOwner(topic)
	return owner.ID == c.selfID
}
