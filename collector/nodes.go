package collector

import (
	"encoding/json"
	"os/exec"
	"strconv"

	"singlestore_exporter/log"

	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

type Node struct {
	MemsqlId      string `json:"memsqlId"`
	Role          string `json:"role"`
	Port          int    `json:"port"`
	ProcessState  string `json:"processState"`
	IsConnectable bool   `json:"isConnectable"`
	Version       string `json:"version"`
	RecoveryState string `json:"recoveryState"`
	BindAddress   string `json:"bindAddress"`
	NodeID        string `json:"nodeID"`
}

type Nodes struct {
	Nodes []Node `json:"nodes"`
}

const (
	node = "node"
)

var (
	nodeStateDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, node, "state"),
		"The state of nodes",
		[]string{"port", "role", "version", "process_state", "recovery_state", "memsql_id", "node_id"},
		nil,
	)
)

type ScrapeNodes struct{}

func (s *ScrapeNodes) Help() string {
	return "Collect node state by memsqlctl"
}

func (s *ScrapeNodes) Scrape(db *sqlx.DB, ch chan<- prometheus.Metric) {
	errorMetric := func() {
		ch <- prometheus.MustNewConstMetric(
			nodeStateDesc, prometheus.GaugeValue, float64(0),
			"Unknown",
			"Unknown",
			"Unknown",
			"Unknown",
			"Unknown",
			"Unknown",
			"Unknown",
		)
	}

	out, err := exec.Command("/usr/bin/memsqlctl", "list-nodes", "--json", "--yes").Output()
	if err != nil {
		log.ErrorLogger.Errorf("scraping command failed: command='/usr/bin/memsqlctl list-nodes --json --yes' error=%v", err)
		errorMetric()
		return
	}

	var nodes Nodes
	if err := json.Unmarshal(out, &nodes); err != nil {
		log.ErrorLogger.Errorf("unmarshal output failed: command='/usr/bin/memsqlctl list-nodes --json --yes' out=%s error=%v", string(out), err)
		errorMetric()
		return
	}

	for _, node := range nodes.Nodes {
		state := 0
		if node.ProcessState == "Running" {
			state = 1
		}

		ch <- prometheus.MustNewConstMetric(
			nodeStateDesc, prometheus.GaugeValue, float64(state),
			strconv.Itoa(node.Port),
			node.Role,
			node.Version,
			node.ProcessState,
			node.RecoveryState,
			node.MemsqlId,
			node.NodeID,
		)

	}
}
