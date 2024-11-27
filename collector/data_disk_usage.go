package collector

import (
	"context"
	"encoding/json"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"os/exec"
	"singlestore_exporter/log"
	"singlestore_exporter/util"
	"sync"
)

type MemsqlNode struct {
	MemsqlId          string `json:"memsqlId"`
	Role              string `json:"role"`
	Port              int    `json:"port"`
	ProcessState      string `json:"processState"`
	IsConnectable     bool   `json:"isConnectable"`
	Version           string `json:"version"`
	RecoveryState     string `json:"recoveryState"`
	AvailabilityGroup int    `json:"availabilityGroup"`
	BindAddress       string `json:"bindAddress"`
	NodeID            string `json:"nodeID"`
}

type MemsqlNodes struct {
	Nodes []MemsqlNode `json:"nodes"`
}

type DataDiskUsage struct {
	NodeID        string `json:"NODE_ID"`
	DatabaseName  string `json:"DATABASE_NAME"`
	Ordinal       string `json:"ORDINAL"`
	BlobsByte     string `json:"BLOBS_B"`
	LogsByte      string `json:"LOGS_B"`
	OtherByte     string `json:"OTHER_B"`
	SnapshotByte  string `json:"SNAPSHOTS_B"`
	TempBlobsByte string `json:"TEMP_BLOBS_B"`
}

type DataDiskUsageRows struct {
	Rows []DataDiskUsage `json:"rows"`
}

var (
	dataDiskUsagesMu sync.Mutex
	dataDiskUsages   []DataDiskUsage
)

func ScrapeDataDiskUsages() {
	var totalRows []DataDiskUsage
	var err error

	defer func() {
		dataDiskUsagesMu.Lock()
		defer dataDiskUsagesMu.Unlock()

		if err != nil {
			dataDiskUsages = nil
		} else {
			dataDiskUsages = totalRows
		}
	}()

	// get memsql nodes first
	var out []byte
	out, err = exec.Command("/usr/bin/memsqlctl", "list-nodes", "--json").Output()
	if err != nil {
		log.ErrorLogger.Errorf("scraping command failed: command='memsqlctl list-nodes --json' out=%s error=%v", string(out), err)
		return
	}

	var memsqlNodes MemsqlNodes
	if err = json.Unmarshal(out, &memsqlNodes); err != nil {
		log.ErrorLogger.Errorf("unmarshal output failed: command='memsqlctl list-nodes --json' out=%s error=%v", string(out), err)
		return
	}

	// get data disk usage per node
	for _, node := range memsqlNodes.Nodes {
		out, err = exec.Command("/usr/bin/memsqlctl", "query", "--memsql-id", node.MemsqlId, "--sql", infoSchemaDataDiskUsageQuery, "--json").Output()
		if err != nil {
			log.ErrorLogger.Errorf("scraping command failed: command='memsqlctl query --sql '%s' --json' out=%s error=%v", infoSchemaDataDiskUsageQuery, string(out), err)
			return
		}

		var rows DataDiskUsageRows
		if err = json.Unmarshal(out, &rows); err != nil {
			log.ErrorLogger.Errorf("unmarshal output failed: command='memsqlctl query --sql '%s' --json' out=%s error=%v", infoSchemaDataDiskUsageQuery, string(out), err)
			return
		}

		totalRows = append(totalRows, rows.Rows...)
	}
}

const (
	dataDiskUsage = "data_disk_usage"

	infoSchemaDataDiskUsageQuery = "SELECT NODE_ID, DATABASE_NAME, ORDINAL, BLOBS_B, TEMP_BLOBS_B, LOGS_B, SNAPSHOTS_B, OTHER_B FROM information_schema.LMV_DATA_DISK_USAGE"
)

var (
	dataDiskUsageBlobsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, dataDiskUsage, "blobs"),
		"disk usage of blob data in bytes",
		[]string{"node_id", "database_name", "ordinal"},
		nil,
	)

	dataDiskUsageLogsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, dataDiskUsage, "logs"),
		"disk usage of log data in bytes",
		[]string{"node_id", "database_name", "ordinal"},
		nil,
	)

	dataDiskUsageOthersDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, dataDiskUsage, "others"),
		"disk usage of other data in bytes",
		[]string{"node_id", "database_name", "ordinal"},
		nil,
	)

	dataDiskUsageSnapshotDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, dataDiskUsage, "snapshot"),
		"disk usage of snapshot data in bytes",
		[]string{"node_id", "database_name", "ordinal"},
		nil,
	)

	dataDiskUsageTempDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, dataDiskUsage, "temp"),
		"disk usage of temp data in bytes",
		[]string{"node_id", "database_name", "ordinal"},
		nil,
	)
)

type ScrapeDataDiskUsage struct{}

func (s *ScrapeDataDiskUsage) Help() string {
	return "Collect data disk usage by memsqlctl"
}

func (s *ScrapeDataDiskUsage) Scrape(ctx context.Context, db *sqlx.DB, ch chan<- prometheus.Metric) {
	if dataDiskUsages == nil {
		return
	}

	dataDiskUsagesMu.Lock()
	defer dataDiskUsagesMu.Unlock()

	for _, usage := range dataDiskUsages {
		blobsByte := usage.BlobsByte
		logsByte := usage.LogsByte
		otherByte := usage.OtherByte
		snapshotByte := usage.SnapshotByte
		tempBlobsByte := usage.TempBlobsByte

		ch <- prometheus.MustNewConstMetric(
			dataDiskUsageBlobsDesc, prometheus.GaugeValue, util.StringToFloat64(blobsByte),
			usage.NodeID,
			usage.DatabaseName,
			usage.Ordinal,
		)
		ch <- prometheus.MustNewConstMetric(
			dataDiskUsageLogsDesc, prometheus.GaugeValue, util.StringToFloat64(logsByte),
			usage.NodeID,
			usage.DatabaseName,
			usage.Ordinal,
		)
		ch <- prometheus.MustNewConstMetric(
			dataDiskUsageOthersDesc, prometheus.GaugeValue, util.StringToFloat64(otherByte),
			usage.NodeID,
			usage.DatabaseName,
			usage.Ordinal,
		)
		ch <- prometheus.MustNewConstMetric(
			dataDiskUsageSnapshotDesc, prometheus.GaugeValue, util.StringToFloat64(snapshotByte),
			usage.NodeID,
			usage.DatabaseName,
			usage.Ordinal,
		)
		ch <- prometheus.MustNewConstMetric(
			dataDiskUsageTempDesc, prometheus.GaugeValue, util.StringToFloat64(tempBlobsByte),
			usage.NodeID,
			usage.DatabaseName,
			usage.Ordinal,
		)
	}
}
