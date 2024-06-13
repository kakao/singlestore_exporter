package main

import (
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"singlestore_exporter/collector"
	"singlestore_exporter/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var Version string
var versionFlag = flag.Bool("version", false, "print the version")

func main() {
	flagListenAddress := flag.String("net.listen_address", "0.0.0.0:9105", "network address on which the exporter listens")
	flagPprof := flag.Bool("debug.pprof", false, "enable pprof")

	flagReplicationStatusPtr := flag.Bool("collect.replication_status", false, "collect replication status")

	flagSlowQueryPtr := flag.Bool("collect.slow_query", false, "collect slow query")
	flagSlowQueryThresholdPtr := flag.Int("collect.slow_query.threshold", 10, "slow query threshold in seconds")
	flagSlowQueryLogPathPtr := flag.String("collect.slow_query.log_path", "", "slow query log path")

	flagDataDiskUsagePtr := flag.Bool("collect.data_disk_usage", false, "collect data disk usage")
	flagDataDiskUsageScrapeIntervalPtr := flag.Int("collect.data_disk_usage.scrape_interval", 30, "data disk usage scrape interval in seconds")

	flagLogPathPtr := flag.String("log.log_path", "", "singlestore_exporter log path")
	flagLogLevel := flag.String("log.level", "info", "log level (default: info)")

	flag.Parse()
	if *versionFlag {
		fmt.Printf("singlestore_exporter version %s\n", Version)
		return
	}

	if err := log.InitLoggers(*flagLogPathPtr, *flagLogLevel, *flagSlowQueryLogPathPtr); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// only aggregator node need DSN
	dsn := os.Getenv("DATA_SOURCE_NAME")

	// scrape data_disk_usage in separate goroutine, because query execution time is too long (> 1s)
	if *flagDataDiskUsagePtr {
		ticker := time.Tick(time.Duration(*flagDataDiskUsageScrapeIntervalPtr) * time.Second)
		go func() {
			for range ticker {
				collector.ScrapeDataDiskUsages()
			}
		}()
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, newHandler(Version, dsn, *flagSlowQueryPtr, *flagSlowQueryThresholdPtr, *flagReplicationStatusPtr, *flagDataDiskUsagePtr)))

	// pprof
	if *flagPprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	log.ErrorLogger.Infof("listening on %s", *flagListenAddress)
	if err := http.ListenAndServe(*flagListenAddress, mux); err != nil {
		panic(err)
	}
}

func newHandler(
	version string,
	dsn string,
	flagSlowQuery bool,
	flagSlowQueryThreshold int,
	flagReplicationStatus bool,
	flagDataDiskUsage bool,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		registry := prometheus.NewRegistry()

		registry.MustRegister(collector.New(version, dsn, flagSlowQuery, flagSlowQueryThreshold, flagReplicationStatus, flagDataDiskUsage))

		gatherers := prometheus.Gatherers{
			prometheus.DefaultGatherer,
			registry,
		}

		h := promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	}
}
