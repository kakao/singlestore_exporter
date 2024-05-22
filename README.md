# SingleStore Exporter

singlestore_exporter collects metrics from SingleStore (formerly MemSQL) clusters.

singlestore_exporter aims to collect node and cluster states that are not gathered by memsql_exporter, focusing on enhancing monitoring capabilities by providing insights into metrics and statuses not covered by the latter.

## Building and running

### Build

```bash
make build
```

### Run

```bash
DATA_SOURCE_NAME='{ID}:{PASSWORD]@tcp({FQDN}:{$PORT})/' ./singlestore_exporter <flags>
# When Exporter Start, You can check metrics
# http://localhost:9105/metrics

# Or you can run exporter from source code
DATA_SOURCE_NAME='{ID}:{PASSWORD]@tcp({FQDN}:{$PORT})/' go run main.go <flags>
```

## Deploy

singlestore_exporter should be run on nodes where SingleStore is installed to collect node status metrics, because it uses memsqlctl to check node's status.

For systemd integration, refer to the example service files under the deploy/ folder.

## Flags

| flag                                    | description                                          | default                       |
|-----------------------------------------|------------------------------------------------------|-------------------------------|
| collect.slow_query                      | Collect slow query metrics                           | false                         |
| collect.slow_query.threshold            | Slow query threshold in seconds                      | 10                            |
| collect.slow_query.log_path             | Path to slow query log                               | "" (logs only to the console) |
| collect.replication_status              | Collect replication status metrics                   | false                         |
| collect.data_disk_usage                 | Collect disk usage per database                      | false                         |
| collect.data_disk_usage.scrape_interval | Collect interval of disk usage per database          | 30                            |
| net.listen_address                      | Address to listen on for web interface and telemetry | 0.0.0.0:9105                  |
| log.log_path                            | Log path                                             | "" (logs only to the console) |
| log.level                               | Log level (info, warn, error, fatal, panic)          | info                          |
| debug.pprof                             | Enable pprof                                         | false                         |

## License

This software is licensed under the [Apache 2 license](LICENSE), quoted below.

Copyright 2024 Kakao Corp. <http://www.kakaocorp.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this project except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
