package collector

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewScrapeProcessList(t *testing.T) {
	tt := []struct {
		threshold         int
		hosts             []string
		infoPatterns      []string
		expectedThreshold int
		expectedQuery     string
	}{
		{
			threshold:         1,
			expectedThreshold: 1,
			hosts:             []string{},
			infoPatterns:      []string{},
			expectedQuery: `SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 1000) AS INFO, RPC_INFO, PLAN_ID, TRANSACTION_STATE, ROW_LOCKS_HELD, PARTITION_LOCKS_HELD, EPOCH, LWPID, RESOURCE_POOL, STMT_VERSION, REASON_FOR_QUEUEING, DATE_SUB(now(), INTERVAL time SECOND) AS SUBMITTED_TIME
FROM information_schema.PROCESSLIST`,
		},
		{
			threshold:         2,
			expectedThreshold: 2,
			hosts:             []string{"host1", "host2"},
			infoPatterns:      []string{},
			expectedQuery: `SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 1000) AS INFO, RPC_INFO, PLAN_ID, TRANSACTION_STATE, ROW_LOCKS_HELD, PARTITION_LOCKS_HELD, EPOCH, LWPID, RESOURCE_POOL, STMT_VERSION, REASON_FOR_QUEUEING, DATE_SUB(now(), INTERVAL time SECOND) AS SUBMITTED_TIME
FROM information_schema.PROCESSLIST
WHERE HOST NOT LIKE 'host1:%' AND HOST NOT LIKE 'host2:%'`,
		},
		{
			threshold:         3,
			expectedThreshold: 3,
			hosts:             []string{},
			infoPatterns:      []string{"pattern1", "pattern2"},
			expectedQuery: `SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 1000) AS INFO, RPC_INFO, PLAN_ID, TRANSACTION_STATE, ROW_LOCKS_HELD, PARTITION_LOCKS_HELD, EPOCH, LWPID, RESOURCE_POOL, STMT_VERSION, REASON_FOR_QUEUEING, DATE_SUB(now(), INTERVAL time SECOND) AS SUBMITTED_TIME
FROM information_schema.PROCESSLIST
WHERE NVL(INFO, '') NOT LIKE '%pattern1%' AND NVL(INFO, '') NOT LIKE '%pattern2%'`,
		},
		{
			threshold:         4,
			expectedThreshold: 4,
			hosts:             []string{"host1", "host2"},
			infoPatterns:      []string{"pattern1", "pattern2"},
			expectedQuery: `SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 1000) AS INFO, RPC_INFO, PLAN_ID, TRANSACTION_STATE, ROW_LOCKS_HELD, PARTITION_LOCKS_HELD, EPOCH, LWPID, RESOURCE_POOL, STMT_VERSION, REASON_FOR_QUEUEING, DATE_SUB(now(), INTERVAL time SECOND) AS SUBMITTED_TIME
FROM information_schema.PROCESSLIST
WHERE HOST NOT LIKE 'host1:%' AND HOST NOT LIKE 'host2:%' AND NVL(INFO, '') NOT LIKE '%pattern1%' AND NVL(INFO, '') NOT LIKE '%pattern2%'`,
		},
	}

	for _, tc := range tt {
		scraper := NewScrapeProcessList(tc.threshold, tc.hosts, tc.infoPatterns)
		assert.Equal(t, tc.expectedThreshold, scraper.Threshold)
		assert.Equal(t, tc.expectedQuery, scraper.Query)
	}
}
