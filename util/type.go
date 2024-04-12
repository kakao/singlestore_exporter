package util

import (
	"database/sql"
	"strconv"
)

func Int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

func NullInt64ToString(nullInt64 sql.NullInt64, defaultValue string) string {
	if nullInt64.Valid {
		return Int64ToString(nullInt64.Int64)
	} else {
		return defaultValue
	}
}

func NullInt64ToFloat64(nullInt64 sql.NullInt64) float64 {
	if nullInt64.Valid {
		return float64(nullInt64.Int64)
	} else {
		return 0
	}
}

func NullStringToString(nullString sql.NullString, defaultValue string) string {
	if nullString.Valid {
		return nullString.String
	} else {
		return defaultValue
	}
}
