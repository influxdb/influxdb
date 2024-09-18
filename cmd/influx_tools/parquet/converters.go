package parquet

import (
	"fmt"
	"math"
	"strconv"
)

func toFloat(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case uint64:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case bool:
		if v {
			return float64(1), nil
		}
		return float64(0), nil
	case string:
		return strconv.ParseFloat(v, 64)
	}
	return nil, fmt.Errorf("unexpected type %T for conversion", value)
}

func toUint(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case float64:
		if v < 0 || v > math.MaxUint64 {
			return nil, fmt.Errorf("float64 value %f not convertible to uint", v)
		}
		return uint64(v), nil
	case uint64:
		return v, nil
	case int64:
		if v < 0 {
			return nil, fmt.Errorf("int64 value %d not convertible to uint", v)
		}
		return uint64(v), nil
	case bool:
		if v {
			return uint64(1), nil
		}
		return uint64(0), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	}
	return nil, fmt.Errorf("unexpected type %T for conversion", value)
}

func toInt(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case float64:
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("float64 value %f not convertible to int", v)
		}
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("uint64 value %d not convertible to int", v)
		}
		return int64(v), nil
	case int64:
		return v, nil
	case bool:
		if v {
			return int64(1), nil
		}
		return int64(0), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	}
	return nil, fmt.Errorf("unexpected type %T for conversion", value)
}

func toBool(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case float64:
		return v != 0, nil
	case uint64:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case bool:
		return v, nil
	case string:
		if v == "true" {
			return true, nil
		}
		if v == "false" {
			return false, nil
		}
		return nil, fmt.Errorf("value %q not convertible to bool", v)
	}
	return nil, fmt.Errorf("unexpected type %T for conversion", value)
}

func toString(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case string:
		return v, nil
	}
	return nil, fmt.Errorf("unexpected type %T for conversion", value)
}

func identity(value interface{}) (interface{}, error) {
	return value, nil
}
