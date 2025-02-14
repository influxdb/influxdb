package influxdb2

import (
	"fmt"
	"strconv"
	"strings"
)

// Parameter is a key value pair.
type Parameter struct {
	Key   string
	Value string
}

// Int converts the Value to an int using strconv.Atoi.
func (p Parameter) Int() (int, error) {
	return strconv.Atoi(p.Value)
}

const (
	// ParameterDefaultItemSeparator is the default item separator.
	ParameterDefaultItemSeparator = ","

	// ParameterDefaultKeyValueSeparator is the default key value separator.
	ParameterDefaultKeyValueSeparator = "="
)

// DecodeParameterStr is an alias for DecodeParameterStrAdvanced using item separator and key value separator
// of ',' and '=' respectively.
func DecodeParameterStr(input string) (opts []Parameter, err error) {
	return DecodeParameterStrAdvanced(input, ParameterDefaultItemSeparator, ParameterDefaultKeyValueSeparator)
}

// DecodeParameterStrAdvanced decodes parameter strings into a []Parameter where sepItem separates each parameter, and sepKV separates the key and value.
func DecodeParameterStrAdvanced(input string, sepItem, sepKV string) (opts []Parameter, err error) {
	if input == "" {
		return nil, fmt.Errorf("empty strings can't be decoded to parameters")
	}

	o := strings.Split(input, sepItem)

	opts = make([]Parameter, len(o))

	for i, joined := range o {
		kv := strings.SplitN(joined, sepKV, 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("parameter pair '%s' is not properly encoded: does not contain kv separator '%s'", joined, sepKV)
		}

		opts[i] = Parameter{Key: kv[0], Value: kv[1]}
	}

	return opts, nil
}
