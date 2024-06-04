package webhook

import (
	"github.com/starlinglab/integrity-v2/aa"
)

// ParseJsonToAttributes parses a JSON map and a file stat map
// to a slice of attributes for POSTing to the AA server
func ParseJsonToAttributes(jsonMap map[string]any, fileAttributes map[string]any) []aa.PostKV {

	var attributes []aa.PostKV

	for k, v := range jsonMap {
		// TODO: add whitelist/blacklist for attributes in config
		if k != "private" {
			attributes = append(attributes, aa.PostKV{Key: k, Value: v})
		}
	}

	for k, v := range fileAttributes {
		attributes = append(attributes, aa.PostKV{Key: k, Value: v})
	}

	return attributes
}
