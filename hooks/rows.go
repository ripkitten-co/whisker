package hooks

import (
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// unpackRow extracts JSONB fields into a column-name->value map that ORM
// result scanners can consume.
func unpackRow(info *modelInfo, id string, jsonData []byte, version int) map[string]any {
	cols := make(map[string]any, len(info.dataCols)+2)
	cols[info.idColumn] = id
	cols[info.versionCol] = version

	var raw map[string]any
	if err := json.Unmarshal(jsonData, &raw); err != nil {
		return cols
	}

	for _, dc := range info.dataCols {
		if val, ok := raw[dc.jsonKey]; ok {
			cols[dc.name] = val
		}
	}

	return cols
}
