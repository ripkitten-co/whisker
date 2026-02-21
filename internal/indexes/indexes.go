package indexes

import (
	"fmt"

	"github.com/ripkitten-co/whisker/internal/meta"
)

func btreeDDL(collection, field string) string {
	return fmt.Sprintf(
		"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whisker_%s_%s ON whisker_%s ((data->>'%s'))",
		collection, field, collection, field,
	)
}

func ginDDL(collection string) string {
	return fmt.Sprintf(
		"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whisker_%s_data_gin ON whisker_%s USING GIN (data)",
		collection, collection,
	)
}

func IndexName(collection string, idx meta.IndexMeta) string {
	if idx.Type == meta.IndexGIN {
		return fmt.Sprintf("idx_whisker_%s_data_gin", collection)
	}
	return fmt.Sprintf("idx_whisker_%s_%s", collection, idx.FieldJSONKey)
}

func IndexDDLs(collection string, indexes []meta.IndexMeta) []string {
	if len(indexes) == 0 {
		return nil
	}
	ddls := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		switch idx.Type {
		case meta.IndexBtree:
			ddls = append(ddls, btreeDDL(collection, idx.FieldJSONKey))
		case meta.IndexGIN:
			ddls = append(ddls, ginDDL(collection))
		}
	}
	return ddls
}
