package hooks

import (
	"reflect"
	"strings"
	"sync"
	"unicode"

	"github.com/ripkitten-co/whisker/internal/meta"
)

type columnInfo struct {
	name    string // SQL column name the ORM sees
	jsonKey string // JSONB field key in data column
}

type modelInfo struct {
	name       string
	table      string
	idColumn   string
	versionCol string
	dataCols   []columnInfo
	structType reflect.Type
	meta       *meta.StructMeta
}

type registry struct {
	mu      sync.RWMutex
	byName  map[string]*modelInfo
	byTable map[string]*modelInfo
	byORM   map[string]*modelInfo
}

func newRegistry() *registry {
	return &registry{
		byName:  make(map[string]*modelInfo),
		byTable: make(map[string]*modelInfo),
		byORM:   make(map[string]*modelInfo),
	}
}

func analyzeModel[T any](name string) *modelInfo {
	m := meta.Analyze[T]()
	t := reflect.TypeOf((*T)(nil)).Elem()

	var dataCols []columnInfo
	for _, f := range m.Fields {
		sf := t.Field(f.Index)
		dataCols = append(dataCols, columnInfo{
			name:    toLowerSnake(sf.Name),
			jsonKey: f.JSONKey,
		})
	}

	return &modelInfo{
		name:       name,
		table:      "whisker_" + name,
		idColumn:   "id",
		versionCol: "version",
		dataCols:   dataCols,
		structType: t,
		meta:       m,
	}
}

func (r *registry) register(name string, info *modelInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byName[name] = info
	r.byTable[info.table] = info
	r.byORM[name] = info
}

func (r *registry) lookup(name string) (*modelInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.byName[name]
	return info, ok
}

func (r *registry) lookupByTable(table string) (*modelInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.byTable[table]
	if ok {
		return info, true
	}
	info, ok = r.byORM[table]
	return info, ok
}

func toLowerSnake(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i, c := range s {
		if unicode.IsUpper(c) {
			if i > 0 {
				prev := rune(s[i-1])
				if unicode.IsLower(prev) || (i+1 < len(s) && unicode.IsLower(rune(s[i+1]))) {
					b.WriteByte('_')
				}
			}
			b.WriteRune(unicode.ToLower(c))
		} else {
			b.WriteRune(c)
		}
	}
	return b.String()
}
