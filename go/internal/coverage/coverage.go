// Package coverage answers one question about a test corpus: does it reach every field of
// the schema?
//
// Two bugs in two steps came from corpora that looked complete and were not — the stream
// builder dropped span LINKS because the seed has none, and every encode path dropped
// the scope's attributes and the resource's dropped count because no fixture set them.
// Both passed a byte-identical round trip, because an absent field round-trips perfectly.
// So a fixture is not trusted to be "wide" on inspection: Missing walks the
// direct-builder value tree it produces and names every field that is never present
// and every union variant that never occurs.
package coverage

import (
	"reflect"
	"sort"
	"strings"
)

// Missing returns "Node.Field" for every value-struct field that is absent everywhere in
// v, and "Union.Variant" for every union variant that never occurs, minus `allow`.
//
// It reads the value tree generically: a node is a struct whose fields are all
// dagr.Opt[T] ({V, Ok}); a union value is a struct with a Tag() method and one exported
// zero-argument accessor per variant, of which exactly one does not panic. Types the
// walk never reaches are not reported — only fields of types that do occur.
func Missing(v any, allow ...string) []string {
	w := &walker{seen: map[string]bool{}, all: map[string]bool{}}
	w.walk(reflect.ValueOf(v))
	skip := map[string]bool{}
	for _, a := range allow {
		skip[a] = true
	}
	var out []string
	for k := range w.all {
		if !w.seen[k] && !skip[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

type walker struct {
	seen map[string]bool // present at least once
	all  map[string]bool // every field / variant of every type reached
}

func isOpt(t reflect.Type) bool {
	return t.Kind() == reflect.Struct && t.NumField() == 2 &&
		t.Field(0).Name == "V" && t.Field(1).Name == "Ok" && t.Field(1).Type.Kind() == reflect.Bool
}

func (w *walker) walk(v reflect.Value) {
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			w.walk(v.Index(i))
		}
	case reflect.Struct:
		t := v.Type()
		if isOpt(t) {
			if v.Field(1).Bool() {
				w.walk(v.Field(0))
			}
			return
		}
		if _, ok := t.MethodByName("Tag"); ok {
			w.union(v)
			return
		}
		name := t.Name()
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() || !isOpt(f.Type) {
				continue
			}
			key := name + "." + f.Name
			w.all[key] = true
			if v.Field(i).Field(1).Bool() {
				w.seen[key] = true
				w.walk(v.Field(i).Field(0))
			}
		}
	}
}

// union records every variant of a union type and the one this value holds, then walks
// that variant's payload.
func (w *walker) union(v reflect.Value) {
	t := v.Type()
	name := strings.TrimSuffix(t.Name(), "Value")
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		if m.Name == "Tag" || m.Type.NumIn() != 1 || m.Type.NumOut() != 1 ||
			strings.HasSuffix(m.Name, "Len") {
			continue
		}
		key := name + "." + m.Name
		w.all[key] = true
		if out, ok := call(v.Method(i)); ok {
			w.seen[key] = true
			w.walk(out)
		}
	}
}

func call(m reflect.Value) (out reflect.Value, ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return m.Call(nil)[0], true
}

// Check fails t (any testing.TB) when Missing reports anything.
func Check(t interface {
	Helper()
	Fatalf(string, ...any)
}, v any, allow ...string) {
	t.Helper()
	if m := Missing(v, allow...); len(m) > 0 {
		t.Fatalf("the fixture never sets %d field(s)/variant(s) — a round trip cannot "+
			"see a bug in what it never writes:\n  %s", len(m), strings.Join(m, "\n  "))
	}
}

// PdataInexpressible are the OtlpCommon fields that pdata v1.67 has no accessor for, so
// no bridge can write them and no fixture can set them. Name them per test with this
// list rather than widening Missing's rules.
var PdataInexpressible = []string{
	"Resource.EntityRefs",          // Status: [Development]; no pcommon accessor yet
	"KeyValue.KeyStrindex",         // profiles-style string-table keys; no pcommon accessor
	"AnyValue.StringValueStrindex", // same, for values
}
