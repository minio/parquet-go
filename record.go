/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package parquet

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bcicen/jstream"
	parquetgen "github.com/minio/parquet-go/gen-go/parquet"
)

// Record - ordered parquet record.
type Record struct {
	nameList     []string
	nameValueMap map[string]Value
}

// String - returns string representation of this record.
func (r *Record) String() string {
	values := []string{}
	r.Range(func(name string, value Value) bool {
		values = append(values, fmt.Sprintf("%v:%v", name, value))
		return true
	})

	return "map[" + strings.Join(values, " ") + "]"
}

// MarshalJSON - marshal record as json.
func (r *Record) MarshalJSON() ([]byte, error) {
	kvs := jstream.KVS{}
	f := func(name string, v Value) bool {
		if v.Value == nil {
			kvs = append(kvs, jstream.KV{Key: name, Value: nil})
			return true
		}

		var value interface{}
		switch v.Type {
		case parquetgen.Type_BOOLEAN:
			value = v.Value.(bool)
		case parquetgen.Type_INT32:
			value = int64(v.Value.(int32))
		case parquetgen.Type_INT64:
			value = int64(v.Value.(int64))
		case parquetgen.Type_FLOAT:
			value = float64(v.Value.(float32))
		case parquetgen.Type_DOUBLE:
			value = v.Value.(float64)
		case parquetgen.Type_INT96, parquetgen.Type_BYTE_ARRAY, parquetgen.Type_FIXED_LEN_BYTE_ARRAY:
			value = string(v.Value.([]byte))
		default:
			return false
		}

		kvs = append(kvs, jstream.KV{Key: name, Value: value})
		return true
	}
	r.Range(f)
	return json.Marshal(kvs)
}

func (r *Record) set(name string, value Value) {
	r.nameValueMap[name] = value
}

// Get - returns Value of name.
func (r *Record) Get(name string) (Value, bool) {
	value, ok := r.nameValueMap[name]
	return value, ok
}

// Range - calls f sequentially for each name and value present in the record. If f returns false, range stops the iteration.
func (r *Record) Range(f func(name string, value Value) bool) {
	for _, name := range r.nameList {
		value, ok := r.nameValueMap[name]
		if !ok {
			continue
		}

		if !f(name, value) {
			break
		}
	}
}

func newRecord(nameList []string) *Record {
	return &Record{
		nameList:     nameList,
		nameValueMap: make(map[string]Value),
	}
}
