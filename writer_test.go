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
	"os"
	"testing"

	"github.com/minio/parquet-go/gen-go/parquet"
	"github.com/minio/parquet-go/schema"
)

func TestWriter(t *testing.T) {
	schemaTree := schema.NewTree()
	{
		one, err := schema.NewElement("one", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		two, err := schema.NewElement("two", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		three, err := schema.NewElement("three", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BOOLEAN), nil, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := schemaTree.Set("one", one); err != nil {
			t.Fatal(err)
		}
		if err := schemaTree.Set("two", two); err != nil {
			t.Fatal(err)
		}
		if err := schemaTree.Set("three", three); err != nil {
			t.Fatal(err)
		}
	}

	file, err := os.Create("test.parquet")
	if err != nil {
		t.Fatal(err)
	}

	writer, err := NewWriter(file, schemaTree, 100)
	if err != nil {
		t.Fatal(err)
	}

	record := map[string]*ColumnData{
		"one": &ColumnData{
			values:           []int32{100},
			definitionLevels: []int32{0},
			repetitionLevels: []int32{0},
		},
		"two": &ColumnData{
			values:           [][]byte{[]byte("foo")},
			definitionLevels: []int32{0},
			repetitionLevels: []int32{0},
		},
		"three": &ColumnData{
			values:           []bool{true},
			definitionLevels: []int32{0},
			repetitionLevels: []int32{0},
		},
	}

	err = writer.Write(record)
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
}
