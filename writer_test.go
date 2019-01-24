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
)

func TestWriter(t *testing.T) {
	numChildren := int32(0)
	schemaElements := []*parquet.SchemaElement{
		&parquet.SchemaElement{
			Type:           parquet.TypePtr(parquet.Type_INT32),
			RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			Name:           "one",
			NumChildren:    &numChildren,
		},
		&parquet.SchemaElement{
			Type:           parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			Name:           "two",
			NumChildren:    &numChildren,
		},
		&parquet.SchemaElement{
			Type:           parquet.TypePtr(parquet.Type_BOOLEAN),
			RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			Name:           "three",
			NumChildren:    &numChildren,
		},
	}

	file, err := os.Create("test.parquet")
	if err != nil {
		t.Fatal(err)
	}

	writer, err := NewWriter(file, schemaElements, 1024)
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Write(map[string]*Value{
		"one":   &Value{int32(100), parquet.Type_INT32},
		"two":   &Value{[]byte("foo"), parquet.Type_BYTE_ARRAY},
		"three": &Value{true, parquet.Type_BOOLEAN},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
}
