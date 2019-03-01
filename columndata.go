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
	"fmt"

	"github.com/minio/parquet-go/gen-go/parquet"
)

// ColumnData - denotes values of a column.
type ColumnData struct {
	values           interface{} // must be a slice of parquet typed values.
	definitionLevels []int32     // exactly same length of values.
	repetitionLevels []int32     // exactly same length of values.
}

func (data *ColumnData) merge(dataToMerge *ColumnData, parquetType parquet.Type) {
	switch parquetType {
	case parquet.Type_BOOLEAN:
		values := data.values.([]bool)
		valuesToMerge := dataToMerge.values.([]bool)
		values = append(values, valuesToMerge...)
		data.values = values
	case parquet.Type_INT32:
		values := data.values.([]int32)
		valuesToMerge := dataToMerge.values.([]int32)
		values = append(values, valuesToMerge...)
		data.values = values
	case parquet.Type_INT64:
		values := data.values.([]int64)
		valuesToMerge := dataToMerge.values.([]int64)
		values = append(values, valuesToMerge...)
		data.values = values
	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		values := data.values.([][]byte)
		valuesToMerge := dataToMerge.values.([][]byte)
		values = append(values, valuesToMerge...)
		data.values = values
	case parquet.Type_FLOAT:
		values := data.values.([]float32)
		valuesToMerge := dataToMerge.values.([]float32)
		values = append(values, valuesToMerge...)
		data.values = values
	case parquet.Type_DOUBLE:
		values := data.values.([]float64)
		valuesToMerge := dataToMerge.values.([]float64)
		values = append(values, valuesToMerge...)
		data.values = values
	default:
		panic(fmt.Errorf("unknown parquet type %v", parquetType))
	}

	data.definitionLevels = append(data.definitionLevels, dataToMerge.definitionLevels...)
	data.repetitionLevels = append(data.repetitionLevels, dataToMerge.repetitionLevels...)
}

// NewColumnData - creates new column data.
func NewColumnData(values interface{}, definitionLevels, repetitionLevels []int32) *ColumnData {
	return &ColumnData{
		values:           values,
		definitionLevels: definitionLevels,
		repetitionLevels: repetitionLevels,
	}
}
