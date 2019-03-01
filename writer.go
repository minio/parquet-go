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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/parquet-go/gen-go/parquet"
	"github.com/minio/parquet-go/schema"
)

const (
	defaultPageSize     = 8 * 1024          // 8 KiB
	defaultRowGroupSize = 128 * 1024 * 1024 // 128 MiB
)

// Writer - represents parquet writer.
type Writer struct {
	PageSize        int64
	RowGroupSize    int64
	CompressionType parquet.CompressionCodec

	writeCloser   io.WriteCloser
	numRows       int64
	offset        int64
	dictRecs      map[string]*dictRec
	footer        *parquet.FileMetaData
	schemaTree    *schema.Tree
	valueElements []*schema.Element
	columnDataMap map[string]*ColumnData
	rowGroupCount int
}

func (writer *Writer) writeData() (err error) {
	if writer.numRows == 0 {
		return nil
	}

	rowGroup := newRowGroup()
	rowGroup.RowGroupHeader.Columns = []*parquet.ColumnChunk{}

	for _, element := range writer.valueElements {
		name := element.PathInTree
		columnData, found := writer.columnDataMap[name]
		if !found {
			continue
		}

		table := new(table)
		table.Path = strings.Split(element.PathInSchema, ".")
		table.MaxDefinitionLevel = int32(element.MaxDefinitionLevel)
		table.MaxRepetitionLevel = int32(element.MaxRepetitionLevel)
		table.RepetitionType = *element.RepetitionType
		table.Type = *element.Type
		table.ConvertedType = -1
		if element.ConvertedType != nil {
			table.ConvertedType = *element.ConvertedType
		}
		table.Values = valuesToInterfaces(columnData.values, *element.Type)
		table.DefinitionLevels = columnData.definitionLevels
		table.RepetitionLevels = columnData.repetitionLevels

		var pages []*page
		if table.Encoding == parquet.Encoding_PLAIN_DICTIONARY {
			if _, ok := writer.dictRecs[name]; !ok {
				writer.dictRecs[name] = newDictRec(table.Type)
			}
			pages, _ = tableToDictDataPages(writer.dictRecs[name], table, int32(writer.PageSize), 32, writer.CompressionType)
		} else {
			pages, _ = tableToDataPages(table, int32(writer.PageSize), writer.CompressionType)
		}

		writer.dictRecs = make(map[string]*dictRec)

		// FIXME: add page encoding support.
		// if len(pages) > 0 && pages[0].Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY {
		// 	dictPage, _ := dictoRecToDictPage(writer.dictRecs[name], int32(writer.PageSize), writer.CompressionType)
		// 	tmp := append([]*page{dictPage}, pages...)
		// 	chunk = pagesToDictColumnChunk(tmp)
		// } else {
		// 	chunk = pagesToColumnChunk(pages)
		// }
		chunk := pagesToColumnChunk(pages)

		rowGroup.Chunks = append(rowGroup.Chunks, chunk)
		rowGroup.RowGroupHeader.TotalByteSize += chunk.chunkHeader.MetaData.TotalCompressedSize
		rowGroup.RowGroupHeader.Columns = append(rowGroup.RowGroupHeader.Columns, chunk.chunkHeader)
	}

	rowGroup.RowGroupHeader.NumRows = writer.numRows

	for i := 0; i < len(rowGroup.Chunks); i++ {
		rowGroup.Chunks[i].chunkHeader.MetaData.DataPageOffset = -1
		rowGroup.Chunks[i].chunkHeader.FileOffset = writer.offset

		for j := 0; j < len(rowGroup.Chunks[i].Pages); j++ {
			switch {
			case rowGroup.Chunks[i].Pages[j].Header.Type == parquet.PageType_DICTIONARY_PAGE:
				offset := writer.offset
				rowGroup.Chunks[i].chunkHeader.MetaData.DictionaryPageOffset = &offset
			case rowGroup.Chunks[i].chunkHeader.MetaData.DataPageOffset <= 0:
				rowGroup.Chunks[i].chunkHeader.MetaData.DataPageOffset = writer.offset
			}

			data := rowGroup.Chunks[i].Pages[j].RawData
			if _, err = writer.writeCloser.Write(data); err != nil {
				return err
			}

			writer.offset += int64(len(data))
		}
	}

	writer.footer.RowGroups = append(writer.footer.RowGroups, rowGroup.RowGroupHeader)
	writer.footer.NumRows += writer.numRows

	writer.numRows = 0
	writer.columnDataMap = nil

	return nil
}

func (writer *Writer) Write(record map[string]*ColumnData) (err error) {
	if writer.columnDataMap == nil {
		writer.columnDataMap = record
	} else {
		for name, columnData := range record {
			var found bool
			var element *schema.Element
			for _, element = range writer.valueElements {
				if element.PathInTree == name {
					found = true
					break
				}
			}

			if !found {
				return fmt.Errorf("%v is not value column", name)
			}

			writer.columnDataMap[name].merge(columnData, *element.Type)
		}
	}

	writer.numRows++
	if writer.numRows == int64(writer.rowGroupCount) {
		return writer.writeData()
	}

	return nil
}

func (writer *Writer) finalize() (err error) {
	if err = writer.writeData(); err != nil {
		return err
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	footerBuf, err := ts.Write(context.TODO(), writer.footer)
	if err != nil {
		return err
	}

	if _, err = writer.writeCloser.Write(footerBuf); err != nil {
		return err
	}

	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))

	if _, err = writer.writeCloser.Write(footerSizeBuf); err != nil {
		return err
	}

	_, err = writer.writeCloser.Write([]byte("PAR1"))
	return err
}

// Close - finalizes and closes writer. If any pending records are available, they are written here.
func (writer *Writer) Close() (err error) {
	if err = writer.finalize(); err != nil {
		return err
	}

	return writer.writeCloser.Close()
}

// NewWriter - creates new parquet writer. Binary data of rowGroupCount records are written to writeCloser.
func NewWriter(writeCloser io.WriteCloser, schemaTree *schema.Tree, rowGroupCount int) (*Writer, error) {
	if _, err := writeCloser.Write([]byte("PAR1")); err != nil {
		return nil, err
	}

	schemaList, valueElements, err := schemaTree.ToParquetSchema()
	if err != nil {
		return nil, err
	}

	footer := parquet.NewFileMetaData()
	footer.Version = 1
	footer.Schema = schemaList

	return &Writer{
		PageSize:        defaultPageSize,
		RowGroupSize:    defaultRowGroupSize,
		CompressionType: parquet.CompressionCodec_SNAPPY,

		writeCloser:   writeCloser,
		offset:        4,
		dictRecs:      make(map[string]*dictRec),
		footer:        footer,
		schemaTree:    schemaTree,
		valueElements: valueElements,
		rowGroupCount: rowGroupCount,
	}, nil
}
