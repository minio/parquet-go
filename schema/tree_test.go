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

package schema

import (
	"fmt"
	"testing"

	"github.com/minio/parquet-go/gen-go/parquet"
)

func TestTreeSet(t *testing.T) {
	a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	b, err := NewElement("b", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewElement("c", parquet.FieldRepetitionType_OPTIONAL,
		parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
		nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name      string
		element   *Element
		expectErr bool
	}{
		{"A", a, false},
		{"A.B", b, false},
		{"A.B.C", c, false},
		{"B.C", nil, true},      // error: parent B does not exist
		{"A.B.C.AA", nil, true}, // error: parent A.B.C is not group element
	}

	root := NewTree()
	for i, testCase := range testCases {
		err := root.Set(testCase.name, testCase.element)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			if testCase.expectErr {
				t.Fatalf("case %v: err: expected: <error>, got: <nil>", i+1)
			} else {
				t.Fatalf("case %v: err: expected: <nil>, got: %v", i+1, err)
			}
		}
	}
}

func TestTreeGet(t *testing.T) {
	a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	b, err := NewElement("b", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewElement("c", parquet.FieldRepetitionType_OPTIONAL,
		parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
		nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	root := NewTree()
	if err := root.Set("A", a); err != nil {
		t.Fatal(err)
	}
	if err := root.Set("A.B", b); err != nil {
		t.Fatal(err)
	}
	if err := root.Set("A.B.C", c); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name            string
		expectedElement *Element
		expectedFound   bool
	}{
		{"A", a, true},
		{"A.B", b, true},
		{"A.B.C", c, true},
		{"B", nil, false},
		{"A.B.C.AA", nil, false},
	}

	for i, testCase := range testCases {
		element, found := root.Get(testCase.name)

		if element != testCase.expectedElement {
			t.Fatalf("case %v: element: expected: %v, got: %v", i+1, testCase.expectedElement, element)
		}

		if found != testCase.expectedFound {
			t.Fatalf("case %v: found: expected: %v, got: %v", i+1, testCase.expectedFound, found)
		}
	}
}

func TestTreeDelete(t *testing.T) {
	testCases := []struct {
		name          string
		expectedFound bool
	}{
		{"A", false},
		{"A.B", false},
		{"A.B.C", false},
	}

	for i, testCase := range testCases {
		a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}

		b, err := NewElement("b", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}

		c, err := NewElement("c", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}

		root := NewTree()
		if err := root.Set("A", a); err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}
		if err := root.Set("A.B", b); err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}
		if err := root.Set("A.B.C", c); err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}

		root.Delete(testCase.name)
		_, found := root.Get(testCase.name)

		if found != testCase.expectedFound {
			t.Fatalf("case %v: found: expected: %v, got: %v", i+1, testCase.expectedFound, found)
		}
	}
}

func TestTreeToParquetSchema(t *testing.T) {
	case1Root := NewTree()
	{
		a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := NewElement("b", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		c, err := NewElement("c", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := case1Root.Set("A", a); err != nil {
			t.Fatal(err)
		}
		if err := case1Root.Set("A.B", b); err != nil {
			t.Fatal(err)
		}
		if err := case1Root.Set("A.B.C", c); err != nil {
			t.Fatal(err)
		}
	}

	case2Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case2Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}
	}

	case3Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_OPTIONAL, nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST), nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case3Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}
	}

	case4Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_OPTIONAL, nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST), nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		b, err := NewElement("b", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case4Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}
		if err := case4Root.Set("Names.A", a); err != nil {
			t.Fatal(err)
		}
		if err := case4Root.Set("Names.B", b); err != nil {
			t.Fatal(err)
		}
	}

	case5Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_OPTIONAL, nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST), nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		a, err := NewElement("list", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case5Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}
		if err := case5Root.Set("Names.list", a); err != nil {
			t.Fatal(err)
		}
	}

	case6Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_OPTIONAL, nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST), nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		a, err := NewElement("list", parquet.FieldRepetitionType_REPEATED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case6Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}
		if err := case6Root.Set("Names.list", a); err != nil {
			t.Fatal(err)
		}
	}

	case7Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case7Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}
	}

	case8Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_OPTIONAL,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case8Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}
	}

	case9Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REPEATED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case9Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}
	}

	case10Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REPEATED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		key, err := NewElement("key", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case10Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case10Root.Set("NameMap.key", key); err != nil {
			t.Fatal(err)
		}
	}

	case11Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REPEATED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		key, err := NewElement("key", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		key2, err := NewElement("value", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case11Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}
		if err := case11Root.Set("NameMap.key", key); err != nil {
			t.Fatal(err)
		}
		if err := case11Root.Set("NameMap.key2", key2); err != nil {
			t.Fatal(err)
		}

		fmt.Println(case11Root)
	}

	case12Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REPEATED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		key, err := NewElement("key", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		value, err := NewElement("value", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case12Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}
		if err := case12Root.Set("NameMap.key", key); err != nil {
			t.Fatal(err)
		}
		if err := case12Root.Set("NameMap.value", value); err != nil {
			t.Fatal(err)
		}

		fmt.Println(case12Root)
	}

	case13Root := NewTree()
	{
		a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := case13Root.Set("A", a); err != nil {
			t.Fatal(err)
		}
	}

	case14Root := NewTree()
	{
		a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := case14Root.Set("A", a); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		tree      *Tree
		expectErr bool
	}{
		{case1Root, false},
		{case2Root, true}, // err: field Names of Type must be nil for LIST ConvertedType
		{case3Root, true}, // err: field Names of Children must have one element for LIST ConvertedType
		{case4Root, true}, // err: field Names of Children must have one element for LIST ConvertedType
		{case5Root, true}, // err: repetition type of Names.list element must be REPEATED
		{case6Root, false},
		{case7Root, true},  // err: field Names of Type must be nil for MAP ConvertedType
		{case8Root, true},  // err: repetition type of NameMap element must be REPEATED
		{case9Root, true},  // err: field NameMap of Children must have only two elements for MAP ConvertedType
		{case10Root, true}, // err: field NameMap of Children must have only two elements for MAP ConvertedType
		{case11Root, true}, // err: field NameMap of Children must have 'value' element for MAP ConvertedType
		{case12Root, false},
		{case13Root, true}, // err: field A of ConvertedType UTF8 must have Type value
		{case14Root, false},
	}

	for i, testCase := range testCases {
		_, _, err := testCase.tree.ToParquetSchema()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			if testCase.expectErr {
				t.Fatalf("case %v: err: expected: <error>, got: <nil>", i+1)
			} else {
				t.Fatalf("case %v: err: expected: <nil>, got: %v", i+1, err)
			}
		}
	}
}
