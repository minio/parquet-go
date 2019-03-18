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
	"strings"

	"github.com/minio/parquet-go/gen-go/parquet"
)

func setMaxDefinitionLevel(schemaMap map[string]*Element, maxDL int) {
	for _, element := range schemaMap {
		element.MaxDefinitionLevel = maxDL
		switch *element.RepetitionType {
		case parquet.FieldRepetitionType_REQUIRED:
		case parquet.FieldRepetitionType_OPTIONAL, parquet.FieldRepetitionType_REPEATED:
			element.MaxDefinitionLevel++
		}

		if element.Children != nil {
			setMaxDefinitionLevel(element.Children.schemaMap, element.MaxDefinitionLevel)
		}
	}
}

func setMaxRepetitionLevel(schemaMap map[string]*Element, maxRL int) {
	for _, element := range schemaMap {
		element.MaxRepetitionLevel = maxRL
		switch *element.RepetitionType {
		case parquet.FieldRepetitionType_REQUIRED, parquet.FieldRepetitionType_OPTIONAL:
		case parquet.FieldRepetitionType_REPEATED:
			element.MaxRepetitionLevel++
		}

		if element.Children != nil {
			setMaxRepetitionLevel(element.Children.schemaMap, element.MaxRepetitionLevel)
		}
	}
}

func toParquetSchema(tree *Tree, treePrefix string, schemaPrefix string, schemaList *[]*parquet.SchemaElement, valueElements *[]*Element) (err error) {
	tree.Range(func(name string, element *Element) bool {
		pathInTree := name
		if treePrefix != "" {
			pathInTree = treePrefix + "." + name
		}

		if element.ConvertedType != nil {
			switch *element.ConvertedType {
			case parquet.ConvertedType_LIST:
				if element.Type != nil {
					err = fmt.Errorf("field %v of Type must be nil for LIST ConvertedType", pathInTree)
					return false
				}

				if element.Children == nil || element.Children.Length() != 1 {
					err = fmt.Errorf("field %v of Children must have one element only for LIST ConvertedType", pathInTree)
					return false
				}

				listElement, ok := element.Children.Get("list")
				if !ok {
					err = fmt.Errorf("field %v of Children must have 'list' element for LIST ConvertedType", pathInTree)
					return false
				}

				if *listElement.RepetitionType != parquet.FieldRepetitionType_REPEATED {
					err = fmt.Errorf("repetition type of %v.list element must be REPEATED", pathInTree)
					return false
				}

			case parquet.ConvertedType_MAP:
				if element.Type != nil {
					err = fmt.Errorf("field %v of Type must be nil for MAP ConvertedType", pathInTree)
					return false
				}

				if *element.RepetitionType != parquet.FieldRepetitionType_REPEATED {
					err = fmt.Errorf("repetition type of %v element must be REPEATED", pathInTree)
					return false
				}

				if element.Children == nil || element.Children.Length() != 2 {
					err = fmt.Errorf("field %v of Children must have two elements only for MAP ConvertedType", pathInTree)
					return false
				}

				keyElement, ok := element.Children.Get("key")
				if !ok {
					err = fmt.Errorf("field %v of Children must have 'key' element for MAP ConvertedType", pathInTree)
					return false
				}

				if *keyElement.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
					err = fmt.Errorf("repetition type of %v.key element must be REQUIRED", pathInTree)
					return false
				}

				if _, ok = element.Children.Get("value"); !ok {
					err = fmt.Errorf("field %v of Children must have 'value' element for MAP ConvertedType", pathInTree)
					return false
				}

			default:
				if element.Type == nil {
					err = fmt.Errorf("field %v of ConvertedType %v must have Type value", pathInTree, element.ConvertedType)
					return false
				}
			}
		}

		element.PathInTree = pathInTree
		element.PathInSchema = element.Name
		if schemaPrefix != "" {
			element.PathInSchema = schemaPrefix + "." + element.Name
		}

		if element.Type != nil {
			*valueElements = append(*valueElements, element)
		}

		*schemaList = append(*schemaList, &element.SchemaElement)
		if element.Children != nil {
			element.numChildren = int32(element.Children.Length())
			err = toParquetSchema(element.Children, element.PathInTree, element.PathInSchema, schemaList, valueElements)
			if err != nil {
				return false
			}
		}

		return true
	})

	return err
}

// Tree - represents tree of schema.  Tree preserves order in which elements are added.
type Tree struct {
	schemaMap map[string]*Element
	keys      []string
}

func (tree *Tree) String() string {
	var s []string
	tree.Range(func(name string, element *Element) bool {
		s = append(s, fmt.Sprintf("%v: %v", name, element))
		return true
	})

	return "{" + strings.Join(s, ", ") + "}"
}

// Length - returns length of tree.
func (tree *Tree) Length() int {
	return len(tree.keys)
}

func (tree *Tree) travel(pathSegments []string) (pathSegmentIndex int, pathSegment string, currElement *Element, parentTree *Tree, found bool) {
	parentTree = tree
	for pathSegmentIndex, pathSegment = range pathSegments {
		if tree == nil {
			found = false
			break
		}

		var tmpCurrElement *Element
		if tmpCurrElement, found = tree.schemaMap[pathSegment]; !found {
			break
		}
		currElement = tmpCurrElement

		parentTree = tree
		tree = currElement.Children
	}

	return
}

// Set - adds or sets element to name.
func (tree *Tree) Set(name string, element *Element) error {
	pathSegments := strings.Split(name, ".")
	if err := validataPathSegments(pathSegments); err != nil {
		return err
	}

	i, pathSegment, currElement, parentTree, found := tree.travel(pathSegments)

	if !found {
		if i != len(pathSegments)-1 {
			return fmt.Errorf("parent %v does not exist", strings.Join(pathSegments[:i+1], "."))
		}

		if currElement == nil {
			parentTree = tree
		} else {
			if currElement.Type != nil {
				return fmt.Errorf("parent %v is not group element", strings.Join(pathSegments[:i], "."))
			}

			if currElement.Children == nil {
				currElement.Children = NewTree()
			}
			parentTree = currElement.Children
		}

		parentTree.keys = append(parentTree.keys, pathSegment)
	}

	parentTree.schemaMap[pathSegment] = element
	return nil
}

// Get - returns the element stored for name.
func (tree *Tree) Get(name string) (element *Element, ok bool) {
	pathSegments := strings.Split(name, ".")
	for _, pathSegment := range pathSegments {
		if tree == nil {
			element = nil
			ok = false
			break
		}

		if element, ok = tree.schemaMap[pathSegment]; !ok {
			break
		}

		tree = element.Children
	}

	return element, ok
}

// Delete - deletes name and its element.
func (tree *Tree) Delete(name string) {
	pathSegments := strings.Split(name, ".")

	_, pathSegment, _, parentTree, found := tree.travel(pathSegments)

	if found {
		for i := range parentTree.keys {
			if parentTree.keys[i] == pathSegment {
				copy(parentTree.keys[i:], parentTree.keys[i+1:])
				parentTree.keys = parentTree.keys[:len(parentTree.keys)-1]
				break
			}
		}

		delete(parentTree.schemaMap, pathSegment)
	}
}

// Range - calls f sequentially for each name and its element. If f returns false, range stops the iteration.
func (tree *Tree) Range(f func(name string, element *Element) bool) {
	for _, name := range tree.keys {
		if !f(name, tree.schemaMap[name]) {
			break
		}
	}
}

// ToParquetSchema - returns list of parquet SchemaElement and list of elements those stores values.
func (tree *Tree) ToParquetSchema() (schemaList []*parquet.SchemaElement, valueElements []*Element, err error) {
	setMaxDefinitionLevel(tree.schemaMap, 0)
	setMaxRepetitionLevel(tree.schemaMap, 0)

	var schemaElements []*parquet.SchemaElement
	err = toParquetSchema(tree, "", "", &schemaElements, &valueElements)
	if err != nil {
		return nil, nil, err
	}

	numChildren := int32(len(tree.keys))
	schemaList = append(schemaList, &parquet.SchemaElement{
		Name:           "schema",
		RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
		NumChildren:    &numChildren,
	})
	schemaList = append(schemaList, schemaElements...)
	return schemaList, valueElements, nil
}

// NewTree - creates new schema tree.
func NewTree() *Tree {
	return &Tree{
		schemaMap: make(map[string]*Element),
	}
}
