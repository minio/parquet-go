# Schema Tree
## Slice representation
For slice `names []string`

* with `FieldRepetitionType_REQUIRED`
```go
schema := map[string]*Element{
	"names": &Element{
		Name:          "names",
		Repetition:    parquet.FieldRepetitionType_REQUIRED,
		ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
		SchemaTree: map[string]*Element{
			"list": &Element{
				Name:          "list",
				Repetition:    parquet.FieldRepetitionType_REPEATED,
				Type:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			},
		},
	},
}
```

* with `FieldRepetitionType_OPTIONAL`
```go
schema := map[string]*Element{
	"names": &Element{
		Name:          "names",
		Repetition:    parquet.FieldRepetitionType_OPTIONAL,
		ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
		SchemaTree: map[string]*Element{
			"list": &Element{
				Name:          "list",
				Repetition:    parquet.FieldRepetitionType_REPEATED,
				Type:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			},
		},
	},
}
```

* with `FieldRepetitionType_REPEATED`
```go
schema := map[string]*Element{
	"names": &Element{
		Name:          "names",
		Repetition:    parquet.FieldRepetitionType_OPTIONAL,
		ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
		SchemaTree: map[string]*Element{
			"list": &Element{
				Name:          "list",
				Repetition:    parquet.FieldRepetitionType_REPEATED,
				Type:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			},
		},
	},
}
```

For slice `names [][]string`

* with `FieldRepetitionType_REQUIRED`
```go
schema := map[string]*Element{
	"names": &Element{
		Name:          "names",
		Repetition:    parquet.FieldRepetitionType_REQUIRED,
		ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
		SchemaTree: map[string]*Element{
			"list": &Element{
				Name:          "list",
				Repetition:    parquet.FieldRepetitionType_REPEATED,
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
				SchemaTree: map[string]*Element{
					"list": &Element{
						Name:          "list",
						Repetition:    parquet.FieldRepetitionType_REPEATED,
						Type:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
						ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
					},
				},
			},
		},
	},
}
```

* with `FieldRepetitionType_OPTIONAL`
```go
schema := map[string]*Element{
	"names": &Element{
		Name:          "names",
		Repetition:    parquet.FieldRepetitionType_OPTIONAL,
		ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
		SchemaTree: map[string]*Element{
			"list": &Element{
				Name:          "list",
				Repetition:    parquet.FieldRepetitionType_REPEATED,
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
				SchemaTree: map[string]*Element{
					"list": &Element{
						Name:          "list",
						Repetition:    parquet.FieldRepetitionType_REPEATED,
						Type:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
						ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
					},
				},
			},
		},
	},
}
```

* with `FieldRepetitionType_REPEATED`
```go
schema := map[string]*Element{
	"names": &Element{
		Name:          "names",
		Repetition:    parquet.FieldRepetitionType_OPTIONAL,
		ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
		SchemaTree: map[string]*Element{
			"list": &Element{
				Name:          "list",
				Repetition:    parquet.FieldRepetitionType_REPEATED,
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
				SchemaTree: map[string]*Element{
					"list": &Element{
						Name:          "list",
						Repetition:    parquet.FieldRepetitionType_REPEATED,
						Type:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
						ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
					},
				},
			},
		},
	},
}
```

## Map representation
For map `nameMap map[string]string` with any repetition type.
```go
schema := map[string]*Element{
	"nameMap": &Element{
		Name:          "nameMap",
		Repetition:    parquet.FieldRepetitionType_REPEATED,
		ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
		SchemaTree: map[string]*Element{
			"key": &Element{
				Name:          "key",
				Repetition:    parquet.FieldRepetitionType_REQUIRED,
				Type:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			},
			"value": &Element{
				Name:          "value",
				Repetition:    parquet.FieldRepetitionType_OPTIONAL,
				Type:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			},
		},
	},
}
```
