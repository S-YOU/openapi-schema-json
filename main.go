package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/iancoleman/strcase"
	"github.com/jinzhu/copier"
	"github.com/jinzhu/inflection"
	"github.com/kenshaw/snaker"
)

var (
	schemaFile = flag.String("schema", "", "input schema files")
	out        = flag.String("o", "", "output file")
)

type TypeBase = string

var baseTypes = map[string]string{
	"double":  "float64",
	"integer": "int64",
	"number":  "int64",
	"boolean": "bool",
}

type TypeLen = int64
type Type struct {
	Array  bool     `json:"array"`
	Base   TypeBase `json:"-"`
	Format string   `json:"format,omitempty"`
	Len    TypeLen  `json:"len,omitempty"`
}
type ColumnDef struct {
	Name           string `json:"namesDb"`
	NameDbSingular string `json:"nameDb"`
	NameJson       string `json:"nameJson"`
	GoName         string `json:"Name"`
	GoVarName      string `json:"name"`
	GoNames        string `json:"Names"`
	GoVarNames     string `json:"names"`
	NameExactJson  string `json:"nameExact"`
	GoType         string `json:"Type"`
	GoBaseType     string `json:"baseType"`
	Format         string `json:"format,omitempty"`
	Size           int64  `json:"size,omitempty"`
	Type           Type   `json:"-"`
	IsArray        bool   `json:"isArray"`
	NotNull        bool   `json:"notNull"`
	AutoIncrement  bool   `json:"autoIncrement,omitempty"`
	Default        string `json:"default,omitempty"`
	Comment        string `json:"comment,omitempty"`
	In             string `json:"in,omitempty"`
	Key            string `json:"key"`
}

func (x ColumnDef) MarshalJSON() ([]byte, error) {
	x.Name = plural(x.NameExactJson)
	x.GoVarName = lowerCamel(x.GoName)
	x.NameJson = strcase.ToLowerCamel(x.NameExactJson)
	x.GoNames = plural(x.GoName)
	if strings.HasSuffix(x.GoNames, "ids") {
		x.GoNames = x.GoNames[:len(x.GoNames)-3] + "Ids"
	}
	x.GoVarNames = strcase.ToLowerCamel(x.GoNames)
	if x.NameJson != "id" && strings.HasSuffix(x.NameJson, "id") {
		x.NameJson = x.NameJson[:len(x.NameJson)-2] + "Id"
	}
	x.Key = x.NameJson
	x.GoType = x.Type.Base
	x.Size = x.Type.Len
	x.IsArray = x.Type.Array
	if !x.NotNull {
		x.GoType = "*" + x.GoType
	}
	x.GoBaseType = x.GoType
	if x.Type.Array {
		x.GoType = "[]" + x.GoType
	}
	x.Format = x.Type.Format
	type MyColumnDef ColumnDef
	return json.Marshal(MyColumnDef(x))
}
func lowerCamel(s string) string {
	if s == "" {
		return ""
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToLower(r)) + s[n:]
}

var shortNameRe = regexp.MustCompile("[A-Z]")

func shortName(s string) string {
	return strings.ToLower(strings.Join(shortNameRe.FindAllString(s, -1), ""))
}

type TypeDef struct {
	GoType     string `json:"Type"`
	GoBaseType string `json:"baseType"`
	Format     string `json:"format,omitempty"`
	Size       int64  `json:"size,omitempty"`
	Type       Type   `json:"-"`
	IsArray    bool   `json:"isArray"`
	NotNull    bool   `json:"notNull"`
	Comment    string `json:"comment,omitempty"`
}

func (x TypeDef) MarshalJSON() ([]byte, error) {
	x.GoType = x.Type.Base
	x.Size = x.Type.Len
	x.IsArray = x.Type.Array
	if !x.NotNull {
		x.GoType = "*" + x.GoType
	}
	x.GoBaseType = x.GoType
	if x.Type.Array {
		x.GoType = "[]" + x.GoType
	}
	x.Format = x.Type.Format
	type MyTypeDef TypeDef
	return json.Marshal(MyTypeDef(x))
}

type Table struct {
	Name           string              `json:"namesDb"`
	NameDbSingular string              `json:"nameDb"`
	GoName         string              `json:"Name"`
	GoVarName      string              `json:"name"`
	GoNames        string              `json:"Names"`
	GoVarNames     string              `json:"names"`
	GoShortName    string              `json:"n"`
	Path           string              `json:"path,omitempty"`
	Verb           string              `json:"verb,omitempty"`
	Comment        string              `json:"comment,omitempty"`
	Key            string              `json:"key"`
	Columns        []*ColumnDef        `json:"fields"`
	Responses      map[string]*TypeDef `json:"responses,omitempty"`
	Kind           string              `json:"kind"`
}

func (x Table) MarshalJSON() ([]byte, error) {
	x.GoVarName = lowerCamel(x.GoName)
	x.GoNames = plural(x.GoName) //snaker.SnakeToCamel(plural(x.Name))
	x.GoVarNames = lowerCamel(x.GoNames)
	x.GoShortName = shortName(x.GoName)
	type MyTable Table
	return json.Marshal(MyTable(x))
}
func parseDDL(fpath string) ([]*Table, map[string]interface{}, error) {
	ctx := context.Background()
	loader := openapi3.Loader{Context: ctx, IsExternalRefsAllowed: true}
	doc, err := loader.LoadFromFile(fpath)
	if err != nil {
		return nil, nil, err
	}
	meta := make(map[string]interface{})
	ddl := doc.Components.Schemas
	paths := doc.Paths
	if doc.Info != nil {
		meta["info"] = map[string]interface{}{
			"title":       doc.Info.Title,
			"description": doc.Info.Description,
			"version":     doc.Info.Version,
		}
	}
	if doc.Servers != nil {
		servers := make([]string, len(doc.Servers))
		for i, v := range doc.Servers {
			servers[i] = v.URL
		}
		meta["servers"] = servers
	}
	tblMap := make(map[string]*Table, len(ddl)+len(paths))
	tables := make([]*Table, 0, len(ddl)+len(paths))
	colMap := make(map[string]map[string]*ColumnDef, len(ddl)+len(paths))
	for _k, _v := range ddl {
		v := _v.Value
		o := &Table{Kind: "schema"}
		if err := copier.Copy(o, v); err != nil {
			return nil, nil, err
		}
		o.GoName = snaker.ForceCamelIdentifier(_k)
		o.Key = o.GoName
		o.NameDbSingular = snaker.CamelToSnakeIdentifier(o.GoName)
		o.Name = plural(o.NameDbSingular)
		o.Comment = v.Description
		o.GoVarName = lowerCamel(inflection.Singular(o.GoName))
		o.GoNames = plural(o.GoName)
		o.GoVarNames = lowerCamel(plural(o.GoVarName))
		o.GoShortName = shortName(o.GoName)
		colMap[o.Key] = make(map[string]*ColumnDef, len(v.Properties))
		o.Columns = make([]*ColumnDef, 0, len(v.Properties))
		for kk, _vv := range v.Properties {
			vv := _vv.Value
			var notNull, autoIncrement bool
			var baseType, defaultVal string
			notNull = stringSliceContains(v.Required, kk)
			isArray := vv.Type == "array"
			format := ""
			if vv.Items != nil {
				if vv.Items.Value.Format != "" {
					format = vv.Items.Value.Format
					baseType = format
				} else {
					if vv.Items.Value.Type == "object" {
						refs := strings.Split(vv.Items.Ref, "/")
						baseType = refs[len(refs)-1]
					} else {
						baseType = vv.Items.Value.Type
					}
				}
			} else if _vv.Ref != "" {
				refs := strings.Split(_vv.Ref, "/")
				baseType = refs[len(refs)-1]
			} else {
				baseType = vv.Type
			}
			if t, ok := baseTypes[baseType]; ok {
				baseType = t
			}
			if format == "" && vv.Format != "" {
				format = vv.Format
			}
			colName := snaker.ForceCamelIdentifier(kk)
			nameDb := snaker.CamelToSnakeIdentifier(colName)
			colDef := &ColumnDef{
				GoName:         colName,
				NameDbSingular: nameDb,
				NameExactJson:  kk,
				Type: Type{
					Base:   baseType,
					Array:  isArray,
					Format: format,
				},
				NotNull:       notNull,
				Default:       defaultVal,
				AutoIncrement: autoIncrement,
				Comment:       vv.Description,
			}
			colDef.GoType = baseType
			colDef.IsArray = colDef.Type.Array
			if !colDef.NotNull {
				colDef.GoType = "*" + colDef.GoType
			}
			colDef.GoBaseType = colDef.GoType
			if colDef.Type.Array {
				colDef.GoType = "[]" + colDef.GoType
			}
			colDef.Format = colDef.Type.Format
			o.Columns = append(o.Columns, colDef)
			colMap[o.Key][colName] = colDef
		}
		tables = append(tables, o)
		tblMap[o.Key] = o
	}
	for k, _v := range paths {
		ops := map[string]*openapi3.Operation{
			"get": _v.Get, "post": _v.Post, "delete": _v.Delete, "patch": _v.Patch, "put": _v.Put,
		}
		for verb, v := range ops {
			if v == nil {
				continue
			}
			o := &Table{Kind: "path"}
			o.GoName = snaker.ForceCamelIdentifier(v.OperationID)
			o.Key = o.GoName
			o.NameDbSingular = snaker.CamelToSnakeIdentifier(o.GoName)
			o.Name = plural(o.NameDbSingular)
			o.GoVarName = lowerCamel(inflection.Singular(o.GoName))
			o.GoNames = plural(o.GoName)
			o.GoVarNames = lowerCamel(plural(o.GoVarName))
			o.GoShortName = shortName(o.GoName)
			o.Path = k
			o.Verb = verb
			o.Columns = make([]*ColumnDef, len(v.Parameters))
			for i, _vv := range v.Parameters {
				vv := _vv.Value
				notNull := vv.Required
				format := vv.Schema.Value.Format
				baseType := vv.Schema.Value.Type
				if baseType == "object" {
					refs := strings.Split(vv.Schema.Ref, "/")
					baseType = refs[len(refs)-1]
				} else {
					baseType = vv.Schema.Value.Type
				}
				if t, ok := baseTypes[baseType]; ok {
					baseType = t
				}
				colName := snaker.ForceCamelIdentifier(vv.Name)
				nameDb := snaker.CamelToSnakeIdentifier(colName)
				namesDb := plural(nameDb)
				colDef := &ColumnDef{
					GoName:         colName,
					NameDbSingular: nameDb,
					Name:           namesDb,
					In:             vv.In,
					Type: Type{
						Base:   baseType,
						Format: format,
					},
					NotNull: notNull,
					Comment: vv.Description,
				}
				colDef.GoType = baseType
				colDef.IsArray = colDef.Type.Array
				if !colDef.NotNull {
					colDef.GoType = "*" + colDef.GoType
				}
				colDef.GoBaseType = colDef.GoType
				if colDef.Type.Array {
					colDef.GoType = "[]" + colDef.GoType
				}
				colDef.Format = colDef.Type.Format
				o.Columns[i] = colDef
			}
			o.Responses = make(map[string]*TypeDef, len(v.Responses))
			for code, _vv := range v.Responses {
				content := _vv.Value.Content.Get("application/json")
				if content == nil {
					continue
				}
				vv := content.Schema.Value
				isArray := vv.Type == "array"
				format := ""
				baseType := vv.Type
				if vv.Items != nil {
					if vv.Items.Value.Format != "" {
						format = vv.Items.Value.Format
						baseType = format
					} else {
						if vv.Items.Value.Type == "object" {
							refs := strings.Split(vv.Items.Ref, "/")
							baseType = refs[len(refs)-1]
						} else {
							baseType = vv.Items.Value.Type
						}
					}
				} else if content.Schema.Ref != "" {
					refs := strings.Split(content.Schema.Ref, "/")
					baseType = refs[len(refs)-1]
				} else {
					baseType = vv.Type
				}
				if t, ok := baseTypes[baseType]; ok {
					baseType = t
				}
				if format == "" && vv.Format != "" {
					format = vv.Format
				}
				comment := ""
				if _vv.Value.Description != nil {
					comment = *_vv.Value.Description
				}
				colDef := &TypeDef{
					Type: Type{
						Base:   baseType,
						Format: format,
						Array:  isArray,
					},
					NotNull: true,
					Comment: comment,
				}
				colDef.GoType = baseType
				colDef.IsArray = colDef.Type.Array
				if !colDef.NotNull {
					colDef.GoType = "*" + colDef.GoType
				}
				colDef.GoBaseType = colDef.GoType
				if colDef.Type.Array {
					colDef.GoType = "[]" + colDef.GoType
				}
				colDef.Format = colDef.Type.Format
				o.Responses[code] = colDef
			}
			tables = append(tables, o)
			tblMap[o.Key] = o
		}
	}
	return tables, meta, nil
}

type FileContent struct {
	FileKind string                 `json:"kind"`
	SrcKind  string                 `json:"srcKind"`
	Data     []*Table               `json:"data"`
	Meta     map[string]interface{} `json:"meta"`
}

func process() error {
	parsed, meta, err := parseDDL(*schemaFile)
	if err != nil {
		return err
	}
	sort.SliceStable(parsed, func(i, j int) bool {
		return parsed[i].Key < parsed[j].Key
	})
	fileContent := FileContent{
		FileKind: "openapi",
		SrcKind:  "openapi",
		Data:     parsed,
		Meta:     meta,
	}
	parsedJson, err := json.MarshalIndent(fileContent, "", "\t")
	if err != nil {
		return err
	}
	if *out == "-" {
		if _, err := os.Stdout.Write(parsedJson); err != nil {
			return err
		}
	} else {
		outFile := *out
		if outFile == "" {
			outFile = strings.Replace(*schemaFile, ".sql", ".json", 1)
		}
		if err := ioutil.WriteFile(outFile, parsedJson, 0644); err != nil {
			return err
		}
	}
	return nil
}
func plural(s string) string {
	out := inflection.Plural(s)
	if out == "information" {
		return "informations"
	} else if out == "Information" {
		return "Informations"
	}
	return out
}
func main() {
	flag.Parse()
	if err := process(); err != nil {
		log.Fatalln(err)
	}
}
func stringSliceContains(in []string, s string) bool {
	for _, x := range in {
		if x == s {
			return true
		}
	}
	return false
}
