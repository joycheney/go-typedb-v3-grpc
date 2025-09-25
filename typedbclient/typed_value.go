package typedbclient

import (
	"fmt"
	"time"
)

// ValueType represents the type of a TypeDB value
type ValueType int

const (
	TypeNull ValueType = iota
	TypeString
	TypeBool
	TypeInt64     // All integers and count() results use this
	TypeFloat64
	TypeDate
	TypeDateTime
	TypeDuration
	TypeDecimal
	TypeConcept
	TypeConceptList
	TypeValueList
	TypeUnknown // For future compatibility
)

// String returns the string representation of ValueType
func (vt ValueType) String() string {
	switch vt {
	case TypeNull:
		return "null"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	case TypeInt64:
		return "int64"
	case TypeFloat64:
		return "float64"
	case TypeDate:
		return "date"
	case TypeDateTime:
		return "datetime"
	case TypeDuration:
		return "duration"
	case TypeDecimal:
		return "decimal"
	case TypeConcept:
		return "concept"
	case TypeConceptList:
		return "concept_list"
	case TypeValueList:
		return "value_list"
	case TypeUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("ValueType(%d)", vt)
	}
}

// Date represents a TypeDB date value
type Date struct {
	NumDaysSinceCE int32 // Days since Common Era
}

// DateTime represents a TypeDB datetime value
type DateTime struct {
	Seconds int64
	Nanos   int32
}

// ToTime converts DateTime to Go time.Time
func (dt *DateTime) ToTime() time.Time {
	return time.Unix(dt.Seconds, int64(dt.Nanos))
}

// Duration represents a TypeDB duration value
type Duration struct {
	Months int32
	Days   int32
	Nanos  int64
}

// Decimal represents a TypeDB decimal value
type Decimal struct {
	Integer    int64
	Fractional int32
}

// ToFloat64 converts Decimal to float64
func (d *Decimal) ToFloat64() float64 {
	return float64(d.Integer) + float64(d.Fractional)/1e9
}

// Concept represents a TypeDB concept (entity, relation, attribute)
type Concept struct {
	Type  string                 // "entity", "relation", "attribute", etc.
	Label string                 // Type label
	IID   string                 // Instance ID
	Value *TypedValue            // For attributes
	Links map[string][]*Concept  // For relations
}

// TypedValue represents a strongly-typed TypeDB value
type TypedValue struct {
	valueType   ValueType
	isNull      bool

	// Type-specific fields (only one will be set based on valueType)
	stringVal   string
	boolVal     bool
	int64Val    int64 // For integer, count(), long, etc.
	float64Val  float64
	dateVal     *Date
	dateTimeVal *DateTime
	durationVal *Duration
	decimalVal  *Decimal
	conceptVal  *Concept
	listVal     []TypedValue
	rawValue    interface{} // Only for TypeUnknown
}

// Type returns the value type
func (tv *TypedValue) Type() ValueType {
	return tv.valueType
}

// IsNull returns whether the value is null
func (tv *TypedValue) IsNull() bool {
	return tv.isNull
}

// AsString returns the value as string, or error if not a string
func (tv *TypedValue) AsString() (string, error) {
	if tv.isNull {
		return "", fmt.Errorf("value is null")
	}
	if tv.valueType != TypeString {
		return "", fmt.Errorf("value is %s, not string", tv.valueType)
	}
	return tv.stringVal, nil
}

// AsBool returns the value as bool, or error if not a bool
func (tv *TypedValue) AsBool() (bool, error) {
	if tv.isNull {
		return false, fmt.Errorf("value is null")
	}
	if tv.valueType != TypeBool {
		return false, fmt.Errorf("value is %s, not bool", tv.valueType)
	}
	return tv.boolVal, nil
}

// AsInt64 returns the value as int64, or error if not an integer
func (tv *TypedValue) AsInt64() (int64, error) {
	if tv.isNull {
		return 0, fmt.Errorf("value is null")
	}
	if tv.valueType != TypeInt64 {
		return 0, fmt.Errorf("value is %s, not int64", tv.valueType)
	}
	return tv.int64Val, nil
}

// AsFloat64 returns the value as float64, or error if not a float
func (tv *TypedValue) AsFloat64() (float64, error) {
	if tv.isNull {
		return 0, fmt.Errorf("value is null")
	}
	if tv.valueType != TypeFloat64 {
		return 0, fmt.Errorf("value is %s, not float64", tv.valueType)
	}
	return tv.float64Val, nil
}

// AsDate returns the value as Date, or error if not a date
func (tv *TypedValue) AsDate() (*Date, error) {
	if tv.isNull {
		return nil, fmt.Errorf("value is null")
	}
	if tv.valueType != TypeDate {
		return nil, fmt.Errorf("value is %s, not date", tv.valueType)
	}
	return tv.dateVal, nil
}

// AsDateTime returns the value as DateTime, or error if not a datetime
func (tv *TypedValue) AsDateTime() (*DateTime, error) {
	if tv.isNull {
		return nil, fmt.Errorf("value is null")
	}
	if tv.valueType != TypeDateTime {
		return nil, fmt.Errorf("value is %s, not datetime", tv.valueType)
	}
	return tv.dateTimeVal, nil
}

// AsDuration returns the value as Duration, or error if not a duration
func (tv *TypedValue) AsDuration() (*Duration, error) {
	if tv.isNull {
		return nil, fmt.Errorf("value is null")
	}
	if tv.valueType != TypeDuration {
		return nil, fmt.Errorf("value is %s, not duration", tv.valueType)
	}
	return tv.durationVal, nil
}

// AsDecimal returns the value as Decimal, or error if not a decimal
func (tv *TypedValue) AsDecimal() (*Decimal, error) {
	if tv.isNull {
		return nil, fmt.Errorf("value is null")
	}
	if tv.valueType != TypeDecimal {
		return nil, fmt.Errorf("value is %s, not decimal", tv.valueType)
	}
	return tv.decimalVal, nil
}

// AsConcept returns the value as Concept, or error if not a concept
func (tv *TypedValue) AsConcept() (*Concept, error) {
	if tv.isNull {
		return nil, fmt.Errorf("value is null")
	}
	if tv.valueType != TypeConcept {
		return nil, fmt.Errorf("value is %s, not concept", tv.valueType)
	}
	return tv.conceptVal, nil
}

// AsList returns the value as list, or error if not a list
func (tv *TypedValue) AsList() ([]TypedValue, error) {
	if tv.isNull {
		return nil, fmt.Errorf("value is null")
	}
	if tv.valueType != TypeConceptList && tv.valueType != TypeValueList {
		return nil, fmt.Errorf("value is %s, not a list", tv.valueType)
	}
	return tv.listVal, nil
}

// String returns a string representation of the typed value
func (tv *TypedValue) String() string {
	if tv.isNull {
		return "<null>"
	}
	switch tv.valueType {
	case TypeString:
		return fmt.Sprintf("string(%q)", tv.stringVal)
	case TypeBool:
		return fmt.Sprintf("bool(%v)", tv.boolVal)
	case TypeInt64:
		return fmt.Sprintf("int64(%d)", tv.int64Val)
	case TypeFloat64:
		return fmt.Sprintf("float64(%f)", tv.float64Val)
	case TypeDate:
		return fmt.Sprintf("date(%d)", tv.dateVal.NumDaysSinceCE)
	case TypeDateTime:
		return fmt.Sprintf("datetime(%s)", tv.dateTimeVal.ToTime())
	case TypeDuration:
		return fmt.Sprintf("duration(%dM%dD%dns)", tv.durationVal.Months, tv.durationVal.Days, tv.durationVal.Nanos)
	case TypeDecimal:
		return fmt.Sprintf("decimal(%f)", tv.decimalVal.ToFloat64())
	case TypeConcept:
		return fmt.Sprintf("concept(%s:%s)", tv.conceptVal.Type, tv.conceptVal.Label)
	case TypeConceptList, TypeValueList:
		return fmt.Sprintf("list[%d]", len(tv.listVal))
	case TypeUnknown:
		return fmt.Sprintf("unknown(%v)", tv.rawValue)
	default:
		return fmt.Sprintf("TypedValue(%s)", tv.valueType)
	}
}

// TypedRow represents a row with typed columns
type TypedRow struct {
	columns map[string]*TypedValue // Column name -> typed value (private for safety)
}

// GetString returns a string value from the specified column
// If the value is an attribute concept, extracts the actual value
func (tr *TypedRow) GetString(column string) (string, error) {
	val, ok := tr.columns[column]
	if !ok {
		return "", fmt.Errorf("column %q not found", column)
	}

	// If it's an attribute concept, extract its value
	if val.valueType == TypeConcept && val.conceptVal != nil &&
	   val.conceptVal.Type == "attribute" && val.conceptVal.Value != nil {
		return val.conceptVal.Value.AsString()
	}

	return val.AsString()
}

// GetBool returns a bool value from the specified column
// If the value is an attribute concept, extracts the actual value
func (tr *TypedRow) GetBool(column string) (bool, error) {
	val, ok := tr.columns[column]
	if !ok {
		return false, fmt.Errorf("column %q not found", column)
	}

	// If it's an attribute concept, extract its value
	if val.valueType == TypeConcept && val.conceptVal != nil &&
	   val.conceptVal.Type == "attribute" && val.conceptVal.Value != nil {
		return val.conceptVal.Value.AsBool()
	}

	return val.AsBool()
}

// GetInt64 returns an int64 value from the specified column
// If the value is an attribute concept, extracts the actual value
func (tr *TypedRow) GetInt64(column string) (int64, error) {
	val, ok := tr.columns[column]
	if !ok {
		return 0, fmt.Errorf("column %q not found", column)
	}

	// If it's an attribute concept, extract its value
	if val.valueType == TypeConcept && val.conceptVal != nil &&
	   val.conceptVal.Type == "attribute" && val.conceptVal.Value != nil {
		return val.conceptVal.Value.AsInt64()
	}

	return val.AsInt64()
}

// GetFloat64 returns a float64 value from the specified column
// If the value is an attribute concept, extracts the actual value
func (tr *TypedRow) GetFloat64(column string) (float64, error) {
	val, ok := tr.columns[column]
	if !ok {
		return 0, fmt.Errorf("column %q not found", column)
	}

	// If it's an attribute concept, extract its value
	if val.valueType == TypeConcept && val.conceptVal != nil &&
	   val.conceptVal.Type == "attribute" && val.conceptVal.Value != nil {
		return val.conceptVal.Value.AsFloat64()
	}

	return val.AsFloat64()
}

// GetCount returns the count value (for aggregation queries)
// Count is always returned as int64 in TypeDB v3
func (tr *TypedRow) GetCount() (int64, error) {
	// Count is typically in a column named "count" or the first column
	if val, ok := tr.columns["count"]; ok {
		return val.AsInt64()
	}
	// Try the first column if "count" not found
	for _, val := range tr.columns {
		if val.Type() == TypeInt64 {
			return val.AsInt64()
		}
	}
	return 0, fmt.Errorf("count column not found")
}

// GetDate returns a Date value from the specified column
// If the value is an attribute concept, extracts the actual value
func (tr *TypedRow) GetDate(column string) (*Date, error) {
	val, ok := tr.columns[column]
	if !ok {
		return nil, fmt.Errorf("column %q not found", column)
	}

	// If it's an attribute concept, extract its value
	if val.valueType == TypeConcept && val.conceptVal != nil &&
	   val.conceptVal.Type == "attribute" && val.conceptVal.Value != nil {
		return val.conceptVal.Value.AsDate()
	}

	return val.AsDate()
}

// GetDateTime returns a DateTime value from the specified column
// If the value is an attribute concept, extracts the actual value
func (tr *TypedRow) GetDateTime(column string) (*DateTime, error) {
	val, ok := tr.columns[column]
	if !ok {
		return nil, fmt.Errorf("column %q not found", column)
	}

	// If it's an attribute concept, extract its value
	if val.valueType == TypeConcept && val.conceptVal != nil &&
	   val.conceptVal.Type == "attribute" && val.conceptVal.Value != nil {
		return val.conceptVal.Value.AsDateTime()
	}

	return val.AsDateTime()
}

// GetDuration returns a Duration value from the specified column
func (tr *TypedRow) GetDuration(column string) (*Duration, error) {
	val, ok := tr.columns[column]
	if !ok {
		return nil, fmt.Errorf("column %q not found", column)
	}
	return val.AsDuration()
}

// GetDecimal returns a Decimal value from the specified column
func (tr *TypedRow) GetDecimal(column string) (*Decimal, error) {
	val, ok := tr.columns[column]
	if !ok {
		return nil, fmt.Errorf("column %q not found", column)
	}
	return val.AsDecimal()
}

// GetConcept returns a Concept value from the specified column
func (tr *TypedRow) GetConcept(column string) (*Concept, error) {
	val, ok := tr.columns[column]
	if !ok {
		return nil, fmt.Errorf("column %q not found", column)
	}
	return val.AsConcept()
}

// GetList returns a list value from the specified column
func (tr *TypedRow) GetList(column string) ([]TypedValue, error) {
	val, ok := tr.columns[column]
	if !ok {
		return nil, fmt.Errorf("column %q not found", column)
	}
	return val.AsList()
}

// GetValue returns the raw TypedValue from the specified column
func (tr *TypedRow) GetValue(column string) (*TypedValue, error) {
	val, ok := tr.columns[column]
	if !ok {
		return nil, fmt.Errorf("column %q not found", column)
	}
	return val, nil
}

// TypedDocument represents a document with typed fields
type TypedDocument struct {
	fields map[string]*TypedValue // Private for safety
}

// GetString returns a string value from the specified field
func (td *TypedDocument) GetString(field string) (string, error) {
	val, ok := td.fields[field]
	if !ok {
		return "", fmt.Errorf("field %q not found", field)
	}
	return val.AsString()
}

// GetInt64 returns an int64 value from the specified field
func (td *TypedDocument) GetInt64(field string) (int64, error) {
	val, ok := td.fields[field]
	if !ok {
		return 0, fmt.Errorf("field %q not found", field)
	}
	return val.AsInt64()
}

// GetValue returns the raw TypedValue from the specified field
func (td *TypedDocument) GetValue(field string) (*TypedValue, error) {
	val, ok := td.fields[field]
	if !ok {
		return nil, fmt.Errorf("field %q not found", field)
	}
	return val, nil
}