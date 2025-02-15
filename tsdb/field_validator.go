package tsdb

import (
	"bytes"
	"fmt"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

const MaxFieldValueLength = 1048576

// ValidateFields will return a PartialWriteError if:
//   - the point has inconsistent fields, or
//   - the point has fields that are too long
func ValidateFields(mf *MeasurementFields, point models.Point, skipSizeValidation bool) ([]*FieldCreate, error) {
	pointSize := point.StringSize()
	iter := point.FieldIterator()
	var fieldsToCreate []*FieldCreate

	for iter.Next() {
		if !skipSizeValidation {
			// Check for size of field too large. Note it is much cheaper to check the whole point size
			// than checking the StringValue size (StringValue potentially takes an allocation if it must
			// unescape the string, and must at least parse the string)
			if pointSize > MaxFieldValueLength && iter.Type() == models.String {
				if sz := len(iter.StringValue()); sz > MaxFieldValueLength {
					return nil, PartialWriteError{
						Reason: fmt.Sprintf(
							"input field \"%s\" on measurement \"%s\" is too long, %d > %d",
							iter.FieldKey(), point.Name(), sz, MaxFieldValueLength),
						Dropped: 1,
					}
				}
			}
		}

		fieldKey := iter.FieldKey()
		// Skip fields name "time", they are illegal.
		if bytes.Equal(fieldKey, timeBytes) {
			continue
		}

		dataType := dataTypeFromModelsFieldType(iter.Type())
		if dataType == influxql.Unknown {
			continue
		}

		// If the field is not present, remember to create it.
		fieldName := string(fieldKey)
		f := mf.Field(fieldName)
		if f == nil {
			fieldsToCreate = append(fieldsToCreate, &FieldCreate{
				Measurement: point.Name(),
				Field: &Field{
					Name: fieldName,
					Type: dataType,
				}})
		} else if f.Type != dataType {
			// If the types are not the same, there is a conflict.
			return nil, PartialWriteError{
				Reason: fmt.Sprintf(
					"%s: input field \"%s\" on measurement \"%s\" is type %s, already exists as type %s",
					ErrFieldTypeConflict, fieldName, point.Name(), dataType, f.Type),
				Dropped: 1,
			}
		}
	}
	return fieldsToCreate, nil
}

// dataTypeFromModelsFieldType returns the influxql.DataType that corresponds to the
// passed in field type. If there is no good match, it returns Unknown.
func dataTypeFromModelsFieldType(fieldType models.FieldType) influxql.DataType {
	switch fieldType {
	case models.Float:
		return influxql.Float
	case models.Integer:
		return influxql.Integer
	case models.Unsigned:
		return influxql.Unsigned
	case models.Boolean:
		return influxql.Boolean
	case models.String:
		return influxql.String
	default:
		return influxql.Unknown
	}
}
