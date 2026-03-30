package postgres

import (
	"fmt"
	"reflect"
	"time"

	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var (
	tDecimal = reflect.TypeOf(decimal.Decimal{})
	tTime    = reflect.TypeOf(time.Time{})
)

// decimalCodec handles encoding/decoding between decimal.Decimal and bson.Decimal128
type decimalCodec struct{}

var _ bson.ValueEncoder = &decimalCodec{}
var _ bson.ValueDecoder = &decimalCodec{}

// EncodeValue encodes a decimal.Decimal to a BSON Decimal128
func (dc *decimalCodec) EncodeValue(ec bson.EncodeContext, vw bson.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tDecimal {
		return bson.ValueEncoderError{
			Name:     "DecimalEncodeValue",
			Types:    []reflect.Type{tDecimal},
			Received: val,
		}
	}

	dec := val.Interface().(decimal.Decimal)

	// Convert decimal.Decimal to bson.Decimal128
	dec128, err := bson.ParseDecimal128(dec.String())
	if err != nil {
		return fmt.Errorf("error converting decimal.Decimal to Decimal128: %w", err)
	}

	return vw.WriteDecimal128(dec128)
}

// DecodeValue decodes a BSON Decimal128 to a decimal.Decimal
func (dc *decimalCodec) DecodeValue(dc2 bson.DecodeContext, vr bson.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Type() != tDecimal {
		return bson.ValueDecoderError{
			Name:     "DecimalDecodeValue",
			Types:    []reflect.Type{tDecimal},
			Received: val,
		}
	}

	var dec128 bson.Decimal128
	switch vr.Type() {
	case bson.TypeDecimal128:
		var err error
		dec128, err = vr.ReadDecimal128()
		if err != nil {
			return err
		}
	case bson.TypeNull:
		if err := vr.ReadNull(); err != nil {
			return err
		}
		val.Set(reflect.Zero(tDecimal))
		return nil
	case bson.TypeUndefined:
		if err := vr.ReadUndefined(); err != nil {
			return err
		}
		val.Set(reflect.Zero(tDecimal))
		return nil
	default:
		return fmt.Errorf("cannot decode %v into a decimal.Decimal", vr.Type())
	}

	// Convert bson.Decimal128 to decimal.Decimal
	str := dec128.String()
	dec, err := decimal.NewFromString(str)
	if err != nil {
		return fmt.Errorf("error converting Decimal128 to decimal.Decimal: %w", err)
	}

	val.Set(reflect.ValueOf(dec))
	return nil
}

// dateTimeCodec handles encoding/decoding between time.Time and bson.DateTime
type dateTimeCodec struct{}

var _ bson.ValueEncoder = &dateTimeCodec{}
var _ bson.ValueDecoder = &dateTimeCodec{}

// EncodeValue encodes a time.Time to a BSON DateTime
func (dtc *dateTimeCodec) EncodeValue(ec bson.EncodeContext, vw bson.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tTime {
		return bson.ValueEncoderError{
			Name:     "DateTimeEncodeValue",
			Types:    []reflect.Type{tTime},
			Received: val,
		}
	}

	t := val.Interface().(time.Time)
	dt := bson.NewDateTimeFromTime(t)
	return vw.WriteDateTime(int64(dt))
}

// DecodeValue decodes a BSON DateTime to a time.Time
func (dtc *dateTimeCodec) DecodeValue(dc bson.DecodeContext, vr bson.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Type() != tTime {
		return bson.ValueDecoderError{
			Name:     "DateTimeDecodeValue",
			Types:    []reflect.Type{tTime},
			Received: val,
		}
	}

	var dt int64
	switch vr.Type() {
	case bson.TypeDateTime:
		var err error
		dt, err = vr.ReadDateTime()
		if err != nil {
			return err
		}
	case bson.TypeNull:
		if err := vr.ReadNull(); err != nil {
			return err
		}
		val.Set(reflect.Zero(tTime))
		return nil
	case bson.TypeUndefined:
		if err := vr.ReadUndefined(); err != nil {
			return err
		}
		val.Set(reflect.Zero(tTime))
		return nil
	default:
		return fmt.Errorf("cannot decode %v into a time.Time", vr.Type())
	}

	// Convert bson.DateTime to time.Time
	t := bson.DateTime(dt).Time()
	val.Set(reflect.ValueOf(t.UTC()))
	return nil
}

// NewBSONRegistry creates a new BSON registry with decimal.Decimal support
func NewBSONRegistry() *bson.Registry {
	reg := bson.NewRegistry()

	// Register the decimal codec for decimal.Decimal type
	codec := &decimalCodec{}
	reg.RegisterTypeEncoder(tDecimal, codec)
	reg.RegisterTypeDecoder(tDecimal, codec)

	// Register the dateTime codec for time.Time type
	dtCodec := &dateTimeCodec{}
	//reg.RegisterTypeEncoder(tTime, dtCodec) //we don't need custom encoder for time.Time
	reg.RegisterTypeDecoder(tTime, dtCodec)

	reg.RegisterTypeMapEntry(bson.TypeDateTime, tTime)
	reg.RegisterTypeMapEntry(bson.TypeDecimal128, tDecimal)

	return reg
}
