package postgres

import (
	"fmt"
	"reflect"
	"time"

	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	tDecimal       = reflect.TypeOf(decimal.Decimal{})
	tDecimal128    = reflect.TypeOf(primitive.Decimal128{})
	tPtrDecimal    = reflect.TypeOf((*decimal.Decimal)(nil))
	tPtrDecimal128 = reflect.TypeOf((*primitive.Decimal128)(nil))
	tTime          = reflect.TypeOf(time.Time{})
	tDateTime      = reflect.TypeOf(primitive.DateTime(0))
)

// decimalCodec handles encoding/decoding between decimal.Decimal and primitive.Decimal128
type decimalCodec struct{}

var _ bsoncodec.ValueCodec = &decimalCodec{}

// EncodeValue encodes a decimal.Decimal to a BSON Decimal128
func (dc *decimalCodec) EncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tDecimal {
		return bsoncodec.ValueEncoderError{
			Name:     "DecimalEncodeValue",
			Types:    []reflect.Type{tDecimal},
			Received: val,
		}
	}

	dec := val.Interface().(decimal.Decimal)

	// Convert decimal.Decimal to primitive.Decimal128
	dec128, err := primitive.ParseDecimal128(dec.String())
	if err != nil {
		return fmt.Errorf("error converting decimal.Decimal to Decimal128: %w", err)
	}

	return vw.WriteDecimal128(dec128)
}

// DecodeValue decodes a BSON Decimal128 to a decimal.Decimal
func (dc *decimalCodec) DecodeValue(dc2 bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Type() != tDecimal {
		return bsoncodec.ValueDecoderError{
			Name:     "DecimalDecodeValue",
			Types:    []reflect.Type{tDecimal},
			Received: val,
		}
	}

	var dec128 primitive.Decimal128
	switch vr.Type() {
	case bsontype.Decimal128:
		var err error
		dec128, err = vr.ReadDecimal128()
		if err != nil {
			return err
		}
	case bsontype.Null:
		if err := vr.ReadNull(); err != nil {
			return err
		}
		val.Set(reflect.Zero(tDecimal))
		return nil
	case bsontype.Undefined:
		if err := vr.ReadUndefined(); err != nil {
			return err
		}
		val.Set(reflect.Zero(tDecimal))
		return nil
	default:
		return fmt.Errorf("cannot decode %v into a decimal.Decimal", vr.Type())
	}

	// Convert primitive.Decimal128 to decimal.Decimal
	str := dec128.String()
	dec, err := decimal.NewFromString(str)
	if err != nil {
		return fmt.Errorf("error converting Decimal128 to decimal.Decimal: %w", err)
	}

	val.Set(reflect.ValueOf(dec))
	return nil
}

// dateTimeCodec handles encoding/decoding between time.Time and primitive.DateTime
type dateTimeCodec struct{}

var _ bsoncodec.ValueCodec = &dateTimeCodec{}

// EncodeValue encodes a time.Time to a BSON DateTime
func (dtc *dateTimeCodec) EncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tTime {
		return bsoncodec.ValueEncoderError{
			Name:     "DateTimeEncodeValue",
			Types:    []reflect.Type{tTime},
			Received: val,
		}
	}

	t := val.Interface().(time.Time)
	dt := primitive.NewDateTimeFromTime(t)
	return vw.WriteDateTime(int64(dt))
}

// DecodeValue decodes a BSON DateTime to a time.Time
func (dtc *dateTimeCodec) DecodeValue(dc bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Type() != tTime {
		return bsoncodec.ValueDecoderError{
			Name:     "DateTimeDecodeValue",
			Types:    []reflect.Type{tTime},
			Received: val,
		}
	}

	var dt int64
	switch vr.Type() {
	case bsontype.DateTime:
		var err error
		dt, err = vr.ReadDateTime()
		if err != nil {
			return err
		}
	case bsontype.Null:
		if err := vr.ReadNull(); err != nil {
			return err
		}
		val.Set(reflect.Zero(tTime))
		return nil
	case bsontype.Undefined:
		if err := vr.ReadUndefined(); err != nil {
			return err
		}
		val.Set(reflect.Zero(tTime))
		return nil
	default:
		return fmt.Errorf("cannot decode %v into a time.Time", vr.Type())
	}

	// Convert primitive.DateTime to time.Time
	t := primitive.DateTime(dt).Time()
	val.Set(reflect.ValueOf(t))
	return nil
}

// NewBSONRegistry creates a new BSON registry with decimal.Decimal support
func NewBSONRegistry() *bsoncodec.Registry {
	rb := bson.NewRegistryBuilder()

	// Register the decimal codec for decimal.Decimal type
	codec := &decimalCodec{}
	rb.RegisterTypeEncoder(tDecimal, codec)
	rb.RegisterTypeDecoder(tDecimal, codec)

	// Register the dateTime codec for time.Time type
	dtCodec := &dateTimeCodec{}
	//rb.RegisterTypeEncoder(tTime, dtCodec) //we don't need custom encoder for time.Time
	rb.RegisterTypeDecoder(tTime, dtCodec)

	return rb.Build()
}
