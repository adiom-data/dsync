package postgres

import (
	"fmt"
	"reflect"

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

// NewBSONRegistry creates a new BSON registry with decimal.Decimal support
func NewBSONRegistry() *bsoncodec.Registry {
	rb := bson.NewRegistryBuilder()

	// Register the decimal codec for decimal.Decimal type
	codec := &decimalCodec{}
	rb.RegisterTypeEncoder(tDecimal, codec)
	rb.RegisterTypeDecoder(tDecimal, codec)

	return rb.Build()
}
