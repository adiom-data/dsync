package mongo

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"sort"

	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"
)

var supportedIDTypes = map[bson.Type]bool{
	bson.TypeObjectID:         true,
	bson.TypeString:           true,
	bson.TypeInt32:            true,
	bson.TypeInt64:            true,
	bson.TypeBinary:           true,
	bson.TypeEmbeddedDocument: true,
}

func compareBSONRawValues(a, b bson.RawValue) int {
	if c := cmp.Compare(bsonTypeSortOrder(a.Type), bsonTypeSortOrder(b.Type)); c != 0 {
		return c
	}

	switch bsonTypeSortOrder(a.Type) {
	case bsonTypeOrderNumber:
		if c, ok := compareBSONRawNumbers(a, b); ok {
			return c
		}
	case bsonTypeOrderString:
		aString, aOK := bsonRawString(a)
		bString, bOK := bsonRawString(b)
		if aOK && bOK {
			return cmp.Compare(aString, bString)
		}
	}

	switch a.Type {
	case bson.TypeDouble:
		ai := math.Float64frombits(binary.LittleEndian.Uint64(a.Value))
		bi := math.Float64frombits(binary.LittleEndian.Uint64(b.Value))
		return cmp.Compare(ai, bi)
	case bson.TypeObjectID:
		return bytes.Compare(a.Value, b.Value)
	case bson.TypeString:
		return bytes.Compare(a.Value[4:len(a.Value)-1], b.Value[4:len(b.Value)-1])
	case bson.TypeEmbeddedDocument:
		return compareBSONRawDocuments(bson.Raw(a.Value), bson.Raw(b.Value))
	case bson.TypeArray:
		return compareBSONRawArrays(bson.RawArray(a.Value), bson.RawArray(b.Value))
	case bson.TypeInt32:
		ai := int32(binary.LittleEndian.Uint32(a.Value))
		bi := int32(binary.LittleEndian.Uint32(b.Value))
		return cmp.Compare(ai, bi)
	case bson.TypeInt64:
		ai := int64(binary.LittleEndian.Uint64(a.Value))
		bi := int64(binary.LittleEndian.Uint64(b.Value))
		return cmp.Compare(ai, bi)
	case bson.TypeBinary:
		if c, ok := compareBSONRawBinary(a, b); ok {
			return c
		}
	case bson.TypeBoolean:
		return cmp.Compare(a.Value[0], b.Value[0])
	case bson.TypeDateTime:
		ai := int64(binary.LittleEndian.Uint64(a.Value))
		bi := int64(binary.LittleEndian.Uint64(b.Value))
		return cmp.Compare(ai, bi)
	case bson.TypeTimestamp:
		at, ai := a.Timestamp()
		bt, bi := b.Timestamp()
		if c := cmp.Compare(at, bt); c != 0 {
			return c
		}
		return cmp.Compare(ai, bi)
	case bson.TypeRegex:
		ap, ao := a.Regex()
		bp, bo := b.Regex()
		if c := cmp.Compare(ap, bp); c != 0 {
			return c
		}
		return cmp.Compare(ao, bo)
	case bson.TypeJavaScript:
		return cmp.Compare(a.JavaScript(), b.JavaScript())
	case bson.TypeCodeWithScope:
		ac, as := a.CodeWithScope()
		bc, bs := b.CodeWithScope()
		if c := cmp.Compare(ac, bc); c != 0 {
			return c
		}
		return compareBSONRawDocuments(as, bs)
	case bson.TypeNull, bson.TypeMinKey, bson.TypeMaxKey:
		return 0
	}
	return bytes.Compare(a.Value, b.Value)
}

func compareBSONRawNumbers(a, b bson.RawValue) (int, bool) {
	aRank, ar, ok := bsonRawNumber(a)
	if !ok {
		return 0, false
	}
	bRank, br, ok := bsonRawNumber(b)
	if !ok {
		return 0, false
	}
	if c := cmp.Compare(aRank, bRank); c != 0 {
		return c, true
	}
	if ar == nil || br == nil {
		return 0, true
	}
	return ar.Cmp(br), true
}

func bsonRawNumber(v bson.RawValue) (int, *big.Rat, bool) {
	switch v.Type {
	case bson.TypeDouble:
		f := math.Float64frombits(binary.LittleEndian.Uint64(v.Value))
		if math.IsNaN(f) {
			return 0, nil, true
		}
		if math.IsInf(f, -1) {
			return 1, nil, true
		}
		if math.IsInf(f, 1) {
			return 3, nil, true
		}
		r := new(big.Rat)
		if r.SetFloat64(f) == nil {
			return 0, nil, false
		}
		return 2, r, true
	case bson.TypeInt32:
		i := int32(binary.LittleEndian.Uint32(v.Value))
		return 2, new(big.Rat).SetInt64(int64(i)), true
	case bson.TypeInt64:
		i := int64(binary.LittleEndian.Uint64(v.Value))
		return 2, new(big.Rat).SetInt64(i), true
	case bson.TypeDecimal128:
		d := v.Decimal128()
		if d.IsNaN() {
			return 0, nil, true
		}
		switch d.IsInf() {
		case -1:
			return 1, nil, true
		case 1:
			return 3, nil, true
		}
		r, ok := decimal128Rat(d)
		return 2, r, ok
	default:
		return 0, nil, false
	}
}

func decimal128Rat(d bson.Decimal128) (*big.Rat, bool) {
	bi, exp, err := d.BigInt()
	if err != nil {
		return nil, false
	}
	r := new(big.Rat).SetInt(bi)
	if exp == 0 {
		return r, true
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(exp))), nil)
	if exp > 0 {
		return r.Mul(r, new(big.Rat).SetInt(scale)), true
	}
	return r.Quo(r, new(big.Rat).SetInt(scale)), true
}

func abs(i int) int {
	if i < 0 {
		return -i
	}
	return i
}

func bsonRawString(v bson.RawValue) (string, bool) {
	switch v.Type {
	case bson.TypeString:
		s, ok := v.StringValueOK()
		return s, ok
	case bson.TypeSymbol:
		s, ok := v.SymbolOK()
		return s, ok
	default:
		return "", false
	}
}

func compareBSONRawBinary(a, b bson.RawValue) (int, bool) {
	if len(a.Value) < 5 || len(b.Value) < 5 {
		return 0, false
	}
	aLen := int32(binary.LittleEndian.Uint32(a.Value[:4]))
	bLen := int32(binary.LittleEndian.Uint32(b.Value[:4]))
	if c := cmp.Compare(aLen, bLen); c != 0 {
		return c, true
	}
	if c := cmp.Compare(a.Value[4], b.Value[4]); c != 0 {
		return c, true
	}
	return bytes.Compare(a.Value[5:], b.Value[5:]), true
}

func compareBSONRawDocuments(a, b bson.Raw) int {
	aElements, aErr := a.Elements()
	bElements, bErr := b.Elements()
	if aErr != nil || bErr != nil {
		return bytes.Compare(a, b)
	}

	for i := 0; i < min(len(aElements), len(bElements)); i++ {
		aValue := aElements[i].Value()
		bValue := bElements[i].Value()
		if c := cmp.Compare(bsonTypeSortOrder(aValue.Type), bsonTypeSortOrder(bValue.Type)); c != 0 {
			return c
		}
		if c := cmp.Compare(aElements[i].Key(), bElements[i].Key()); c != 0 {
			return c
		}
		if c := compareBSONRawValues(aValue, bValue); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(aElements), len(bElements))
}

func compareBSONRawArrays(a, b bson.RawArray) int {
	aValues, aErr := a.Values()
	bValues, bErr := b.Values()
	if aErr != nil || bErr != nil {
		return bytes.Compare(a, b)
	}

	for i := 0; i < min(len(aValues), len(bValues)); i++ {
		if c := compareBSONRawValues(aValues[i], bValues[i]); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(aValues), len(bValues))
}

const (
	bsonTypeOrderNumber = 3
	bsonTypeOrderString = 4
)

func bsonTypeSortOrder(t bson.Type) int {
	switch t {
	case bson.TypeMinKey:
		return 1
	case bson.TypeNull, bson.TypeUndefined:
		return 2
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
		return 3
	case bson.TypeSymbol, bson.TypeString:
		return 4
	case bson.TypeEmbeddedDocument:
		return 5
	case bson.TypeArray:
		return 6
	case bson.TypeBinary:
		return 7
	case bson.TypeObjectID:
		return 8
	case bson.TypeBoolean:
		return 9
	case bson.TypeDateTime:
		return 10
	case bson.TypeTimestamp:
		return 11
	case bson.TypeRegex:
		return 12
	case bson.TypeJavaScript:
		return 13
	case bson.TypeCodeWithScope:
		return 14
	case bson.TypeMaxKey:
		return 15
	default:
		return 14
	}
}

func (c *conn) sampleIDs(ctx context.Context, col *mongo.Collection, numSamples int64) ([]bson.RawValue, error) {
	if c.flavor != FlavorDocumentDB {
		res, err := col.Aggregate(ctx, mongo.Pipeline{
			{{"$sample", bson.D{{"size", numSamples}}}},
			{{"$project", bson.D{{"_id", 1}}}},
			{{"$sort", bson.D{{"_id", 1}}}},
		})
		if err != nil {
			return nil, fmt.Errorf("error getting %v samples: %w", numSamples, err)
		}
		defer res.Close(ctx)
		var ids []bson.RawValue
		for res.Next(ctx) {
			ids = append(ids, res.Current.Lookup("_id"))
		}
		if res.Err() != nil {
			return nil, fmt.Errorf("err iterating through samples: %w", res.Err())
		}
		return ids, nil
	}

	type sampleResult struct {
		id  bson.RawValue
		key string
	}

	results := make([]sampleResult, numSamples)
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(c.documentDBSamplingFanout())
	for i := int64(0); i < numSamples; i++ {
		i := i
		eg.Go(func() error {
			res, err := col.Aggregate(ctx, mongo.Pipeline{
				{{"$sample", bson.D{{"size", 1}}}},
				{{"$project", bson.D{{"_id", 1}}}},
			})
			if err != nil {
				return fmt.Errorf("error getting sample %v: %w", i, err)
			}
			defer res.Close(ctx)
			if res.Next(ctx) {
				id := res.Current.Lookup("_id")
				results[i] = sampleResult{id: id, key: hex.EncodeToString(id.Value)}
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	seen := map[string]struct{}{}
	var ids []bson.RawValue
	for _, r := range results {
		if r.key == "" {
			continue
		}
		if _, dup := seen[r.key]; !dup {
			seen[r.key] = struct{}{}
			ids = append(ids, r.id)
		}
	}

	if len(ids) > 0 {
		t := ids[0].Type
		for _, id := range ids[1:] {
			if id.Type != t {
				return nil, fmt.Errorf("mixed _id types not supported for DocumentDB sampling")
			}
		}
		if !supportedIDTypes[t] {
			return nil, fmt.Errorf("unsupported _id type for DocumentDB sampling: %v", t)
		}
		sort.Slice(ids, func(i, j int) bool {
			return compareBSONRawValues(ids[i], ids[j]) < 0
		})
	}

	return ids, nil
}

func (c *conn) documentDBSamplingFanout() int {
	if c.settings.DocumentDBSamplingFanout > 0 {
		return c.settings.DocumentDBSamplingFanout
	}
	return defaultDocumentDBSamplingFanoutLimit
}
