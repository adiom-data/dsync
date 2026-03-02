package dialects_test

import (
	"fmt"
	"testing"

	"github.com/adiom-data/dsync/connectors/sqlbatch/dialects"
	"github.com/stretchr/testify/assert"
)

func TestKeySub(t *testing.T) {
	p := func(i int) string { return fmt.Sprintf("X%v", i) }
	assert.Equal(t, "(X0)", dialects.KeySub(1, 1, p))
	assert.Equal(t, "(X0), (X1), (X2)", dialects.KeySub(1, 3, p))
	assert.Equal(t, "(X0, X1, X2), (X3, X4, X5)", dialects.KeySub(3, 2, p))
	assert.Equal(t, "(X0, X1, X2)", dialects.KeySub(3, 1, p))
}
