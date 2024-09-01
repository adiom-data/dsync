package logger

import (
	"bytes"
)

// ReverseBuffer is a custom buffer that prepends new data to the beginning of the buffer.
type ReverseBuffer struct {
	bytes.Buffer
}

// Write prepends the new data to the beginning of the buffer.
func (rb *ReverseBuffer) Write(p []byte) (n int, err error) {
	// Store existing buffer data in a temporary buffer
	tempBuffer := make([]byte, rb.Len())
	copy(tempBuffer, rb.Bytes())

	// Reset the original buffer
	rb.Reset()

	// Write the new data first
	n, err = rb.Buffer.Write(p)
	if err != nil {
		return n, err
	}

	// Append the old data back to the buffer
	_, err = rb.Buffer.Write(tempBuffer)
	if err != nil {
		return n, err
	}

	return len(p), nil
}
