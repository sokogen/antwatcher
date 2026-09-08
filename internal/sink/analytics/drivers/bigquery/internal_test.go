package bigquery

import (
	"errors"
	"testing"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sokogen/antwatcher/internal/sink"
)

func TestWithRowErrors(t *testing.T) {
	base := status.Error(codes.InvalidArgument, "append failed")

	assert.Equal(t, base, withRowErrors(base, nil), "no response, error unchanged")
	assert.Equal(t, base, withRowErrors(base, &storagepb.AppendRowsResponse{}), "no row errors, error unchanged")

	resp := &storagepb.AppendRowsResponse{RowErrors: []*storagepb.RowError{
		{Index: 0, Code: storagepb.RowError_FIELDS_ERROR, Message: "field record_id: required"},
		{Index: 2, Code: storagepb.RowError_FIELDS_ERROR, Message: "field kind: bad"},
		{Index: 3, Code: storagepb.RowError_FIELDS_ERROR, Message: "third"},
		{Index: 4, Code: storagepb.RowError_FIELDS_ERROR, Message: "fourth"},
		{Index: 5, Code: storagepb.RowError_FIELDS_ERROR, Message: "fifth"},
	}}
	err := withRowErrors(base, resp)
	require.ErrorIs(t, err, base, "the status stays in the chain")
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.True(t, sink.IsPermanent(classify(err)))
	msg := err.Error()
	assert.Contains(t, msg, "row 0 FIELDS_ERROR: field record_id: required")
	assert.Contains(t, msg, "row 2 FIELDS_ERROR: field kind: bad")
	assert.Contains(t, msg, "row 3 FIELDS_ERROR: third")
	assert.Contains(t, msg, "and 2 more")
	assert.NotContains(t, msg, "fourth")
}

func TestClassify_Nil(t *testing.T) {
	require.NoError(t, classify(nil))
	plain := errors.New("x")
	assert.Equal(t, plain, classify(plain))
}
