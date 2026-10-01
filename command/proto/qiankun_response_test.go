package proto

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestQiankunResponseRoundTrip(t *testing.T) {
	responses := []*ExecuteQueryResponse{
		{Mutated: true, Result: &ExecuteQueryResponse_Q{Q: &QueryRows{Columns: []string{"id"}, Types: []string{"integer"}, Values: []*Values{{Parameters: []*Parameter{{Value: &Parameter_I{I: 42}}}}}}}},
		{ErrorCode: 19, ErrorExtendedCode: 2067, Result: &ExecuteQueryResponse_Error{Error: "constraint failed"}},
		{Result: &ExecuteQueryResponse_Q{Q: &QueryRows{Error: "query failed", ErrorCode: 1, ErrorExtendedCode: 1}}},
	}
	for _, original := range responses {
		wire, err := proto.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		var decoded ExecuteQueryResponse
		if err := proto.Unmarshal(wire, &decoded); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(original, &decoded) {
			t.Fatalf("mutation, result or SQLite error lost in protocol round trip: %v", &decoded)
		}
	}
}
