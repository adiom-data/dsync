// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        (unknown)
// source: adiom/v1/adiom.proto

package adiomv1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var File_adiom_v1_adiom_proto protoreflect.FileDescriptor

var file_adiom_v1_adiom_proto_rawDesc = []byte{
	0x0a, 0x14, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2f, 0x76, 0x31, 0x2f, 0x61, 0x64, 0x69, 0x6f, 0x6d,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31,
	0x1a, 0x17, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x32, 0xfc, 0x04, 0x0a, 0x10, 0x43, 0x6f,
	0x6e, 0x6e, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x3e,
	0x0a, 0x07, 0x47, 0x65, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x18, 0x2e, 0x61, 0x64, 0x69, 0x6f,
	0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x47,
	0x65, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x65,
	0x0a, 0x14, 0x47, 0x65, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x4d, 0x65,
	0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x25, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76,
	0x31, 0x2e, 0x47, 0x65, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x4d, 0x65,
	0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x26, 0x2e,
	0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x4e, 0x61, 0x6d, 0x65,
	0x73, 0x70, 0x61, 0x63, 0x65, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x41, 0x0a, 0x08, 0x4c, 0x69, 0x73, 0x74, 0x44, 0x61, 0x74,
	0x61, 0x12, 0x19, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x4c, 0x69, 0x73,
	0x74, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1a, 0x2e, 0x61,
	0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x44, 0x61, 0x74, 0x61,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x44, 0x0a, 0x09, 0x57, 0x72, 0x69, 0x74,
	0x65, 0x44, 0x61, 0x74, 0x61, 0x12, 0x1a, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31,
	0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1b, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x57, 0x72, 0x69,
	0x74, 0x65, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x4d,
	0x0a, 0x0c, 0x57, 0x72, 0x69, 0x74, 0x65, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x73, 0x12, 0x1d,
	0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x55,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1e, 0x2e,
	0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x55, 0x70,
	0x64, 0x61, 0x74, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x4d, 0x0a,
	0x0c, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x50, 0x6c, 0x61, 0x6e, 0x12, 0x1d, 0x2e,
	0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74,
	0x65, 0x50, 0x6c, 0x61, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1e, 0x2e, 0x61,
	0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65,
	0x50, 0x6c, 0x61, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x52, 0x0a, 0x0d,
	0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x73, 0x12, 0x1e, 0x2e,
	0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x55,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1f, 0x2e,
	0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x55,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x30, 0x01,
	0x12, 0x46, 0x0a, 0x09, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x4c, 0x53, 0x4e, 0x12, 0x1a, 0x2e,
	0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x4c,
	0x53, 0x4e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1b, 0x2e, 0x61, 0x64, 0x69, 0x6f,
	0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x4c, 0x53, 0x4e, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x30, 0x01, 0x42, 0x32, 0x5a, 0x30, 0x67, 0x69, 0x74, 0x68,
	0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2d, 0x64, 0x61, 0x74,
	0x61, 0x2f, 0x64, 0x73, 0x79, 0x6e, 0x63, 0x2f, 0x67, 0x65, 0x6e, 0x2f, 0x61, 0x64, 0x69, 0x6f,
	0x6d, 0x2f, 0x76, 0x31, 0x3b, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x76, 0x31, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
}

var file_adiom_v1_adiom_proto_goTypes = []any{
	(*GetInfoRequest)(nil),               // 0: adiom.v1.GetInfoRequest
	(*GetNamespaceMetadataRequest)(nil),  // 1: adiom.v1.GetNamespaceMetadataRequest
	(*ListDataRequest)(nil),              // 2: adiom.v1.ListDataRequest
	(*WriteDataRequest)(nil),             // 3: adiom.v1.WriteDataRequest
	(*WriteUpdatesRequest)(nil),          // 4: adiom.v1.WriteUpdatesRequest
	(*GeneratePlanRequest)(nil),          // 5: adiom.v1.GeneratePlanRequest
	(*StreamUpdatesRequest)(nil),         // 6: adiom.v1.StreamUpdatesRequest
	(*StreamLSNRequest)(nil),             // 7: adiom.v1.StreamLSNRequest
	(*GetInfoResponse)(nil),              // 8: adiom.v1.GetInfoResponse
	(*GetNamespaceMetadataResponse)(nil), // 9: adiom.v1.GetNamespaceMetadataResponse
	(*ListDataResponse)(nil),             // 10: adiom.v1.ListDataResponse
	(*WriteDataResponse)(nil),            // 11: adiom.v1.WriteDataResponse
	(*WriteUpdatesResponse)(nil),         // 12: adiom.v1.WriteUpdatesResponse
	(*GeneratePlanResponse)(nil),         // 13: adiom.v1.GeneratePlanResponse
	(*StreamUpdatesResponse)(nil),        // 14: adiom.v1.StreamUpdatesResponse
	(*StreamLSNResponse)(nil),            // 15: adiom.v1.StreamLSNResponse
}
var file_adiom_v1_adiom_proto_depIdxs = []int32{
	0,  // 0: adiom.v1.ConnectorService.GetInfo:input_type -> adiom.v1.GetInfoRequest
	1,  // 1: adiom.v1.ConnectorService.GetNamespaceMetadata:input_type -> adiom.v1.GetNamespaceMetadataRequest
	2,  // 2: adiom.v1.ConnectorService.ListData:input_type -> adiom.v1.ListDataRequest
	3,  // 3: adiom.v1.ConnectorService.WriteData:input_type -> adiom.v1.WriteDataRequest
	4,  // 4: adiom.v1.ConnectorService.WriteUpdates:input_type -> adiom.v1.WriteUpdatesRequest
	5,  // 5: adiom.v1.ConnectorService.GeneratePlan:input_type -> adiom.v1.GeneratePlanRequest
	6,  // 6: adiom.v1.ConnectorService.StreamUpdates:input_type -> adiom.v1.StreamUpdatesRequest
	7,  // 7: adiom.v1.ConnectorService.StreamLSN:input_type -> adiom.v1.StreamLSNRequest
	8,  // 8: adiom.v1.ConnectorService.GetInfo:output_type -> adiom.v1.GetInfoResponse
	9,  // 9: adiom.v1.ConnectorService.GetNamespaceMetadata:output_type -> adiom.v1.GetNamespaceMetadataResponse
	10, // 10: adiom.v1.ConnectorService.ListData:output_type -> adiom.v1.ListDataResponse
	11, // 11: adiom.v1.ConnectorService.WriteData:output_type -> adiom.v1.WriteDataResponse
	12, // 12: adiom.v1.ConnectorService.WriteUpdates:output_type -> adiom.v1.WriteUpdatesResponse
	13, // 13: adiom.v1.ConnectorService.GeneratePlan:output_type -> adiom.v1.GeneratePlanResponse
	14, // 14: adiom.v1.ConnectorService.StreamUpdates:output_type -> adiom.v1.StreamUpdatesResponse
	15, // 15: adiom.v1.ConnectorService.StreamLSN:output_type -> adiom.v1.StreamLSNResponse
	8,  // [8:16] is the sub-list for method output_type
	0,  // [0:8] is the sub-list for method input_type
	0,  // [0:0] is the sub-list for extension type_name
	0,  // [0:0] is the sub-list for extension extendee
	0,  // [0:0] is the sub-list for field type_name
}

func init() { file_adiom_v1_adiom_proto_init() }
func file_adiom_v1_adiom_proto_init() {
	if File_adiom_v1_adiom_proto != nil {
		return
	}
	file_adiom_v1_messages_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_adiom_v1_adiom_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_adiom_v1_adiom_proto_goTypes,
		DependencyIndexes: file_adiom_v1_adiom_proto_depIdxs,
	}.Build()
	File_adiom_v1_adiom_proto = out.File
	file_adiom_v1_adiom_proto_rawDesc = nil
	file_adiom_v1_adiom_proto_goTypes = nil
	file_adiom_v1_adiom_proto_depIdxs = nil
}