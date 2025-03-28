// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.1
// 	protoc        (unknown)
// source: adiom/v1/vector.proto

package adiomv1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type GetChunkedRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Data          [][]byte               `protobuf:"bytes,1,rep,name=data,proto3" json:"data,omitempty"`
	Type          DataType               `protobuf:"varint,2,opt,name=type,proto3,enum=adiom.v1.DataType" json:"type,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetChunkedRequest) Reset() {
	*x = GetChunkedRequest{}
	mi := &file_adiom_v1_vector_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetChunkedRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetChunkedRequest) ProtoMessage() {}

func (x *GetChunkedRequest) ProtoReflect() protoreflect.Message {
	mi := &file_adiom_v1_vector_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetChunkedRequest.ProtoReflect.Descriptor instead.
func (*GetChunkedRequest) Descriptor() ([]byte, []int) {
	return file_adiom_v1_vector_proto_rawDescGZIP(), []int{0}
}

func (x *GetChunkedRequest) GetData() [][]byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *GetChunkedRequest) GetType() DataType {
	if x != nil {
		return x.Type
	}
	return DataType_DATA_TYPE_UNKNOWN
}

type Chunked struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Data          [][]byte               `protobuf:"bytes,1,rep,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Chunked) Reset() {
	*x = Chunked{}
	mi := &file_adiom_v1_vector_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Chunked) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Chunked) ProtoMessage() {}

func (x *Chunked) ProtoReflect() protoreflect.Message {
	mi := &file_adiom_v1_vector_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Chunked.ProtoReflect.Descriptor instead.
func (*Chunked) Descriptor() ([]byte, []int) {
	return file_adiom_v1_vector_proto_rawDescGZIP(), []int{1}
}

func (x *Chunked) GetData() [][]byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type GetChunkedResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Data          []*Chunked             `protobuf:"bytes,1,rep,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetChunkedResponse) Reset() {
	*x = GetChunkedResponse{}
	mi := &file_adiom_v1_vector_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetChunkedResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetChunkedResponse) ProtoMessage() {}

func (x *GetChunkedResponse) ProtoReflect() protoreflect.Message {
	mi := &file_adiom_v1_vector_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetChunkedResponse.ProtoReflect.Descriptor instead.
func (*GetChunkedResponse) Descriptor() ([]byte, []int) {
	return file_adiom_v1_vector_proto_rawDescGZIP(), []int{2}
}

func (x *GetChunkedResponse) GetData() []*Chunked {
	if x != nil {
		return x.Data
	}
	return nil
}

type GetEmbeddingRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Data          [][]byte               `protobuf:"bytes,1,rep,name=data,proto3" json:"data,omitempty"`
	Type          DataType               `protobuf:"varint,2,opt,name=type,proto3,enum=adiom.v1.DataType" json:"type,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetEmbeddingRequest) Reset() {
	*x = GetEmbeddingRequest{}
	mi := &file_adiom_v1_vector_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetEmbeddingRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetEmbeddingRequest) ProtoMessage() {}

func (x *GetEmbeddingRequest) ProtoReflect() protoreflect.Message {
	mi := &file_adiom_v1_vector_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetEmbeddingRequest.ProtoReflect.Descriptor instead.
func (*GetEmbeddingRequest) Descriptor() ([]byte, []int) {
	return file_adiom_v1_vector_proto_rawDescGZIP(), []int{3}
}

func (x *GetEmbeddingRequest) GetData() [][]byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *GetEmbeddingRequest) GetType() DataType {
	if x != nil {
		return x.Type
	}
	return DataType_DATA_TYPE_UNKNOWN
}

type Embedding struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Data          []float64              `protobuf:"fixed64,1,rep,packed,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Embedding) Reset() {
	*x = Embedding{}
	mi := &file_adiom_v1_vector_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Embedding) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Embedding) ProtoMessage() {}

func (x *Embedding) ProtoReflect() protoreflect.Message {
	mi := &file_adiom_v1_vector_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Embedding.ProtoReflect.Descriptor instead.
func (*Embedding) Descriptor() ([]byte, []int) {
	return file_adiom_v1_vector_proto_rawDescGZIP(), []int{4}
}

func (x *Embedding) GetData() []float64 {
	if x != nil {
		return x.Data
	}
	return nil
}

type GetEmbeddingResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Data          []*Embedding           `protobuf:"bytes,1,rep,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetEmbeddingResponse) Reset() {
	*x = GetEmbeddingResponse{}
	mi := &file_adiom_v1_vector_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetEmbeddingResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetEmbeddingResponse) ProtoMessage() {}

func (x *GetEmbeddingResponse) ProtoReflect() protoreflect.Message {
	mi := &file_adiom_v1_vector_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetEmbeddingResponse.ProtoReflect.Descriptor instead.
func (*GetEmbeddingResponse) Descriptor() ([]byte, []int) {
	return file_adiom_v1_vector_proto_rawDescGZIP(), []int{5}
}

func (x *GetEmbeddingResponse) GetData() []*Embedding {
	if x != nil {
		return x.Data
	}
	return nil
}

type GetSupportedDataTypesRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetSupportedDataTypesRequest) Reset() {
	*x = GetSupportedDataTypesRequest{}
	mi := &file_adiom_v1_vector_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetSupportedDataTypesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetSupportedDataTypesRequest) ProtoMessage() {}

func (x *GetSupportedDataTypesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_adiom_v1_vector_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetSupportedDataTypesRequest.ProtoReflect.Descriptor instead.
func (*GetSupportedDataTypesRequest) Descriptor() ([]byte, []int) {
	return file_adiom_v1_vector_proto_rawDescGZIP(), []int{6}
}

type GetSupportedDataTypesResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Types         []DataType             `protobuf:"varint,1,rep,packed,name=types,proto3,enum=adiom.v1.DataType" json:"types,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetSupportedDataTypesResponse) Reset() {
	*x = GetSupportedDataTypesResponse{}
	mi := &file_adiom_v1_vector_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetSupportedDataTypesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetSupportedDataTypesResponse) ProtoMessage() {}

func (x *GetSupportedDataTypesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_adiom_v1_vector_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetSupportedDataTypesResponse.ProtoReflect.Descriptor instead.
func (*GetSupportedDataTypesResponse) Descriptor() ([]byte, []int) {
	return file_adiom_v1_vector_proto_rawDescGZIP(), []int{7}
}

func (x *GetSupportedDataTypesResponse) GetTypes() []DataType {
	if x != nil {
		return x.Types
	}
	return nil
}

var File_adiom_v1_vector_proto protoreflect.FileDescriptor

var file_adiom_v1_vector_proto_rawDesc = []byte{
	0x0a, 0x15, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2f, 0x76, 0x31, 0x2f, 0x76, 0x65, 0x63, 0x74, 0x6f,
	0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76,
	0x31, 0x1a, 0x17, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x4f, 0x0a, 0x11, 0x47, 0x65,
	0x74, 0x43, 0x68, 0x75, 0x6e, 0x6b, 0x65, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0c, 0x52, 0x04, 0x64,
	0x61, 0x74, 0x61, 0x12, 0x26, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x12, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x61, 0x74,
	0x61, 0x54, 0x79, 0x70, 0x65, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x22, 0x1d, 0x0a, 0x07, 0x43,
	0x68, 0x75, 0x6e, 0x6b, 0x65, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01,
	0x20, 0x03, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x22, 0x3b, 0x0a, 0x12, 0x47, 0x65,
	0x74, 0x43, 0x68, 0x75, 0x6e, 0x6b, 0x65, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x25, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11,
	0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x68, 0x75, 0x6e, 0x6b, 0x65,
	0x64, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x22, 0x51, 0x0a, 0x13, 0x47, 0x65, 0x74, 0x45, 0x6d,
	0x62, 0x65, 0x64, 0x64, 0x69, 0x6e, 0x67, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x12,
	0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61,
	0x74, 0x61, 0x12, 0x26, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e,
	0x32, 0x12, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x61, 0x74, 0x61,
	0x54, 0x79, 0x70, 0x65, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x22, 0x1f, 0x0a, 0x09, 0x45, 0x6d,
	0x62, 0x65, 0x64, 0x64, 0x69, 0x6e, 0x67, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18,
	0x01, 0x20, 0x03, 0x28, 0x01, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x22, 0x3f, 0x0a, 0x14, 0x47,
	0x65, 0x74, 0x45, 0x6d, 0x62, 0x65, 0x64, 0x64, 0x69, 0x6e, 0x67, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x12, 0x27, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x13, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x6d, 0x62,
	0x65, 0x64, 0x64, 0x69, 0x6e, 0x67, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x22, 0x1e, 0x0a, 0x1c,
	0x47, 0x65, 0x74, 0x53, 0x75, 0x70, 0x70, 0x6f, 0x72, 0x74, 0x65, 0x64, 0x44, 0x61, 0x74, 0x61,
	0x54, 0x79, 0x70, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x49, 0x0a, 0x1d,
	0x47, 0x65, 0x74, 0x53, 0x75, 0x70, 0x70, 0x6f, 0x72, 0x74, 0x65, 0x64, 0x44, 0x61, 0x74, 0x61,
	0x54, 0x79, 0x70, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x28, 0x0a,
	0x05, 0x74, 0x79, 0x70, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0e, 0x32, 0x12, 0x2e, 0x61,
	0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x61, 0x74, 0x61, 0x54, 0x79, 0x70, 0x65,
	0x52, 0x05, 0x74, 0x79, 0x70, 0x65, 0x73, 0x32, 0xc4, 0x01, 0x0a, 0x0f, 0x43, 0x68, 0x75, 0x6e,
	0x6b, 0x69, 0x6e, 0x67, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x68, 0x0a, 0x15, 0x47,
	0x65, 0x74, 0x53, 0x75, 0x70, 0x70, 0x6f, 0x72, 0x74, 0x65, 0x64, 0x44, 0x61, 0x74, 0x61, 0x54,
	0x79, 0x70, 0x65, 0x73, 0x12, 0x26, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e,
	0x47, 0x65, 0x74, 0x53, 0x75, 0x70, 0x70, 0x6f, 0x72, 0x74, 0x65, 0x64, 0x44, 0x61, 0x74, 0x61,
	0x54, 0x79, 0x70, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x27, 0x2e, 0x61,
	0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x53, 0x75, 0x70, 0x70, 0x6f,
	0x72, 0x74, 0x65, 0x64, 0x44, 0x61, 0x74, 0x61, 0x54, 0x79, 0x70, 0x65, 0x73, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x47, 0x0a, 0x0a, 0x47, 0x65, 0x74, 0x43, 0x68, 0x75, 0x6e,
	0x6b, 0x65, 0x64, 0x12, 0x1b, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x47,
	0x65, 0x74, 0x43, 0x68, 0x75, 0x6e, 0x6b, 0x65, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x1c, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x43,
	0x68, 0x75, 0x6e, 0x6b, 0x65, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x32, 0xcb,
	0x01, 0x0a, 0x10, 0x45, 0x6d, 0x62, 0x65, 0x64, 0x64, 0x69, 0x6e, 0x67, 0x53, 0x65, 0x72, 0x76,
	0x69, 0x63, 0x65, 0x12, 0x68, 0x0a, 0x15, 0x47, 0x65, 0x74, 0x53, 0x75, 0x70, 0x70, 0x6f, 0x72,
	0x74, 0x65, 0x64, 0x44, 0x61, 0x74, 0x61, 0x54, 0x79, 0x70, 0x65, 0x73, 0x12, 0x26, 0x2e, 0x61,
	0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x53, 0x75, 0x70, 0x70, 0x6f,
	0x72, 0x74, 0x65, 0x64, 0x44, 0x61, 0x74, 0x61, 0x54, 0x79, 0x70, 0x65, 0x73, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x27, 0x2e, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e,
	0x47, 0x65, 0x74, 0x53, 0x75, 0x70, 0x70, 0x6f, 0x72, 0x74, 0x65, 0x64, 0x44, 0x61, 0x74, 0x61,
	0x54, 0x79, 0x70, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x4d, 0x0a,
	0x0c, 0x47, 0x65, 0x74, 0x45, 0x6d, 0x62, 0x65, 0x64, 0x64, 0x69, 0x6e, 0x67, 0x12, 0x1d, 0x2e,
	0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x45, 0x6d, 0x62, 0x65,
	0x64, 0x64, 0x69, 0x6e, 0x67, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1e, 0x2e, 0x61,
	0x64, 0x69, 0x6f, 0x6d, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x45, 0x6d, 0x62, 0x65, 0x64,
	0x64, 0x69, 0x6e, 0x67, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x32, 0x5a, 0x30,
	0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x64, 0x69, 0x6f, 0x6d,
	0x2d, 0x64, 0x61, 0x74, 0x61, 0x2f, 0x64, 0x73, 0x79, 0x6e, 0x63, 0x2f, 0x67, 0x65, 0x6e, 0x2f,
	0x61, 0x64, 0x69, 0x6f, 0x6d, 0x2f, 0x76, 0x31, 0x3b, 0x61, 0x64, 0x69, 0x6f, 0x6d, 0x76, 0x31,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_adiom_v1_vector_proto_rawDescOnce sync.Once
	file_adiom_v1_vector_proto_rawDescData = file_adiom_v1_vector_proto_rawDesc
)

func file_adiom_v1_vector_proto_rawDescGZIP() []byte {
	file_adiom_v1_vector_proto_rawDescOnce.Do(func() {
		file_adiom_v1_vector_proto_rawDescData = protoimpl.X.CompressGZIP(file_adiom_v1_vector_proto_rawDescData)
	})
	return file_adiom_v1_vector_proto_rawDescData
}

var file_adiom_v1_vector_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_adiom_v1_vector_proto_goTypes = []any{
	(*GetChunkedRequest)(nil),             // 0: adiom.v1.GetChunkedRequest
	(*Chunked)(nil),                       // 1: adiom.v1.Chunked
	(*GetChunkedResponse)(nil),            // 2: adiom.v1.GetChunkedResponse
	(*GetEmbeddingRequest)(nil),           // 3: adiom.v1.GetEmbeddingRequest
	(*Embedding)(nil),                     // 4: adiom.v1.Embedding
	(*GetEmbeddingResponse)(nil),          // 5: adiom.v1.GetEmbeddingResponse
	(*GetSupportedDataTypesRequest)(nil),  // 6: adiom.v1.GetSupportedDataTypesRequest
	(*GetSupportedDataTypesResponse)(nil), // 7: adiom.v1.GetSupportedDataTypesResponse
	(DataType)(0),                         // 8: adiom.v1.DataType
}
var file_adiom_v1_vector_proto_depIdxs = []int32{
	8, // 0: adiom.v1.GetChunkedRequest.type:type_name -> adiom.v1.DataType
	1, // 1: adiom.v1.GetChunkedResponse.data:type_name -> adiom.v1.Chunked
	8, // 2: adiom.v1.GetEmbeddingRequest.type:type_name -> adiom.v1.DataType
	4, // 3: adiom.v1.GetEmbeddingResponse.data:type_name -> adiom.v1.Embedding
	8, // 4: adiom.v1.GetSupportedDataTypesResponse.types:type_name -> adiom.v1.DataType
	6, // 5: adiom.v1.ChunkingService.GetSupportedDataTypes:input_type -> adiom.v1.GetSupportedDataTypesRequest
	0, // 6: adiom.v1.ChunkingService.GetChunked:input_type -> adiom.v1.GetChunkedRequest
	6, // 7: adiom.v1.EmbeddingService.GetSupportedDataTypes:input_type -> adiom.v1.GetSupportedDataTypesRequest
	3, // 8: adiom.v1.EmbeddingService.GetEmbedding:input_type -> adiom.v1.GetEmbeddingRequest
	7, // 9: adiom.v1.ChunkingService.GetSupportedDataTypes:output_type -> adiom.v1.GetSupportedDataTypesResponse
	2, // 10: adiom.v1.ChunkingService.GetChunked:output_type -> adiom.v1.GetChunkedResponse
	7, // 11: adiom.v1.EmbeddingService.GetSupportedDataTypes:output_type -> adiom.v1.GetSupportedDataTypesResponse
	5, // 12: adiom.v1.EmbeddingService.GetEmbedding:output_type -> adiom.v1.GetEmbeddingResponse
	9, // [9:13] is the sub-list for method output_type
	5, // [5:9] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_adiom_v1_vector_proto_init() }
func file_adiom_v1_vector_proto_init() {
	if File_adiom_v1_vector_proto != nil {
		return
	}
	file_adiom_v1_messages_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_adiom_v1_vector_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   2,
		},
		GoTypes:           file_adiom_v1_vector_proto_goTypes,
		DependencyIndexes: file_adiom_v1_vector_proto_depIdxs,
		MessageInfos:      file_adiom_v1_vector_proto_msgTypes,
	}.Build()
	File_adiom_v1_vector_proto = out.File
	file_adiom_v1_vector_proto_rawDesc = nil
	file_adiom_v1_vector_proto_goTypes = nil
	file_adiom_v1_vector_proto_depIdxs = nil
}
