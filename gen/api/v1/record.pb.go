// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.1
// 	protoc        (unknown)
// source: api/v1/record.proto

package apiv1

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

type Record struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key      []byte    `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Payload  []byte    `protobuf:"bytes,2,opt,name=payload,proto3" json:"payload,omitempty"`
	Metadata *Metadata `protobuf:"bytes,3,opt,name=metadata,proto3" json:"metadata,omitempty"`
	Headers  *Headers  `protobuf:"bytes,4,opt,name=headers,proto3" json:"headers,omitempty"`
	// the record is hashed with crc-32
	// nit: in kafka apparently record level
	// checksum is replaced by batch-level checksum
	Checksum uint32 `protobuf:"varint,5,opt,name=checksum,proto3" json:"checksum,omitempty"`
}

func (x *Record) Reset() {
	*x = Record{}
	mi := &file_api_v1_record_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Record) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Record) ProtoMessage() {}

func (x *Record) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_record_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Record.ProtoReflect.Descriptor instead.
func (*Record) Descriptor() ([]byte, []int) {
	return file_api_v1_record_proto_rawDescGZIP(), []int{0}
}

func (x *Record) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *Record) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}

func (x *Record) GetMetadata() *Metadata {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *Record) GetHeaders() *Headers {
	if x != nil {
		return x.Headers
	}
	return nil
}

func (x *Record) GetChecksum() uint32 {
	if x != nil {
		return x.Checksum
	}
	return 0
}

// Headers are metadata on a record
// that are eligible to be overriden by
// producing clients.
// They should _ideally_ map to an existing field in
// the Metadata type.
type Headers struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp int64 `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

func (x *Headers) Reset() {
	*x = Headers{}
	mi := &file_api_v1_record_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Headers) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Headers) ProtoMessage() {}

func (x *Headers) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_record_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Headers.ProtoReflect.Descriptor instead.
func (*Headers) Descriptor() ([]byte, []int) {
	return file_api_v1_record_proto_rawDescGZIP(), []int{1}
}

func (x *Headers) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

// Metadata is immutable data about the record
// that is generated before it is written to the log
// it is not permissible to override this information
type Metadata struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp               int64  `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Offset                  uint64 `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
	RelativeOffset          uint64 `protobuf:"varint,3,opt,name=relative_offset,json=relativeOffset,proto3" json:"relative_offset,omitempty"`
	KeyUncompressedLength   int32  `protobuf:"varint,4,opt,name=key_uncompressed_length,json=keyUncompressedLength,proto3" json:"key_uncompressed_length,omitempty"`
	ValueUncompressedLength int32  `protobuf:"varint,5,opt,name=value_uncompressed_length,json=valueUncompressedLength,proto3" json:"value_uncompressed_length,omitempty"`
}

func (x *Metadata) Reset() {
	*x = Metadata{}
	mi := &file_api_v1_record_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Metadata) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Metadata) ProtoMessage() {}

func (x *Metadata) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_record_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Metadata.ProtoReflect.Descriptor instead.
func (*Metadata) Descriptor() ([]byte, []int) {
	return file_api_v1_record_proto_rawDescGZIP(), []int{2}
}

func (x *Metadata) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *Metadata) GetOffset() uint64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *Metadata) GetRelativeOffset() uint64 {
	if x != nil {
		return x.RelativeOffset
	}
	return 0
}

func (x *Metadata) GetKeyUncompressedLength() int32 {
	if x != nil {
		return x.KeyUncompressedLength
	}
	return 0
}

func (x *Metadata) GetValueUncompressedLength() int32 {
	if x != nil {
		return x.ValueUncompressedLength
	}
	return 0
}

var File_api_v1_record_proto protoreflect.FileDescriptor

var file_api_v1_record_proto_rawDesc = []byte{
	0x0a, 0x13, 0x61, 0x70, 0x69, 0x2f, 0x76, 0x31, 0x2f, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x61, 0x70, 0x69, 0x2e, 0x76, 0x31, 0x22, 0xa9, 0x01,
	0x0a, 0x06, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x18, 0x0a, 0x07, 0x70, 0x61,
	0x79, 0x6c, 0x6f, 0x61, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x70, 0x61, 0x79,
	0x6c, 0x6f, 0x61, 0x64, 0x12, 0x2c, 0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x76, 0x31, 0x2e,
	0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61,
	0x74, 0x61, 0x12, 0x29, 0x0a, 0x07, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x73, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x76, 0x31, 0x2e, 0x48, 0x65, 0x61,
	0x64, 0x65, 0x72, 0x73, 0x52, 0x07, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x73, 0x12, 0x1a, 0x0a,
	0x08, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0d, 0x52,
	0x08, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d, 0x22, 0x27, 0x0a, 0x07, 0x48, 0x65, 0x61,
	0x64, 0x65, 0x72, 0x73, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x22, 0xdd, 0x01, 0x0a, 0x08, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12,
	0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x16, 0x0a,
	0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x06, 0x6f,
	0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x27, 0x0a, 0x0f, 0x72, 0x65, 0x6c, 0x61, 0x74, 0x69, 0x76,
	0x65, 0x5f, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0e,
	0x72, 0x65, 0x6c, 0x61, 0x74, 0x69, 0x76, 0x65, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x36,
	0x0a, 0x17, 0x6b, 0x65, 0x79, 0x5f, 0x75, 0x6e, 0x63, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73,
	0x65, 0x64, 0x5f, 0x6c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x15, 0x6b, 0x65, 0x79, 0x55, 0x6e, 0x63, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x65, 0x64,
	0x4c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x12, 0x3a, 0x0a, 0x19, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x5f,
	0x75, 0x6e, 0x63, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x65, 0x64, 0x5f, 0x6c, 0x65, 0x6e,
	0x67, 0x74, 0x68, 0x18, 0x05, 0x20, 0x01, 0x28, 0x05, 0x52, 0x17, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x55, 0x6e, 0x63, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x65, 0x64, 0x4c, 0x65, 0x6e, 0x67,
	0x74, 0x68, 0x42, 0x7d, 0x0a, 0x0a, 0x63, 0x6f, 0x6d, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x76, 0x31,
	0x42, 0x0b, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a,
	0x29, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x79, 0x61, 0x72, 0x65,
	0x66, 0x73, 0x2f, 0x63, 0x61, 0x72, 0x6e, 0x61, 0x78, 0x2f, 0x67, 0x65, 0x6e, 0x2f, 0x61, 0x70,
	0x69, 0x2f, 0x76, 0x31, 0x3b, 0x61, 0x70, 0x69, 0x76, 0x31, 0xa2, 0x02, 0x03, 0x41, 0x58, 0x58,
	0xaa, 0x02, 0x06, 0x41, 0x70, 0x69, 0x2e, 0x56, 0x31, 0xca, 0x02, 0x06, 0x41, 0x70, 0x69, 0x5c,
	0x56, 0x31, 0xe2, 0x02, 0x12, 0x41, 0x70, 0x69, 0x5c, 0x56, 0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d,
	0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0xea, 0x02, 0x07, 0x41, 0x70, 0x69, 0x3a, 0x3a, 0x56,
	0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_api_v1_record_proto_rawDescOnce sync.Once
	file_api_v1_record_proto_rawDescData = file_api_v1_record_proto_rawDesc
)

func file_api_v1_record_proto_rawDescGZIP() []byte {
	file_api_v1_record_proto_rawDescOnce.Do(func() {
		file_api_v1_record_proto_rawDescData = protoimpl.X.CompressGZIP(file_api_v1_record_proto_rawDescData)
	})
	return file_api_v1_record_proto_rawDescData
}

var file_api_v1_record_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_api_v1_record_proto_goTypes = []any{
	(*Record)(nil),   // 0: api.v1.Record
	(*Headers)(nil),  // 1: api.v1.Headers
	(*Metadata)(nil), // 2: api.v1.Metadata
}
var file_api_v1_record_proto_depIdxs = []int32{
	2, // 0: api.v1.Record.metadata:type_name -> api.v1.Metadata
	1, // 1: api.v1.Record.headers:type_name -> api.v1.Headers
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_api_v1_record_proto_init() }
func file_api_v1_record_proto_init() {
	if File_api_v1_record_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_api_v1_record_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_api_v1_record_proto_goTypes,
		DependencyIndexes: file_api_v1_record_proto_depIdxs,
		MessageInfos:      file_api_v1_record_proto_msgTypes,
	}.Build()
	File_api_v1_record_proto = out.File
	file_api_v1_record_proto_rawDesc = nil
	file_api_v1_record_proto_goTypes = nil
	file_api_v1_record_proto_depIdxs = nil
}
