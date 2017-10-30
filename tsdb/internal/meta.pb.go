// Code generated by protoc-gen-go. DO NOT EDIT.
// source: internal/meta.proto

/*
Package meta is a generated protocol buffer package.

It is generated from these files:
	internal/meta.proto

It has these top-level messages:
	Series
	Tag
	MeasurementFields
	Field
*/
package meta

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

type Series struct {
	Key              *string `protobuf:"bytes,1,req,name=Key" json:"Key,omitempty"`
	Tags             []*Tag  `protobuf:"bytes,2,rep,name=Tags" json:"Tags,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Series) Reset()                    { *m = Series{} }
func (m *Series) String() string            { return proto.CompactTextString(m) }
func (*Series) ProtoMessage()               {}
func (*Series) Descriptor() ([]byte, []int) { return fileDescriptorMeta, []int{0} }

func (m *Series) GetKey() string {
	if m != nil && m.Key != nil {
		return *m.Key
	}
	return ""
}

func (m *Series) GetTags() []*Tag {
	if m != nil {
		return m.Tags
	}
	return nil
}

type Tag struct {
	Key              *string `protobuf:"bytes,1,req,name=Key" json:"Key,omitempty"`
	Value            *string `protobuf:"bytes,2,req,name=Value" json:"Value,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Tag) Reset()                    { *m = Tag{} }
func (m *Tag) String() string            { return proto.CompactTextString(m) }
func (*Tag) ProtoMessage()               {}
func (*Tag) Descriptor() ([]byte, []int) { return fileDescriptorMeta, []int{1} }

func (m *Tag) GetKey() string {
	if m != nil && m.Key != nil {
		return *m.Key
	}
	return ""
}

func (m *Tag) GetValue() string {
	if m != nil && m.Value != nil {
		return *m.Value
	}
	return ""
}

type MeasurementFields struct {
	Fields           []*Field `protobuf:"bytes,1,rep,name=Fields" json:"Fields,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *MeasurementFields) Reset()                    { *m = MeasurementFields{} }
func (m *MeasurementFields) String() string            { return proto.CompactTextString(m) }
func (*MeasurementFields) ProtoMessage()               {}
func (*MeasurementFields) Descriptor() ([]byte, []int) { return fileDescriptorMeta, []int{2} }

func (m *MeasurementFields) GetFields() []*Field {
	if m != nil {
		return m.Fields
	}
	return nil
}

type Field struct {
	ID               *int32  `protobuf:"varint,1,req,name=ID" json:"ID,omitempty"`
	Name             *string `protobuf:"bytes,2,req,name=Name" json:"Name,omitempty"`
	Type             *int32  `protobuf:"varint,3,req,name=Type" json:"Type,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Field) Reset()                    { *m = Field{} }
func (m *Field) String() string            { return proto.CompactTextString(m) }
func (*Field) ProtoMessage()               {}
func (*Field) Descriptor() ([]byte, []int) { return fileDescriptorMeta, []int{3} }

func (m *Field) GetID() int32 {
	if m != nil && m.ID != nil {
		return *m.ID
	}
	return 0
}

func (m *Field) GetName() string {
	if m != nil && m.Name != nil {
		return *m.Name
	}
	return ""
}

func (m *Field) GetType() int32 {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return 0
}

func init() {
	proto.RegisterType((*Series)(nil), "meta.Series")
	proto.RegisterType((*Tag)(nil), "meta.Tag")
	proto.RegisterType((*MeasurementFields)(nil), "meta.MeasurementFields")
	proto.RegisterType((*Field)(nil), "meta.Field")
}

func init() { proto.RegisterFile("internal/meta.proto", fileDescriptorMeta) }

var fileDescriptorMeta = []byte{
	// 180 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x54, 0x8c, 0xbd, 0xca, 0xc2, 0x30,
	0x14, 0x40, 0x69, 0xd2, 0x16, 0x7a, 0xfb, 0x7d, 0x83, 0x71, 0x30, 0xe0, 0x52, 0x33, 0x75, 0x6a,
	0xc5, 0x67, 0x10, 0x41, 0x44, 0x17, 0x83, 0xfb, 0x05, 0x2f, 0xa5, 0xd0, 0x3f, 0x92, 0x74, 0xe8,
	0xdb, 0x4b, 0x52, 0x17, 0xb7, 0x73, 0xee, 0xcf, 0x81, 0x6d, 0x3b, 0x38, 0x32, 0x03, 0x76, 0x75,
	0x4f, 0x0e, 0xab, 0xc9, 0x8c, 0x6e, 0x14, 0xb1, 0x67, 0x55, 0x41, 0xfa, 0x24, 0xd3, 0x92, 0x15,
	0x39, 0xf0, 0x1b, 0x2d, 0x32, 0x2a, 0x58, 0x99, 0x89, 0x1d, 0xc4, 0x1a, 0x1b, 0x2b, 0x59, 0xc1,
	0xcb, 0xfc, 0x94, 0x55, 0xe1, 0x4f, 0x63, 0xa3, 0x0e, 0xc0, 0x35, 0x36, 0xbf, 0xc7, 0xff, 0x90,
	0xbc, 0xb0, 0x9b, 0x49, 0x32, 0xaf, 0xea, 0x08, 0x9b, 0x3b, 0xa1, 0x9d, 0x0d, 0xf5, 0x34, 0xb8,
	0x4b, 0x4b, 0xdd, 0xdb, 0x8a, 0x3d, 0xa4, 0x2b, 0xc9, 0x28, 0x24, 0xf3, 0x35, 0x19, 0x66, 0xaa,
	0x86, 0x24, 0x80, 0x00, 0x60, 0xd7, 0x73, 0xa8, 0x26, 0xe2, 0x0f, 0xe2, 0x07, 0xf6, 0xdf, 0xa8,
	0x37, 0xbd, 0x4c, 0x24, 0xb9, 0xdf, 0x7d, 0x02, 0x00, 0x00, 0xff, 0xff, 0x04, 0x3d, 0x58, 0x4a,
	0xd1, 0x00, 0x00, 0x00,
}
