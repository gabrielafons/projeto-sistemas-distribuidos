"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    7,
    35,
    1,
    '',
    'chat.proto'
)

_sym_db = _symbol_database.Default()

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nchat.proto\":\n\nServerInfo\x12\x0c\n\x04nome\x18\x01 \x01(\t\x12\x0c\n\x04rank\x18\x02 \x01(\x05\x12\x10\n\x08\x65ndereco\x18\x03 \x01(\t\"\x86\x01\n\x08LogEntry\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0e\n\x06origem\x18\x02 \x01(\t\x12\x0c\n\x04tipo\x18\x03 \x01(\t\x12\x10\n\x08instante\x18\x04 \x01(\x01\x12\x0f\n\x07relogio\x18\x05 \x01(\x03\x12\x0f\n\x07usuario\x18\x06 \x01(\t\x12\r\n\x05\x63\x61nal\x18\x07 \x01(\t\x12\r\n\x05texto\x18\x08 \x01(\t\"\xbc\x02\n\x08\x45nvelope\x12\x13\n\x04tipo\x18\x01 \x01(\x0e\x32\x05.Type\x12\x11\n\tremetente\x18\x02 \x01(\t\x12\x10\n\x08instante\x18\x03 \x01(\x01\x12\x17\n\x06status\x18\x04 \x01(\x0e\x32\x07.Status\x12\x0c\n\x04\x65rro\x18\x05 \x01(\t\x12\x0f\n\x07usuario\x18\x06 \x01(\t\x12\r\n\x05\x63\x61nal\x18\x07 \x01(\t\x12\x0e\n\x06\x63\x61nais\x18\x08 \x03(\t\x12\r\n\x05texto\x18\t \x01(\t\x12\x0f\n\x07relogio\x18\n \x01(\x03\x12\x0c\n\x04rank\x18\x0b \x01(\x05\x12\x1f\n\nservidores\x18\x0c \x03(\x0b\x32\x0b.ServerInfo\x12\x0c\n\x04hora\x18\r \x01(\x01\x12\x13\n\x0b\x63oordenador\x18\x0e \x01(\t\x12\x10\n\x08\x65ndereco\x18\x0f \x01(\t\x12\x1b\n\x08\x65ntradas\x18\x10 \x03(\x0b\x32\t.LogEntry*\xe5\x01\n\x04Type\x12\x10\n\x0c\x44\x45SCONHECIDO\x10\x00\x12\t\n\x05LOGIN\x10\x01\x12\x11\n\rLISTAR_CANAIS\x10\x02\x12\x0f\n\x0b\x43RIAR_CANAL\x10\x03\x12\x0c\n\x08PUBLICAR\x10\x04\x12\x0e\n\nPUBLICACAO\x10\x05\x12\x0e\n\nOBTER_RANK\x10\x06\x12\x15\n\x11LISTAR_SERVIDORES\x10\x07\x12\r\n\tBATIMENTO\x10\x08\x12\x0b\n\x07\x45LEICAO\x10\t\x12\x0b\n\x07RELOGIO\x10\n\x12\x0f\n\x0b\x43OORDENADOR\x10\x0b\x12\x0c\n\x08REPLICAR\x10\x0c\x12\x0f\n\x0bSINCRONIZAR\x10\r*3\n\x06Status\x12\x17\n\x13STATUS_NAO_DEFINIDO\x10\x00\x12\x06\n\x02OK\x10\x01\x12\x08\n\x04\x45RRO\x10\x02\x42\x16\n\x03\x62\x62sB\tChatProto\xaa\x02\x03\x42\x62sb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'chat_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  _globals['DESCRIPTOR']._loaded_options = None
  _globals['DESCRIPTOR']._serialized_options = b'\n\003bbsB\tChatProto\252\002\003Bbs'
  _globals['_TYPE']._serialized_start=531
  _globals['_TYPE']._serialized_end=760
  _globals['_STATUS']._serialized_start=762
  _globals['_STATUS']._serialized_end=813
  _globals['_SERVERINFO']._serialized_start=14
  _globals['_SERVERINFO']._serialized_end=72
  _globals['_LOGENTRY']._serialized_start=75
  _globals['_LOGENTRY']._serialized_end=209
  _globals['_ENVELOPE']._serialized_start=212
  _globals['_ENVELOPE']._serialized_end=528
