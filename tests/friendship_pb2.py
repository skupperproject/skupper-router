# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: friendship.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x10\x66riendship.proto\"6\n\x06Person\x12\r\n\x05\x65mail\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x0f\n\x07\x66riends\x18\x03 \x03(\t\"0\n\x0c\x43reateResult\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t\"\x1c\n\x0bPersonEmail\x12\r\n\x05\x65mail\x18\x01 \x01(\t\"5\n\x13\x43ommonFriendsResult\x12\x0f\n\x07\x66riends\x18\x01 \x03(\t\x12\r\n\x05\x63ount\x18\x02 \x01(\x05\"3\n\x11\x46riendshipRequest\x12\x0e\n\x06\x65mail1\x18\x01 \x01(\t\x12\x0e\n\x06\x65mail2\x18\x02 \x01(\t\"W\n\x12\x46riendshipResponse\x12\x18\n\x07\x66riend1\x18\x01 \x01(\x0b\x32\x07.Person\x12\x18\n\x07\x66riend2\x18\x02 \x01(\x0b\x32\x07.Person\x12\r\n\x05\x65rror\x18\x03 \x01(\t2\xd6\x01\n\nFriendship\x12\"\n\x06\x43reate\x12\x07.Person\x1a\r.CreateResult\"\x00\x12<\n\x0bMakeFriends\x12\x12.FriendshipRequest\x1a\x13.FriendshipResponse\"\x00(\x01\x30\x01\x12(\n\x0bListFriends\x12\x0c.PersonEmail\x1a\x07.Person\"\x00\x30\x01\x12<\n\x12\x43ommonFriendsCount\x12\x0c.PersonEmail\x1a\x14.CommonFriendsResult\"\x00(\x01\x62\x06proto3')



_PERSON = DESCRIPTOR.message_types_by_name['Person']
_CREATERESULT = DESCRIPTOR.message_types_by_name['CreateResult']
_PERSONEMAIL = DESCRIPTOR.message_types_by_name['PersonEmail']
_COMMONFRIENDSRESULT = DESCRIPTOR.message_types_by_name['CommonFriendsResult']
_FRIENDSHIPREQUEST = DESCRIPTOR.message_types_by_name['FriendshipRequest']
_FRIENDSHIPRESPONSE = DESCRIPTOR.message_types_by_name['FriendshipResponse']
Person = _reflection.GeneratedProtocolMessageType('Person', (_message.Message,), {
  'DESCRIPTOR' : _PERSON,
  '__module__' : 'friendship_pb2'
  # @@protoc_insertion_point(class_scope:Person)
  })
_sym_db.RegisterMessage(Person)

CreateResult = _reflection.GeneratedProtocolMessageType('CreateResult', (_message.Message,), {
  'DESCRIPTOR' : _CREATERESULT,
  '__module__' : 'friendship_pb2'
  # @@protoc_insertion_point(class_scope:CreateResult)
  })
_sym_db.RegisterMessage(CreateResult)

PersonEmail = _reflection.GeneratedProtocolMessageType('PersonEmail', (_message.Message,), {
  'DESCRIPTOR' : _PERSONEMAIL,
  '__module__' : 'friendship_pb2'
  # @@protoc_insertion_point(class_scope:PersonEmail)
  })
_sym_db.RegisterMessage(PersonEmail)

CommonFriendsResult = _reflection.GeneratedProtocolMessageType('CommonFriendsResult', (_message.Message,), {
  'DESCRIPTOR' : _COMMONFRIENDSRESULT,
  '__module__' : 'friendship_pb2'
  # @@protoc_insertion_point(class_scope:CommonFriendsResult)
  })
_sym_db.RegisterMessage(CommonFriendsResult)

FriendshipRequest = _reflection.GeneratedProtocolMessageType('FriendshipRequest', (_message.Message,), {
  'DESCRIPTOR' : _FRIENDSHIPREQUEST,
  '__module__' : 'friendship_pb2'
  # @@protoc_insertion_point(class_scope:FriendshipRequest)
  })
_sym_db.RegisterMessage(FriendshipRequest)

FriendshipResponse = _reflection.GeneratedProtocolMessageType('FriendshipResponse', (_message.Message,), {
  'DESCRIPTOR' : _FRIENDSHIPRESPONSE,
  '__module__' : 'friendship_pb2'
  # @@protoc_insertion_point(class_scope:FriendshipResponse)
  })
_sym_db.RegisterMessage(FriendshipResponse)

_FRIENDSHIP = DESCRIPTOR.services_by_name['Friendship']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _PERSON._serialized_start=20
  _PERSON._serialized_end=74
  _CREATERESULT._serialized_start=76
  _CREATERESULT._serialized_end=124
  _PERSONEMAIL._serialized_start=126
  _PERSONEMAIL._serialized_end=154
  _COMMONFRIENDSRESULT._serialized_start=156
  _COMMONFRIENDSRESULT._serialized_end=209
  _FRIENDSHIPREQUEST._serialized_start=211
  _FRIENDSHIPREQUEST._serialized_end=262
  _FRIENDSHIPRESPONSE._serialized_start=264
  _FRIENDSHIPRESPONSE._serialized_end=351
  _FRIENDSHIP._serialized_start=354
  _FRIENDSHIP._serialized_end=568
# @@protoc_insertion_point(module_scope)
