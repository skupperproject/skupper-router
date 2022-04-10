#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""An extension to the Proton Message class that supports the custom Inter-Router
Annotations section. Note well that this file monkey-patches out the original
Proton message class: importing this module will change every instance of
Proton.Message to use the InterRouterMessage class
"""

from typing import List, Optional

import proton
from proton import Data, Delivery, Link, Receiver, Sender


class RouterAnnotationsSection:
    """
    Representation of the router annotation section that is passed in every
    inter-router message
    """
    SECTION_HEADER = b'\x00\x80\x53\x4B\x50\x52\x2D\x2D\x52\x41'

    def __init__(self, flags: int = 0,
                 to_override: Optional[str] = None,
                 ingress_router: Optional[str] = None,
                 trace: Optional[List[str]] = None) -> None:
        self.flags = flags
        self.to_override = to_override
        self.ingress_router = ingress_router
        self.trace = trace or []

    def encode(self) -> bytes:
        data = Data()
        data.put_list()
        data.enter()
        data.put_uint(self.flags)
        if self.to_override:
            data.put_string(self.to_override)
        else:
            data.put_null()
        if self.ingress_router:
            data.put_string(self.ingress_router)
        else:
            data.put_null()
        data.put_sequence(self.trace)
        data.exit()

        return self.SECTION_HEADER + data.encode()

    @staticmethod
    def strip(encoded: bytes) -> bytes:
        """
        Remove the router annotations section from the start of the encoded
        bytes
        """
        header = RouterAnnotationsSection.SECTION_HEADER
        if encoded.startswith(header):
            encoded = encoded[len(header):]
            # next octet should be the list tag:
            assert encoded[0] in [0x45, 0xC0, 0xD0]
            # skip the entire list
            if encoded[0] == 0x45:
                # AMQP type list0: empty list
                size = 0
                length = 0
            elif encoded[0] == 0xC0:
                # list8: 8 bit length
                size = 1
                length = encoded[1]
            else:
                #list32: 32 bit length
                size = 4
                length = int.from_bytes(encoded[1:5], byteorder='big',
                                        signed=False)
            encoded = encoded[1 + size + length:]
        return encoded

    @staticmethod
    def decode(data: bytes) -> 'RouterAnnotationsSection':
        header = RouterAnnotationsSection.SECTION_HEADER
        assert data.startswith(header)
        data = data[len(header):]
        obj = Data()
        obj.decode(data)
        lcount = obj.get_list()
        assert lcount == 4
        obj.enter()

        ras = RouterAnnotationsSection()

        # index 0: flags
        tag = obj.next()
        assert tag == Data.UINT
        ras.flags = obj.get_uint()

        # index 1: to-override
        tag = obj.next()
        assert tag in [Data.NULL, Data.STRING]
        if tag == Data.STRING:
            ras.to_override = obj.get_string()

        # index 2: ingress_router
        tag = obj.next()
        assert tag in [Data.NULL, Data.STRING]
        if tag == Data.STRING:
            ras.ingress_router = obj.get_string()

        # index 4: trace list
        tag = obj.next()
        assert tag == Data.LIST
        ras.trace = obj.get_sequence()
        obj.exit()

        return ras

    def __repr__(self) -> str:
        value = "flags=%r " % self.flags
        value += "to-override=%r " % self.to_override
        value += "ingress-router=%r " % self.ingress_router
        value += "trace=%r" % self.trace
        return "RouterAnnotations(%s)" % value


class InterRouterMessage(proton.Message):
    """
    A custom Proton message that includes the optional proprietary Router
    Annotations section prior to the AMQP header section.

    Note: does not yet support message streaming!
    """
    def __init__(self,
                 body=None,
                 router_annotations: Optional['RouterAnnotationsSection'] = None,
                 **kwargs):
        # router annotations section
        self.router_annotations = router_annotations
        super(InterRouterMessage, self).__init__(body=body, **kwargs)

    def send(self, sender: Sender, tag: Optional[str] = None) -> Delivery:
        """
        Overrides the proton message send method in order to send the extra
        router annotations section
        """
        dlv = sender.delivery(tag or sender.delivery_tag())
        if self.router_annotations:
            encoded = self.router_annotations.encode() + self.encode()
        else:
            encoded = self.encode()
        sender.stream(encoded)
        sender.advance()
        if sender.snd_settle_mode == Link.SND_SETTLED:
            dlv.settle()
        return dlv

    def recv(self, link: Receiver) -> Optional[Delivery]:
        """
        Overrides the proton message receive method in order to handle the
        extra router annotations section
        """
        if link.is_sender:
            return None
        dlv = link.current
        if not dlv or dlv.partial:
            return None

        encoded = link.recv(dlv.pending)
        if encoded.startswith(RouterAnnotationsSection.SECTION_HEADER):
            self.router_annotations = RouterAnnotationsSection.decode(encoded)
            encoded = RouterAnnotationsSection.strip(encoded)
        else:
            self.router_annotations = None

        dlv.encoded = encoded
        link.advance()
        # the sender has already forgotten about the delivery, so we might
        # as well too
        if link.remote_snd_settle_mode == Link.SND_SETTLED:
            dlv.settle()
        self.decode(dlv.encoded)
        return dlv

    def decode(self, encoded: bytes) -> None:
        if encoded.startswith(RouterAnnotationsSection.SECTION_HEADER):
            self.router_annotations = RouterAnnotationsSection.decode(encoded)
            encoded = RouterAnnotationsSection.strip(encoded)
        else:
            self.router_annotations = None
        super(InterRouterMessage, self).decode(encoded)


# Override the original class:
proton.Message = InterRouterMessage
proton._message.Message = InterRouterMessage
