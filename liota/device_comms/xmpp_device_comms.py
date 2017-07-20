# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------#
#  Copyright © 2015-2016 VMware, Inc. All Rights Reserved.                    #
#                                                                             #
#  Licensed under the BSD 2-Clause License (the “License”); you may not use   #
#  this file except in compliance with the License.                           #
#                                                                             #
#  The BSD 2-Clause License                                                   #
#                                                                             #
#  Redistribution and use in source and binary forms, with or without         #
#  modification, are permitted provided that the following conditions are met:#
#                                                                             #
#  - Redistributions of source code must retain the above copyright notice,   #
#      this list of conditions and the following disclaimer.                  #
#                                                                             #
#  - Redistributions in binary form must reproduce the above copyright        #
#      notice, this list of conditions and the following disclaimer in the    #
#      documentation and/or other materials provided with the distribution.   #
#                                                                             #
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"#
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE  #
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE #
#  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE  #
#  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR        #
#  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF       #
#  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS   #
#  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN    #
#  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)    #
#  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF     #
#  THE POSSIBILITY OF SUCH DAMAGE.                                            #
# ----------------------------------------------------------------------------#
import sys
import logging

from liota.device_comms.device_comms import DeviceComms
from liota.lib.transports.xmpp import Xmpp

log = logging.getLogger(__name__)

# Python versions before 3.0 do not use UTF-8 encoding
# by default. To ensure that Unicode is handled properly
# throughout SleekXMPP, we will set the default encoding
# ourselves to UTF-8.
if sys.version_info < (3, 0):
    reload(sys)
    sys.setdefaultencoding('utf8')
else:
    raw_input = input


class XmppDeviceComms(DeviceComms):
    """
    DeviceComms for XMPP Transport
    """

    def __init__(self, jid, password, host, port, identity=None, reattempt=True, use_tls=None, use_ssl=None):
        """

        :param jid:
        :param password:
        :param host:
        :param port:
        :param identity:
        :param reattempt:
        :param use_tls:
        :param use_ssl:
        """
        self.jid = jid
        self.password = password
        self.host = host
        self.port = port
        self.identity = identity
        self.reattempt = reattempt
        self.use_tls = use_tls
        self.use_ssl = use_ssl
        self._connect()

    def _connect(self):
        """

        :return: 
        """
        self.client = Xmpp(self.jid, self.password, self.host, self.port, self.identity,
                           self.reattempt, self.use_tls, self.use_ssl)


    def _disconnect(self):
        """

        :return: 
        """
        self.client.disconnect()

    def create_node(self, server, node):
        """

        :param server: 
        :param node: 
        :return: 
        """
        self.client.create(server, node)

    def publish(self, server, node, data):
        """

        :param server: 
        :param node: 
        :param data: 
        :return: 
        """
        self.client.publish(server, node, data)

    def subscribe(self, server, node, callback):
        """

        :param server:
        :param node:
        :param callback:
        :return:
        """
        data = self.client.subscribe(server, node, callback)
        return data

    def getdata(self, server, node):
        """

        :return: 
        """
        msg = self.client.get_msg(server, node)
        print msg
        return msg

    def send(self, message):
        raise NotImplementedError

    def receive(self):
        raise NotImplementedError
