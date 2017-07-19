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
import sleekxmpp
from sleekxmpp.xmlstream.matcher import StanzaPath
from sleekxmpp.xmlstream.handler import Callback

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
        :param jid: system id name assigned to sensor
        :param password: for authentication into server
        :param server: where you send data for liota to catch it
        :param node: create node for client service invocation
        :param action: what do you want the created node to do
        :param data: actual data to be sent to cloud via liota gateway
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

    def subscribe(self, server, node):
        """

        :param server: 
        :param node: 
        :return: 
        """
        self.client.subscribe(server, node)

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


"""
These are the changes to be done for event handling

# class PubsubEvents(sleekxmpp.ClientXMPP):
#
#     def __init__(self, jid, password):
#         super(PubsubEvents, self).__init__(jid, password)
#
#         self.register_plugin('xep_0030')
#         self.register_plugin('xep_0059')
#         self.register_plugin('xep_0060')
#
#         self.register_handler(
#             Callback('Pubsub event',
#                      StanzaPath('message/pubsub_event'),
#                      self._handle_event))
#
#         self.add_event_handler('session_start', self.start)
#
#     def start(self, event):
#         self.get_roster()
#         self.send_presence()
#
#     def _handle_event(self, msg):
#         print "Collecting server DATA"
#         Openfire_Data = msg
#         print "Printing variable msg data"
#         print Openfire_Data
#         print "Done printing"
#         print('Received pubsub event: %s' % msg['pubsub_event'])

"""
