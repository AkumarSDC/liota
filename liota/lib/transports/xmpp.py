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

import logging
import xmltodict
import json
import sys

import sleekxmpp.clientxmpp as sleekxmpp

# from liota.lib.utilities.utility import systemUUID


log = logging.getLogger(__name__)

from sleekxmpp.xmlstream import ET
from sleekxmpp.xmlstream.matcher import StanzaPath
from sleekxmpp.xmlstream.handler import Callback

# Python versions before 3.0 do not use UTF-8 encoding
# by default. To ensure that Unicode is handled properly
# throughout SleekXMPP, we will set the default encoding
# ourselves to UTF-8.
if sys.version_info < (3, 0):
    reload(sys)
    sys.setdefaultencoding('utf8')
else:
    raw_input = input


def parse(data):
    data_str = str(data)
    json_data = json.dumps(xmltodict.parse(data_str))
    return json.loads(json_data)


class Xmpp():
    """
    XMPP Transport implementation for LIOTA. It internally uses Python sleekxmpp library.
    """

    def __init__(self, jid, password, host, port, identity=None, reattempt=True, use_tls=None, use_ssl=None):
        """

        :param host:
        :param port:
        :param jid:
        :param password:
        :param identity:
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
        self._xmpp_client = sleekxmpp.ClientXMPP(jid=self.jid, password=self.password, reattempt=self.reattempt)

        self._xmpp_client.register_plugin('xep_0030')
        self._xmpp_client.register_plugin('xep_0059')
        self._xmpp_client.register_plugin('xep_0060')
        self._xmpp_client.add_event_handler('session_start', self.start)
        self.connect_soc()
        self._xmpp_client.process()

    def connect_soc(self):
        """
        Establishes connection with XMPP Broker
        :return:
        """
        self._xmpp_client.connect(reattempt=self.reattempt)

    def start(self, event):
        self._xmpp_client.get_roster()
        self._xmpp_client.send_presence()

    def nodes(self, server, node):
        try:
            result = self._xmpp_client['xep_0060'].get_nodes(server, node)
            for item in result['disco_items']['items']:
                print('  - %s' % str(item))
        except:
            logging.error('Could not retrieve node list.')

    def create(self, server, node):
        try:
            self._xmpp_client['xep_0060'].create_node(server, node)
        except:
            logging.error('Could not create node: %s' % node)

    def delete(self, server, node):
        try:
            self._xmpp_client['xep_0060'].delete_node(server, node)
            print('Deleted node: %s' % node)
        except:
            logging.error('Could not delete node: %s' % node)

    def publish(self, server, node, data):
        payload = ET.fromstring("<test xmlns='test'>%s</test>" % data)
        try:
            result = self._xmpp_client['xep_0060'].publish(server, node, payload=payload)

            id = result['pubsub']['publish']['item']['id']
            print('Published at item id: %s' % id)
        except:
            logging.error('Could not publish to: %s' % node)

    def get_id(self, server, node):
        try:
            xml_id = (self._xmpp_client['xep_0060'].get_item_ids(server, node))
            id = parse(xml_id)["iq"]["query"]["item"]["@name"]
            return id
        except:
            logging.error('Could not retrieve from node %s' % node)

    def get_msg(self, server, node):
        try:
            result = self._xmpp_client['xep_0060'].get_item(server, node, str(self.get_id(server, node)))
            message = parse(result['pubsub']['items']['item'])
            return str(message['item']['test']['#text'])
            # for item in result['pubsub']['items']['substanzas']:
            #     print('Retrieved item :%s' % (parse(tostring(item['payload']))['test']['#text']))
            # return
        except:
            logging.error('Could not retrieve item %s from node %s' % node)

    def subscribe(self, server, node, callback):
        try:
            result = self._xmpp_client['xep_0060'].subscribe(server, node)
            print result
            print('Subscribed %s to node %s' % (self._xmpp_client.boundjid.bare, node))
            self._xmpp_client.register_handler(
                Callback('Pubsub event',
                         StanzaPath('message/pubsub_event'),
                         callback))
        except:
            logging.error('Could not subscribe %s to node %s' % (self._xmpp_client.boundjid.bare, node))

    def unsubscribe(self, server, node):
        try:
            result = self._xmpp_client['xep_0060'].unsubscribe(server, node)
            print('Unsubscribed %s from node %s' % (self._xmpp_client.boundjid.bare, node))
        except:
            logging.error('Could not unsubscribe %s from node %s' % (self._xmpp_client.boundjid.bare, node))

    def disconnect(self):
        self._xmpp_client.disconnect()


# to be implemented for auto generation
class XmppMessagingAttributes:
    """
    Encapsulates MessagingAttributes related to Xmpp

    """

    def __init__(self, edge_system_name=None, node=None):

        if edge_system_name:
            #  For Project ICE and Non-Project ICE, topics will be auto-generated if edge_system_name is not None
            self.node = 'liota/' + edge_system_name
        else:
            #  When edge_system_name is None, pub_topic or sub_topic must be provided
            self.node = node
