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

import os
import unittest
from ConfigParser import ConfigParser

import mock

from liota.dcc_comms.amqp_dcc_comms import *
from liota.entities.edge_systems.dell5k_edge_system import Dell5KEdgeSystem
from liota.lib.utilities.identity import Identity
from liota.lib.utilities.tls_conf import TLSConf
from liota.lib.utilities.utility import read_liota_config
from liota.lib.utilities.utility import systemUUID


# Sample function
def sampling_function():
    pass


# Callback function
def test_callback():
    pass


class AmqpDccCommsTest(unittest.TestCase):
    """
    AmqpDccComms unit test cases
    """

    def setUp(self):
        """
        Setup all required parameters for AmqpDccComms tests
        :return: None
        """
        # ConfigParser to parse ini file
        self.config = ConfigParser()
        self.uuid_file = read_liota_config('UUID_PATH', 'uuid_path')

        # Broker details
        self.url = "Broker-IP"
        self.port = "Broker-Port"
        self.amqp_username = "test"
        self.amqp_password = "test"
        self.enable_authentication = True
        self.transport = "tcp"
        self.connection_disconnect_timeout_sec = 2

        # EdgeSystem name
        self.edge_system = Dell5KEdgeSystem("TestGateway")

        # TLS configurations
        self.root_ca_cert = "/etc/liota/amqp/conf/ca.crt"
        self.client_cert_file = "/etc/liota/amqp/conf/client.crt"
        self.client_key_file = "/etc/liota/amqp/conf/client.key"
        self.cert_required = "CERT_REQUIRED"
        self.tls_version = "PROTOCOL_TLSv1"
        self.cipher = None

        # Encapsulate the authentication details
        self.identity = Identity(self.root_ca_cert, self.amqp_username, self.amqp_password,
                                 self.client_cert_file, self.client_key_file)

        # Encapsulate TLS parameters
        self.tls_conf = TLSConf(self.cert_required, self.tls_version, self.cipher)

        self.send_message = "test-message"

        # Creating messaging attributes
        self.amqp_msg_attr = AmqpPublishMessagingAttributes(exchange_name="test_exchange", routing_key=["test"])

        with mock.patch.object(Amqp, '_init_or_re_init') as mocked_init_or_re_init, \
                mock.patch.object(Amqp, "declare_publish_exchange") as mocked_declare_publish_exchange:
            # Instantiate client for DCC communication
            self.client = AmqpDccComms(edge_system_name=self.edge_system.name, url=self.url, port=self.port)

    def tearDown(self):
        """
        Clean up all parameters used in test cases
        :return: None
        """

        # Check path exists
        if os.path.exists(self.uuid_file):
            try:
                # Remove the file
                os.remove(self.uuid_file)
            except OSError as e:
                log.error("Unable to remove UUID file" + str(e))

        self.config = None
        self.uuid_file = None
        self.edge_system = None
        self.url = None
        self.port = None
        self.amqp_username = None
        self.amqp_password = None
        self.enable_authentication = None
        self.connection_disconnect_timeout_sec = None

        # EdgeSystem name
        self.edge_system = None

        # TLS configurations
        self.root_ca_cert = None
        self.client_cert_file = None
        self.client_key_file = None
        self.cert_required = None
        self.tls_version = None
        self.cipher = None

        # Identity
        self.identity = None

        # TLS configurations
        self.tls_conf = None

        self.send_message = None
        self.client = None

    @mock.patch.object(Amqp, "declare_publish_exchange")
    @mock.patch.object(Amqp, "__init__")
    def test_init_implementation(self, mocked_init, mocked_declare_publish_exchange):
        """
        Test AmqpDccComms implementation without publish messaging attributes.
        :param mocked_init: Mocked init from Amqp class
        :param mocked_init: Mocked declare_publish_exchange from Amqp class
        :return: None
        """

        # Mocked init method
        mocked_init.return_value = None

        # Instantiate client for DCC communication
        self.amqp_client = AmqpDccComms(edge_system_name=self.edge_system.name, url=self.url, port=self.port,
                                        enable_authentication=self.enable_authentication,
                                        tls_conf=self.tls_conf, identity=self.identity,
                                        connection_timeout_sec=self.connection_disconnect_timeout_sec)

        # Read uuid.ini
        self.config.read(self.uuid_file)

        edge_system_name = self.config.get('GATEWAY', 'name')
        local_uuid = self.config.get('GATEWAY', 'local-uuid')

        # Compare stored edge system name
        self.assertEquals(edge_system_name, self.edge_system.name)

        # Compare stored client-id
        self.assertEquals(local_uuid, systemUUID().get_uuid(self.edge_system.name))

        # Check Amqp init called method call has been made
        mocked_init.assert_called()

        # Check declare publish exchange has been called
        mocked_declare_publish_exchange.assert_called_with(self.amqp_client.pub_msg_attr)

    @mock.patch.object(Amqp, "declare_publish_exchange")
    @mock.patch.object(Amqp, "__init__")
    def test_init_implementation_without_msg_attr(self, mocked_init, mocked_declare_publish_exchange):
        """
        Test AmqpDccComms implementation without publish messaging attributes.
        :param mocked_init: Mocked init from Amqp class
        :param mocked_init: Mocked declare_publish_exchange from Amqp class
        :return: None
        """

        # Mocked init method
        mocked_init.return_value = None

        # Instantiate client for DCC communication
        self.amqp_client = AmqpDccComms(edge_system_name=self.edge_system.name, url=self.url, port=self.port,
                                        enable_authentication=self.enable_authentication,
                                        tls_conf=self.tls_conf, identity=self.identity,
                                        connection_timeout_sec=self.connection_disconnect_timeout_sec)

        # Read uuid.ini
        self.config.read(self.uuid_file)

        edge_system_name = self.config.get('GATEWAY', 'name')
        local_uuid = self.config.get('GATEWAY', 'local-uuid')

        # Compare stored edge system name
        self.assertEquals(edge_system_name, self.edge_system.name)

        # Compare stored client-id
        self.assertEquals(local_uuid, systemUUID().get_uuid(self.edge_system.name))

        # Check Amqp init called method call has been made
        mocked_init.assert_called_with(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                       self.connection_disconnect_timeout_sec)

        # Check declare publish exchange has been called
        mocked_declare_publish_exchange.assert_called_with(self.amqp_client.pub_msg_attr)

    @mock.patch.object(Amqp, "declare_publish_exchange")
    @mock.patch.object(Amqp, "__init__")
    def test_init_implementation_with_msg_attr(self, mocked_init, mocked_declare_publish_exchange):
        """
        Test AmqpDccComms implementation with publish messaging attributes.
        :param mocked_init: Mocked init from Amqp class
        :param mocked_init: Mocked declare_publish_exchange from Amqp class
        :return: None
        """

        # Mocked init method
        mocked_init.return_value = None

        # Check path exists
        if os.path.exists(self.uuid_file):
            try:
                # Remove the file
                os.remove(self.uuid_file)
            except OSError as e:
                log.error("Unable to remove UUID file" + str(e))

        # Instantiate client for DCC communication
        self.amqp_client = AmqpDccComms(edge_system_name=self.edge_system.name, url=self.url, port=self.port,
                                        enable_authentication=self.enable_authentication,
                                        tls_conf=self.tls_conf, identity=self.identity,
                                        amqp_pub_msg_attr=self.amqp_msg_attr,
                                        connection_timeout_sec=self.connection_disconnect_timeout_sec)

        # Check uuid file get created or not
        self.assertFalse(os.path.exists(self.uuid_file))

        # Check Amqp init called method call has been made
        mocked_init.assert_called_with(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                       self.connection_disconnect_timeout_sec)

        # Check declare publish exchange has been called
        mocked_declare_publish_exchange.assert_called_with(self.amqp_msg_attr)

    def test_init_validation(self):
        """
        Test the validation of init method for Publish messaging attributes
        :return: None
        """

        with self.assertRaises(TypeError):
            # Pass invalid amqp_pub_msg_attr attribute and check implemetation rasing the TypeError
            AmqpDccComms(self.edge_system, self.url, self.port, amqp_pub_msg_attr=True)

    @mock.patch.object(Amqp, "disconnect")
    def test_disconnect_implementation(self, mocked_disconnect):
        """
        
        :param mocked_disconnect: Mocked disconnect from Amqp class 
        :return: None
        """

        # Call _disconnect method
        self.client._disconnect()

        # Check implementation calling the mocked method
        mocked_disconnect.assert_called()

    @mock.patch.object(Amqp, "disconnect_consumer")
    def test_stop_receiving_implementation(self, mocked_disconnect_consumer):
        """
        Test the implementation of stop_receiving 
        :param mocked_disconnect_consumer: Mocked disconnect_consumer consumer from Amqp class 
        :return: None
        """

        # Call _disconnect method
        self.client.stop_receiving()

        # Check implementation calling the mocked method
        mocked_disconnect_consumer.assert_called()

    def test_validation_receive(self):
        """
        Test the validation of consume_msg_attr_list attributes from receive method.
        :return: None
        """

        with self.assertRaises(TypeError):
            # Call receive with invalid params
            self.client.receive(True, None)

            # Create Amap consumer messaging attributes
            AmqpConsumeMessagingAttributes(exchange_name="test_exchange",
                                           routing_keys=["test"])

    @mock.patch.object(Amqp, "consume")
    def test_implementation_receive(self, mocked_consume):
        """
        Test the implementation of consume_msg_attr_list attributes without auto_gen_callback from receive method.
        :return: None
        """

        # Create Amap consumer messaging attributes
        amqp_consumer_msg_attr = [AmqpConsumeMessagingAttributes(exchange_name="test_exchange",
                                                                 routing_keys=["test"])]

        # Call receive with invalid params
        self.client.receive(amqp_consumer_msg_attr)

        # Check consume called with following params
        mocked_consume.assert_called_with(amqp_consumer_msg_attr)

    @mock.patch.object(Amqp, "consume")
    def test_implementation_receive(self, mocked_consume):
        """
        Test the implementation of consume_msg_attr_list attributes without auto_gen_callback from receive method.
        :param mocked_consume: Mocked consume method of Amqp class 
        :return: 
        """

        # Create Amap consumer messaging attributes
        amqp_consumer_msg_attr = [AmqpConsumeMessagingAttributes(exchange_name="test_exchange",
                                                                 routing_keys=["test"])]

        # Call receive with invalid params
        self.client.receive(amqp_consumer_msg_attr, test_callback)

        # Check consume called with following params
        mocked_consume.assert_called_with(amqp_consumer_msg_attr)

        # Check length of the array
        self.assertEquals(2, len(amqp_consumer_msg_attr))

    @mock.patch.object(Amqp, "publish")
    def test_send_without_pub_msg_attr(self, mocked_publish):
        """
        Test the implementation of publish without pub_msg_attr
        :param mocked_publish: Mocked publish method of Amqp class  
        :return: None
        """
        with mock.patch.object(Amqp, '_init_or_re_init') as mocked_init_or_re_init, \
                mock.patch.object(Amqp, "declare_publish_exchange") as mocked_declare_publish_exchange:
            # Instantiate client for DCC communication
            self.client = AmqpDccComms(edge_system_name=self.edge_system.name, url=self.url, port=self.port,
                                       enable_authentication=True, tls_conf=self.tls_conf, identity=self.identity,
                                       amqp_pub_msg_attr=self.amqp_msg_attr)

            # Call send method with message
            self.client.send(self.send_message)

            # Check publish called with following args
            mocked_publish.assert_called_with(self.amqp_msg_attr.exchange_name, self.amqp_msg_attr.routing_key,
                                              self.send_message, self.amqp_msg_attr.properties)

    @mock.patch.object(Amqp, "publish")
    def test_send_with_pub_msg_attr(self, mocked_publish):
        """
        Test the implementation of publish with pub_msg_attr
        :param mocked_publish: Mocked publish method of Amqp class  
        :return: None
        """
        with mock.patch.object(Amqp, '_init_or_re_init') as mocked_init_or_re_init, \
                mock.patch.object(Amqp, "declare_publish_exchange") as mocked_declare_publish_exchange:
            # Creating messaging attributes
            amqp_msg_attr = AmqpPublishMessagingAttributes(exchange_name="test_exchange_new",
                                                           routing_key=["test1", "test2"])

            # Instantiate client for DCC communication
            self.client = AmqpDccComms(edge_system_name=self.edge_system.name, url=self.url, port=self.port,
                                       enable_authentication=True, tls_conf=self.tls_conf, identity=self.identity,
                                       amqp_pub_msg_attr=self.amqp_msg_attr)

            # Call send method with message and amqp_msg_attr
            self.client.send(self.send_message, amqp_msg_attr)

            # Check publish called with following args
            mocked_publish.assert_called_with(amqp_msg_attr.exchange_name, amqp_msg_attr.routing_key,
                                              self.send_message, amqp_msg_attr.properties)

    @mock.patch.object(Amqp, "publish")
    def test_send_with_pub_msg_attr_exchange_none(self, mocked_publish):
        """
        Test the implementation of publish with pub_msg_attr and exchange name as None
        :param mocked_publish: Mocked publish method of Amqp class  
        :return: None
        """
        with mock.patch.object(Amqp, '_init_or_re_init') as mocked_init_or_re_init, \
                mock.patch.object(Amqp, "declare_publish_exchange") as mocked_declare_publish_exchange:
            # Creating messaging attributes
            amqp_msg_attr = AmqpPublishMessagingAttributes(routing_key=["test1", "test2"])

            # Instantiate client for DCC communication
            self.client = AmqpDccComms(edge_system_name=self.edge_system.name, url=self.url, port=self.port,
                                       enable_authentication=True, tls_conf=self.tls_conf, identity=self.identity,
                                       amqp_pub_msg_attr=self.amqp_msg_attr)

            # Call send method with message and amqp_msg_attr
            self.client.send(self.send_message, amqp_msg_attr)

            # Check publish called with following args
            mocked_publish.assert_called_with(self.amqp_msg_attr.exchange_name, amqp_msg_attr.routing_key,
                                              self.send_message, amqp_msg_attr.properties)


if __name__ == '__main__':
    unittest.main(verbosity=1)
