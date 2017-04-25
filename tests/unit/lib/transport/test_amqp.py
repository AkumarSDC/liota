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

import unittest

import mock
from kombu import Consumer
from kombu import Message
from kombu.pools import Resource

from liota.entities.edge_systems.dell5k_edge_system import Dell5KEdgeSystem
from liota.lib.transports.amqp import *
from liota.lib.utilities.identity import Identity
from liota.lib.utilities.tls_conf import TLSConf
from liota.lib.utilities.utility import systemUUID


def mocked_os_path_root_ca_and_client_crt(path):
    """
    Method used to mock the behaviour of os.path.exists for some of the test cases.
    :param path: Path to check
    :return: Bool
    """
    if path == "/etc/liota/mqtt/conf/ca.crt":
        return True

    if path == "/etc/liota/mqtt/conf/client.crt":
        return True

    return False


def mocked_os_path_root_ca(path):
    """
    Method used to mock the behaviour of os.path.exists for some of the test cases.
    :param path: Path to check
    :return: Bool
    """
    if path == "/etc/liota/mqtt/conf/ca.crt":
        return True

    return False


def custom_callback(body, message):
    """
    Custom callback method for  
    :param body: Message body
    :param message: Kombu Message object.
    return None
    """
    message.ack()


def mock_exception(*args, **kwargs):
    """
    Method used to raise the exception during the execution of some of the test case.
    :param args: 
    :param kwargs: 
    :return: None
    """
    raise Exception("Test Exception")


def callback_method(*args, **kwargs):
    """
    Callback method used in AmqpWorkerThread class unit test cases.
    :param args: 
    :param kwargs: 
    :return: None
    """
    pass


def validate_json(obj):
    """
    Method to sort the provided json and returns back the sorted list representation of the json.
    :param obj: json object
    :return: sorted list of json
    """
    if isinstance(obj, dict):
        return sorted((k, validate_json(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(validate_json(x) for x in obj)
    else:
        return obj


class GlobalFunctionsTest(unittest.TestCase):
    """
    Unit test cases of all public functions of Amqp transport. 
    """

    def setUp(self):
        """
        Method to initialise the parameters for global functions.
        :return: None
        """
        # EdgeSystem configurations
        self.edge_system = Dell5KEdgeSystem("TestEdgeSystem")

    def tearDown(self):
        """
        Method to cleanup the resource created during the execution of test case.
        :return: None
        """
        # EdgeSystem configurations
        self.edge_system = None

    def test_auto_generate_exchage_name(self):
        """
        Test case to test the implementation of auto_generate_exchage_name method.
        :return: None 
        """
        # Check the implementation of auto_generate_exchage_name
        self.assertEqual("liota.exchange." + systemUUID().get_uuid(self.edge_system.name),
                         auto_generate_exchage_name(self.edge_system.name),
                         "Invalid implementation of auto_generate_exchage_name")

    def test_auto_generate_routing_key_request(self):
        """
        Test case to test the implementation of auto_generate_routing_key method for request.
        :return: None 
        """
        # Check the implementation of auto_generate_routing_key for request
        self.assertEqual("liota." + systemUUID().get_uuid(self.edge_system.name) + ".request",
                         auto_generate_routing_key(self.edge_system.name, for_publish=True),
                         "Invalid implementation of auto_generate_routing_key")

    def test_auto_generate_routing_key_response(self):
        """
        Test case to test the implementation of auto_generate_routing_key method for response.
        :return: None 
        """
        # Check the implementation of auto_generate_routing_key for response
        self.assertEqual("liota." + systemUUID().get_uuid(self.edge_system.name) + ".response",
                         auto_generate_routing_key(self.edge_system.name, for_publish=False),
                         "Invalid implementation of auto_generate_routing_key")

    def test_auto_generate_queue_name(self):
        """
        Test case to test the implementation of auto_generate_queue_name method.
        :return: None 
        """
        # Check the implementation of auto_generate_queue_name
        self.assertEqual("liota.queue." + systemUUID().get_uuid(self.edge_system.name),
                         auto_generate_queue_name(self.edge_system.name),
                         "Invalid implementation of auto_generate_queue_name")


class AmqpPublishMessagingAttributesTest(unittest.TestCase):
    """
    Unit test cases for AmqpPublishMessagingAttributes class
    """

    def setUp(self):
        """
        Method to initialise the AmqpPublishMessagingAttributes parameters.
        :return: None
        """
        # Edge system configurations
        self.edge_system_name = Dell5KEdgeSystem("TestEdgeSystem")

        # Amqp configurations
        self.exchange_name = "test_amqp_exchange",
        self.exchange_type = DEFAULT_EXCHANGE_TYPE
        self.exchange_durable = False
        self.routing_key = "test_key"
        self.msg_delivery_mode = 2
        self.header_args = {
            "test": "header_test"
        }
        self.amqp_msg_attr = AmqpPublishMessagingAttributes(edge_system_name=self.edge_system_name.name,
                                                            exchange_type=self.exchange_type,
                                                            exchange_durable=self.exchange_durable,
                                                            msg_delivery_mode=self.msg_delivery_mode,
                                                            header_args=self.header_args)

    def tearDown(self):
        """
        Method to cleanup the resource created during the execution of test case.
        :return: None
        """
        # Edge system configurations
        self.edge_system_name = None

        # Amqp configurations
        self.exchange_name = None
        self.exchange_type = None
        self.exchange_durable = None
        self.routing_key = None
        self.msg_delivery_mode = None
        self.header_args = None

    def test_class_implementation(self):
        """
        Test the AmqpPublishMessagingAttributes class implementation.
        :return: None
        """

        # Check implementation for exchange_name
        self.assertEqual(self.amqp_msg_attr.exchange_name, auto_generate_exchage_name(self.edge_system_name.name))

        # Check implementation for routing_key
        self.assertEqual(self.amqp_msg_attr.routing_key, auto_generate_routing_key(self.edge_system_name.name))

        # Check implementation for exchange_durable
        self.assertEqual(self.amqp_msg_attr.exchange_durable, self.exchange_durable)

        # Check implementation for properties
        self.assertEqual(self.amqp_msg_attr.properties, DEFAULT_PUBLISH_PROPERTIES)

        # Check implementation for exchange_type
        self.assertEqual(self.amqp_msg_attr.exchange_type, self.exchange_type)

        # Check is_exchange_declared attributes
        self.assertEqual(self.amqp_msg_attr.is_exchange_declared, False)

    def test_validations_routing_key(self):
        """
        Test the validation for routing_key for exchange type other than headers.
        :return: None
        """
        # Check implementation raising the ValueError
        with self.assertRaises(ValueError):
            AmqpPublishMessagingAttributes(exchange_type=DEFAULT_EXCHANGE_TYPE)

    def test_exchange_validations(self):
        """
        Test the validation for exchange_type.
        :return: None
        """
        # Check implementation raising the TypeError
        with self.assertRaises(TypeError):
            AmqpPublishMessagingAttributes(self.edge_system_name.name, exchange_type="invalid")

    def test_header_exchange_validations(self):
        """
        Test the validation for header exchange_type.
        :return: None
        """
        # Check implementation raising the ValueError
        with self.assertRaises(ValueError):
            AmqpPublishMessagingAttributes(self.edge_system_name.name, exchange_type="headers")

    def test_msg_delivery_mode_validations(self):
        """
        Test the validation for msg_delivery_mode.
        :return: None
        """
        # Check implementation raising the ValueError
        with self.assertRaises(ValueError):
            AmqpPublishMessagingAttributes(self.edge_system_name.name, msg_delivery_mode=4)

    def test_none_exchange_name_implementation(self):
        """
        Test the value of is_exchange_declared when exchange_name passed as None.
        :return: None
        """
        # Create AmqpPublishMessagingAttributes class object
        amqp_msg_attr = AmqpPublishMessagingAttributes(exchange_type=self.exchange_type, routing_key=self.routing_key)

        # Check value of is_exchange_declared as True
        self.assertEqual(amqp_msg_attr.is_exchange_declared, True)

    def test_msg_delivery_mode(self):
        """
        Test the implementation of properties for msg_deliver_mode 2. 
        :return: None 
        """
        # Default publisher properties
        properties_cmp = DEFAULT_PUBLISH_PROPERTIES

        # Append the delivery mode
        properties_cmp["delivery_mode"] = 2

        # Create AmqpPublishMessagingAttributes class object
        amqp_msg_attr = AmqpPublishMessagingAttributes(exchange_type=self.exchange_type, routing_key=self.routing_key)

        # Check value of is_exchange_declared as True
        self.assertEqual(validate_json(properties_cmp) == validate_json(amqp_msg_attr.properties), True)

    def test_headers(self):
        """
        Test the implementation of headers_args parameter. 
        :return: None 
        """
        # Default publisher properties
        properties_cmp = self.header_args

        # Create AmqpPublishMessagingAttributes class object
        amqp_msg_attr = AmqpPublishMessagingAttributes(exchange_type=self.exchange_type, routing_key=self.routing_key,
                                                       header_args=self.header_args)

        # Check value of is_exchange_declared as True
        self.assertEqual(validate_json(properties_cmp) == validate_json(amqp_msg_attr.properties["headers"]), True)


class AmqpConsumeMessagingAttributesTest(unittest.TestCase):
    """
    AmqpConsumeMessagingAttributes unit test cases
    """

    def setUp(self):
        """
        Method to initialise the AmqpConsumeMessagingAttributes parameters.
        :return: None
        """
        # Edge system configurations
        self.edge_system_name = Dell5KEdgeSystem("TestEdgeSystem")

        # Amqp configurations
        self.exchange_name = "test_amqp_exchange"
        self.exchange_type = DEFAULT_EXCHANGE_TYPE
        self.exchange_durable = False
        self.queue_name = "test_queue_name"
        self.queue_durable = False
        self.queue_auto_delete = True
        self.queue_exclusive = False
        self.routing_keys = ["test1_key", "test2_key", "test3_key"]
        self.queue_no_ack = False
        self.prefetch_size = 0
        self.prefetch_count = 0
        self.callback = None
        self.header_args = {
            "test": "header_test"
        }
        self.amqp_msg_attr = AmqpConsumeMessagingAttributes(edge_system_name=self.edge_system_name.name,
                                                            exchange_type=self.exchange_type,
                                                            exchange_durable=self.exchange_durable,
                                                            queue_durable=self.queue_durable,
                                                            queue_auto_delete=self.queue_auto_delete,
                                                            queue_exclusive=self.queue_exclusive,
                                                            routing_keys=self.routing_keys,
                                                            queue_no_ack=self.queue_no_ack,
                                                            prefetch_size=self.prefetch_size,
                                                            prefetch_count=self.prefetch_count,
                                                            callback=self.callback,
                                                            header_args=self.header_args)

    def tearDown(self):
        """
        Method to cleanup the resource created during the execution of test case.
        :return: None
        """
        # Edge system configurations
        self.edge_system_name = None

        # Amqp configurations
        self.exchange_name = None
        self.exchange_type = None
        self.exchange_durable = None
        self.queue_name = None
        self.queue_durable = None
        self.queue_auto_delete = None
        self.queue_exclusive = None
        self.routing_keys = None
        self.queue_no_ack = None
        self.prefetch_size = None
        self.prefetch_count = None
        self.callback = None
        self.header_args = None

    def test_class_implementation(self):
        """
        Test the AmqpConsumeMessagingAttributes class implementation.
        :return: None
        """

        # Check implementation for exchange_name
        self.assertEqual(self.amqp_msg_attr.exchange_name, auto_generate_exchage_name(self.edge_system_name.name))

        # Check implementation for routing_keys
        self.assertEqual(self.amqp_msg_attr.routing_keys, [auto_generate_routing_key(self.edge_system_name.name,
                                                                                     for_publish=False)])

        # Check implementation for queue_name
        self.assertEqual(self.amqp_msg_attr.queue_name, auto_generate_queue_name(self.edge_system_name.name))

        # Check implementation for exchange_type
        self.assertEqual(self.amqp_msg_attr.exchange_type, self.exchange_type)

        # Check implementation for exchange_durable
        self.assertEqual(self.amqp_msg_attr.exchange_durable, self.exchange_durable)

        # Check implementation for queue_durable
        self.assertEqual(self.amqp_msg_attr.queue_durable, self.queue_durable)

        # Check implementation for queue_auto_delete
        self.assertEqual(self.amqp_msg_attr.queue_auto_delete, self.queue_auto_delete)

        # Check implementation for queue_exclusive
        self.assertEqual(self.amqp_msg_attr.queue_exclusive, self.queue_exclusive)

        # Check implementation for queue_no_ack
        self.assertEqual(self.amqp_msg_attr.queue_no_ack, self.queue_no_ack)

        # Check implementation for prefetch_size
        self.assertEqual(self.amqp_msg_attr.prefetch_size, self.prefetch_size)

        # Check implementation for prefetch_count
        self.assertEqual(self.amqp_msg_attr.prefetch_count, self.prefetch_count)

        # Check implementation for callback
        self.assertEqual(self.amqp_msg_attr.callback, self.callback)

        # Check implementation for header_args
        self.assertEqual(self.amqp_msg_attr.header_args, self.header_args)

    def test_exchange_validations(self):
        """
        Test the validation for exchange_type.
        :return: None
        """
        # Check implementation raising the TypeError
        with self.assertRaises(TypeError):
            AmqpConsumeMessagingAttributes(self.edge_system_name.name, exchange_type="invalid")

    def test_routing_keys_none(self):
        """
        Test the validation for None routing_keys if exchange_type is other than "headers".
        :return: None
        """
        # Check implementation raising the ValueError
        with self.assertRaises(ValueError):
            AmqpConsumeMessagingAttributes(exchange_type=DEFAULT_EXCHANGE_TYPE)

    def test_header_exchange_validation(self):
        """
        Test the validation for header exchange_type.
        :return: None
        """
        # Check implementation raising the ValueError
        with self.assertRaises(ValueError):
            AmqpConsumeMessagingAttributes(exchange_type="headers")

    def test_routing_keys_validation(self):
        """
        Test the validation for routing_keys type.
        :return: None
        """
        # Check implementation raising the TypeError
        with self.assertRaises(TypeError):
            AmqpConsumeMessagingAttributes(routing_keys="invalid")

    def test_routing_keys_empty(self):
        """
        Test to check if routing_keys initialised to empty list when exchange type provided as headers.
        :return: None
        """

        # Create AmqpPublishMessagingAttributes class object
        amqp_msg_attr = AmqpConsumeMessagingAttributes(exchange_type="headers", header_args=self.header_args)

        # Check if routing_keys is empty list
        self.assertEqual(amqp_msg_attr.routing_keys, [])

    def test_class_implementation_without_edge_system_name(self):
        """
        Test to check if exchange_name, routing_keys and queue_name are assigned properly if edge_system_name not given
        :return: None
        """
        # Create AmqpPublishMessagingAttributes class object
        amqp_msg_attr = AmqpConsumeMessagingAttributes(exchange_name=self.exchange_name,
                                                       routing_keys=self.routing_keys,
                                                       queue_name=self.queue_name)

        # Check if exchange_name is assigned properly
        self.assertEqual(amqp_msg_attr.exchange_name, self.exchange_name)

        # Check if routing_keys is assigned properly
        self.assertEqual(amqp_msg_attr.routing_keys, self.routing_keys)

        # Check if queue_name is assigned properly
        self.assertEqual(amqp_msg_attr.queue_name, self.queue_name)


class ConsumerWorkerThreadTest(unittest.TestCase):
    """
    ConsumerWorkerThread unit test cases
    """

    def setUp(self):
        """
        Method to initialise the ConsumerWorkerThread parameters.
        :return: None
        """
        # Kombu's configurations
        self.kombu_queues = []
        self.callbacks = []
        self.prefetch_size_list = []
        self.prefetch_count_list = []
        self.callbacks = []

        # Broker configurations
        self.url = "localhost"
        self.port = 5672

        # Mocking the constructor of Connection class
        with mock.patch.object(Connection, "__init__") as mock_init:
            mock_init.return_value = None
            self.amqp_connection = Connection(hostname=self.url, port=self.port, transport="pyamqp")

    def tearDown(self):
        """
        Method to cleanup the resource created during the execution of test case.
        :return: None
        """
        # Kombu's configurations
        self.kombu_queues = None
        self.callbacks = None
        self.prefetch_size_list = None
        self.prefetch_count_list = None
        self.callbacks = None

        # Broker configurations
        self.url = None
        self.port = None
        self.amqp_connection = None

    @mock.patch.object(Thread, 'start')
    def test_class_implementation(self, mocked_start):
        """
        Test the ConsumerWorkerThread class implementation.
        :param mocked_start: Mocked start method from Thread class 
        :return: None
        """

        # Check ConsumerWorkerThread is extending the Thread class
        self.assertTrue(issubclass(ConsumerWorkerThread, Thread))

        ConsumerWorkerThread(self.amqp_connection, self.kombu_queues, self.callbacks,
                             self.prefetch_size_list, self.prefetch_count_list)

        # Check mocked method call has been made.
        mocked_start.assert_called()

    @mock.patch.object(AmqpConsumerWorker, 'run')
    @mock.patch.object(AmqpConsumerWorker, '__init__')
    def test_run_implementation(self, mocked_init, mocked_run):
        """
        Test the ConsumerWorkerThread run method implementation.
        :param mocked_init: Mocked init method from AmqpConsumerWorker class
        :param mocked_run: Mocked run method from AmqpConsumerWorker class
        :return: None
        """

        # Mocked method return value
        mocked_init.return_value = None

        ConsumerWorkerThread(self.amqp_connection, self.kombu_queues, self.callbacks,
                             self.prefetch_size_list, self.prefetch_count_list)

        # Check mocked method call has been made
        mocked_init.assert_called_with(self.amqp_connection, self.kombu_queues, self.callbacks,
                                       self.prefetch_size_list, self.prefetch_count_list)

        # Check mocked method call has been made
        mocked_run.assert_called()

    @mock.patch.object(ConsumerWorkerThread, 'start')
    @mock.patch.object(AmqpConsumerWorker, 'run')
    @mock.patch.object(AmqpConsumerWorker, '__init__')
    def test_run_implementation_exception(self, mocked_init, mocked_run, mocked_start):
        """
        Test the ConsumerWorkerThread run method exception flow.
        :param mocked_init: Mocked init method from AmqpConsumerWorker class
        :param mocked_run: Mocked run method from AmqpConsumerWorker class
        :param mocked_start: Mocked start method from ConsumerWorkerThread class
        :return: None
        """

        # Assign return value to mocked method
        mocked_init.return_value = None

        # Assign method to be invoked on calling the mocked method
        mocked_init.side_effect = mock_exception

        with mock.patch.object(ConsumerWorkerThread, 'stop') as mock_stop:
            consumer_thread = ConsumerWorkerThread(self.amqp_connection, self.kombu_queues, self.callbacks,
                                                   self.prefetch_size_list, self.prefetch_count_list)
            # Call run method
            consumer_thread.run()

            # Check stop call has been made
            mock_stop.assert_called()

    @mock.patch.object(ConsumerWorkerThread, 'start')
    @mock.patch.object(AmqpConsumerWorker, 'run')
    @mock.patch.object(AmqpConsumerWorker, '__init__')
    def test_stop_implementation(self, mocked_init, mocked_run, mocked_start):
        """
        Test the ConsumerWorkerThread stop method implementation.
        :param mocked_init: Mocked init method from AmqpConsumerWorker class
        :param mocked_run: Mocked run method from AmqpConsumerWorker class
        :param mocked_start: Mocked start method from ConsumerWorkerThread class
        :return: None
        """

        # Assign return value to constructor
        mocked_init.return_value = None

        consumer_thread = ConsumerWorkerThread(self.amqp_connection, self.kombu_queues, self.callbacks,
                                               self.prefetch_size_list, self.prefetch_count_list)
        # Call run method
        consumer_thread.run()

        # Call stop method
        consumer_thread.stop()

        # Check should_stop value
        self.assertTrue(consumer_thread._consumer.should_stop)

    @mock.patch.object(ConsumerWorkerThread, 'start')
    @mock.patch.object(AmqpConsumerWorker, 'run')
    @mock.patch.object(AmqpConsumerWorker, '__init__')
    def test_stop_implementation_stopped_consumer(self, mocked_init, mocked_run, mocked_start):
        """
        Test the ConsumerWorkerThread stop method implementation for stopped consumer flow.
        :param mocked_init: Mocked init method from AmqpConsumerWorker class
        :param mocked_run: Mocked run method from AmqpConsumerWorker class
        :param mocked_start: Mocked start method from ConsumerWorkerThread class
        :return: None 
        """

        # Assign return value to constructor
        mocked_init.return_value = None

        consumer_thread = ConsumerWorkerThread(self.amqp_connection, self.kombu_queues, self.callbacks,
                                               self.prefetch_size_list, self.prefetch_count_list)
        # Call run method
        consumer_thread.run()

        # Explicitly setting a _consumer to None to check the code flow
        consumer_thread._consumer = None

        # Call stop method
        consumer_thread.stop()


class AmqpConsumerWorkerTest(unittest.TestCase):
    """
    AmqpConsumerWorkerTest unit test cases
    """

    def setUp(self):
        """
        Method to initialise the AmqpConsumerWorkerTest parameters.
        :return: None
        """

        # Broker configuration
        self.url = "localhost"
        self.port = 5672

        # Consumer messaging attributes
        self.consume_msg_attr = AmqpConsumeMessagingAttributes(exchange_name="test-exchange",
                                                               routing_keys=["test1-key"], prefetch_count=1,
                                                               prefetch_size=1)
        # Kombu's configurations
        self.kombu_queues = []
        self.callbacks = [callback_method]
        self.prefetch_size_list = []
        self.prefetch_count_list = []

        self.exchange = Exchange(name=self.consume_msg_attr.exchange_name, type=self.consume_msg_attr.exchange_type)
        self.exchange.durable = self.consume_msg_attr.exchange_durable
        self.exchange.delivery_mode = 1

        self.kombu_queue = KombuQueue(name=self.consume_msg_attr.queue_name,
                                      exchange=self.exchange, binding_arguments=self.consume_msg_attr.header_args)

        self.kombu_queue.durable = self.consume_msg_attr.queue_durable
        self.kombu_queue.exclusive = self.consume_msg_attr.queue_exclusive
        self.kombu_queue.auto_delete = self.consume_msg_attr.queue_auto_delete
        self.kombu_queue.no_ack = self.consume_msg_attr.queue_no_ack
        self.kombu_queues.append(self.kombu_queue)

        self.prefetch_size_list.append(self.consume_msg_attr.prefetch_size)
        self.prefetch_count_list.append(self.consume_msg_attr.prefetch_count)

        # Mocking the constructor of Connection class
        with mock.patch.object(Connection, "__init__") as mock_init:
            mock_init.return_value = None

            self.amqp_connection = Connection(hostname=self.url, port=self.port, transport="pyamqp")

            self.consumer = AmqpConsumerWorker(self.amqp_connection, self.kombu_queues, self.callbacks,
                                               self.prefetch_size_list, self.prefetch_count_list)

    def tearDown(self):
        """
        Method to cleanup the resource created during the execution of test case.
        :return: None
        """
        # Kombu's configuration
        self.kombu_queues = None
        self.callbacks = None
        self.prefetch_size_list = None
        self.prefetch_count_list = None
        self.exchange = None
        self.kombu_queue = None
        self.amqp_connection = None
        self.consumer = None

        # Broker configurations
        self.url = None
        self.port = None
        self.consume_msg_attr = None

    def test_class_implementation(self):
        """
        Test the AmqpConsumerWorkerTest class implementation
        :return: None 
        """
        # Check the amqp connection object
        self.assertEqual(self.consumer.connection, self.amqp_connection)

        # Check the queue attribute
        self.assertEqual(self.consumer.queues, self.kombu_queues)

        # Check the prefetch_size_list attribute
        self.assertEqual(self.consumer.prefetch_size_list, self.prefetch_size_list)

        # Check the prefetch_count_list attribute
        self.assertEqual(self.consumer.prefetch_count_list, self.prefetch_count_list)

        # Check the callbacks attribute
        self.assertEqual(self.consumer.callbacks, self.callbacks)

    def test_init_validation_connection(self):
        """
        Test the validation for connection object.
        :return: None 
        """

        # Pass invalid connection object
        with self.assertRaises(TypeError):
            AmqpConsumerWorker(None, self.kombu_queues, self.callbacks, self.prefetch_count_list,
                               self.prefetch_count_list)

    def test_init_validation_queues_callbacks(self):
        """
        Test the validation for queues callback object.
        :return: None 
        """

        # Pass invalid queues and callbacks object
        with self.assertRaises(TypeError):
            AmqpConsumerWorker(self.amqp_connection, None, None, self.prefetch_count_list,
                               self.prefetch_count_list)

    def test_init_with_callbacks_parameter(self):
        """
        Test the AmqpConsumerWorker class implementation with callbacks parameter.
        :return:None 
        """
        # Creating object with valid non zero callBacks
        test_amqp_consumer_worker = AmqpConsumerWorker(self.amqp_connection, self.kombu_queues, self.callbacks,
                                                       self.prefetch_count_list, self.prefetch_count_list)

        self.assertEqual(test_amqp_consumer_worker.callbacks, self.callbacks)

    def test_inti_without_callbacks_parameter(self):
        """
        Test the AmqpConsumerWorker class implementation without callbacks parameter.
        :return:None 
        """
        # Creating object with empty callbacks list
        test_amqp_consumer_worker = AmqpConsumerWorker(self.amqp_connection, self.kombu_queues, [],
                                                       self.prefetch_count_list, self.prefetch_count_list)

        self.assertEqual(test_amqp_consumer_worker.callbacks, [test_amqp_consumer_worker.on_message])

    @mock.patch.object(Message, "ack")
    def test_on_message(self, mocked_ack):
        """
        Test the implementation of on_message method.
        :param mocked_ack: Mocked ack method from Kombu's Message class 
        :return: None
        """
        # Create AmqpConsumerWorker object
        amqp_consumer = AmqpConsumerWorker(self.amqp_connection, self.kombu_queues, self.callbacks,
                                           self.prefetch_count_list, self.prefetch_count_list)
        # Create Kombu message object
        amqp_message = Message()

        # Make call to on_message method
        amqp_consumer.on_message("test_message", amqp_message)

        # Check implementation calling the ack method
        mocked_ack.assert_called()

    @mock.patch.object(Consumer, "qos")
    @mock.patch.object(Consumer, "__init__")
    def test_get_consumers_implementation(self, mocked_init, mocked_qos):
        """
        Test the implementation of get_consumers.
        :param mocked_init: Mocked init from Consumer class 
        :param mocked_qos: Mocked qos from Consumer class
        :return: None
        """

        # Assign return value to mocked init
        mocked_init.return_value = None

        # Create AmqpConsumerWorker object
        amqp_consumer = AmqpConsumerWorker(self.amqp_connection, self.kombu_queues, self.callbacks,
                                           self.prefetch_count_list, self.prefetch_count_list)
        # Call get_consumers
        list_consumers = amqp_consumer.get_consumers(Consumer, None)

        # CHeck the constructor method called with following params
        mocked_init.assert_called_with(queues=[self.kombu_queues[0]], callbacks=[self.callbacks[0]],
                                       accept=['json', 'pickle', 'msgpack', 'yaml'])

        mocked_qos.assert_called_with(prefetch_size=self.prefetch_size_list[0],
                                      prefetch_count=self.prefetch_count_list[0],
                                      apply_global=False)

        # Check the return type of getConsumers
        self.assertIsInstance(list_consumers, list)

        # Check exactly one object is present in the array
        self.assertEqual(len(list_consumers), 1)

        # Check the type of object in the array
        self.assertIsInstance(list_consumers[0], Consumer)


class AmqpTest(unittest.TestCase):
    """
    Amqp unit test cases
    """

    def setUp(self):
        """
        Method to initialise the parameters for Amqp class.
        :return: None
        """
        # EdgeSystem name
        self.edge_system = Dell5KEdgeSystem("TestEdgeSystem")

        # Broker parameters
        self.url = "127.0.0.1"
        self.port = 5672
        self.amqp_username = "test"
        self.amqp_password = "test"
        self.enable_authentication = True
        self.connection_timeout_sec = 1

        # EdgeSystem name
        self.edge_system = Dell5KEdgeSystem("TestEdgeSystem")

        # TLS configurations
        self.root_ca_cert = "/etc/liota/mqtt/conf/ca.crt"
        self.client_cert_file = "/etc/liota/mqtt/conf/client.crt"
        self.client_key_file = "/etc/liota/mqtt/conf/client.key"
        self.cert_required = "CERT_REQUIRED"
        self.tls_version = "PROTOCOL_TLSv1"
        self.cipher = None

        # Encapsulate the authentication details
        self.identity = Identity(self.root_ca_cert, self.amqp_username, self.amqp_password,
                                 self.client_cert_file, self.client_key_file)

        # Encapsulate TLS parameters
        self.tls_conf = TLSConf(self.cert_required, self.tls_version, self.cipher)

        # Amqp configurations
        self.amqp_msg_attr = [AmqpConsumeMessagingAttributes(exchange_name="test1_exchange",
                                                             routing_keys=["test1", "test2"],
                                                             callback=custom_callback),
                              AmqpConsumeMessagingAttributes(exchange_name="test2_exchange",
                                                             routing_keys=["test1", "test2"],
                                                             callback=custom_callback)]
        self.header_args = {
            "test": "test1"
        }

        self.amqp_pub_msg_attr = AmqpPublishMessagingAttributes(edge_system_name=self.edge_system.name,
                                                                exchange_type=DEFAULT_EXCHANGE_TYPE,
                                                                exchange_durable=False)
        # Create amqp client
        with mock.patch.object(Amqp, "_init_or_re_init"):
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

    def tearDown(self):
        """
        Method to cleanup the resource created during the execution of test case.
        :return: None
        """

        # EdgeSystem name
        self.edge_system = None

        # Broker parameters
        self.url = None
        self.port = None
        self.amqp_username = None
        self.amqp_password = None
        self.enable_authentication = None
        self.connection_timeout_sec = None

        # EdgeSystem name
        self.edge_system = None

        # TLS configurations
        self.root_ca_cert = None
        self.client_cert_file = None
        self.client_key_file = None
        self.cert_required = None
        self.tls_version = None
        self.cipher = None

        # Encapsulate the authentication details
        self.identity = None

        # Encapsulate TLS parameters
        self.tls_conf = None

        # Amqp configurations
        self.amqp_msg_attr = None
        self.header_args = None
        self.amqp_pub_msg_attr = None
        self.amqp_client = None

    def test_class_implementation(self):
        """
        Test the Amqp class implementation. 
        :return: None 
        """
        # Mock the _init_or_re_init method
        with mock.patch.object(Amqp, "_init_or_re_init") as _init_or_re_init:
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

            # Check mocked method call has been made
            _init_or_re_init.assert_called()

            # Check broker URL
            self.assertEqual(self.url, self.amqp_client.url)

            # Check broker port
            self.assertEqual(self.port, self.amqp_client.port)

            # Check identity
            self.assertEqual(self.identity, self.amqp_client.identity)

            # Check tls_conf
            self.assertEqual(self.tls_conf, self.amqp_client.tls_conf)

            # Check connection_timeout_sec
            self.assertEqual(self.connection_timeout_sec, self.amqp_client.connection_timeout_sec)

    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(Amqp, "connect_soc")
    @mock.patch.object(Amqp, "disconnect")
    def test_init_or_re_init_implementation(self, mocked_disconnect, mocked_connect_soc, _initialize_producer):
        """
        Test the _init_or_re_init method implementation.
        :param mocked_disconnect: Mocked disconnect method from Amqp class 
        :param mocked_connect_soc: Mocked connect_soc method from Amqp class
        :param _initialize_producer: Mocked _initialize_producer from Amqp class
        :return: None
        """

        # Create amqp client
        self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                self.connection_timeout_sec)

        # Check disconnect method call has been made
        mocked_disconnect.assert_called()

        # Check connect_soc method call has need made
        mocked_connect_soc.assert_called()

        # Check _initialize_producer method called has been made
        _initialize_producer.assert_called()

    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(Amqp, "connect_soc")
    @mock.patch.object(Amqp, "disconnect_producer")
    @mock.patch.object(Amqp, "disconnect_consumer")
    @mock.patch.object(pools, "reset")
    def test_disconnect_implementation(self, mocked_reset, mocked_disconnect_consumer, mocked_disconnect_producer,
                                       mocked_connect_soc, mocked_initialize_producer):
        """
        Test the disconnect method implementation.
        :param mocked_reset: Mocked reset method from Kombu's pools class
        :param mocked_disconnect_consumer: Mocked disconnect_consumer method from Amqp class 
        :param mocked_disconnect_producer: Mocked disconnect_producer method from Amqp class
        :param mocked_connect_soc: Mocked connect_soc from Amqp class
        :param mocked_initialize_producer: Mocked _initialize_producer method from Amqp class
        :return: None
        """

        # Create amqp client
        self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                self.connection_timeout_sec)

        # Check disconnect producer call has been made
        mocked_disconnect_producer.aseert_called()

        # Check disconnect producer call has been made
        mocked_disconnect_consumer.assert_called()

        # Check the reset call has been made
        mocked_reset.assert_called()

        # Check value of _connection_pool
        self.assertEqual(self.amqp_client._connection_pool, None)

    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(Amqp, "connect_soc")
    @mock.patch.object(Amqp, "disconnect_producer")
    @mock.patch.object(Amqp, "disconnect_consumer")
    @mock.patch.object(pools, "reset")
    def test_disconnect_implementation_exception_flow(self, mocked_reset, mocked_disconnect_consumer,
                                                      mocked_disconnect_producer,
                                                      mocked_connect_soc, mocked_initialize_producer):
        """
        Test the disconnect method implementation for exception flow.
        :param mocked_reset: Mocked reset method from Kombu's pool class
        :param mocked_disconnect_consumer: Mocked disconnect_consumer method from Amqp class 
        :param mocked_disconnect_producer: 
        :param mocked_connect_soc: 
        :param mocked_initialize_producer: 
        :return: None
        """
        # Assign method to be invoked on mocked method call
        mocked_reset.side_effect = mock_exception

        with self.assertRaises(Exception):
            # Create amqp client
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

            # Check disconnect producer call has been made
            mocked_disconnect_producer.aseert_called()

            # Check disconnect producer call has been made
            mocked_disconnect_consumer.assert_called()

            # Check the reset call has been made
            mocked_reset.assert_called()

    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(Amqp, "connect_soc")
    @mock.patch.object(Amqp, "disconnect_producer")
    @mock.patch.object(Amqp, "disconnect_consumer")
    @mock.patch.object(pools, "reset")
    def test_disconnect_implementation_exception_flow(self, mocked_reset, mocked_disconnect_consumer,
                                                      mocked_disconnect_producer,
                                                      mocked_connect_soc, mocked_initialize_producer):
        """
        Test the disconnect method implementation for exception flow.
        :param mocked_reset: Mocked reset from Kobus pools class
        :param mocked_disconnect_consumer: Mocked disconnect_consumer method from Amqp class
        :param mocked_disconnect_producer: Mocked disconnect_producer method from Amqp class
        :param mocked_connect_soc: Mocked connect_soc method from Amqp class
        :param mocked_initialize_producer: Mocked _initialize_producer method from Amqp class
        :return: None
        """

        # Assign method to be invoked on mocked method call
        mocked_reset.side_effect = mock_exception

        with self.assertRaises(Exception):
            # Create amqp client
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

            # Check disconnect producer call has been made
            mocked_disconnect_producer.aseert_called()

            # Check disconnect producer call has been made
            mocked_disconnect_consumer.assert_called()

            # Check the reset call has been made
            mocked_reset.assert_called()

    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Amqp, "_initialize_producer")
    def test_connect_implementation_username_validation(self, mocked_initialize_producer, mocked_disconnect):
        """
        Test the connect method validation for empty username.
        :param mocked_initialize_producer: Mocked  _initialize_producer method from Amqp class
        :param mocked_disconnect: Mocked disconnect method from Amqp class
        :return: None
        """

        # Check the implementation raising the ValueError for empty username
        with self.assertRaises(ValueError):
            # Encapsulate the authentication details
            self.identity = Identity(self.root_ca_cert, "", self.amqp_password, self.client_cert_file,
                                     self.client_key_file)

            # Create amqp client
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Amqp, "_initialize_producer")
    def test_connect_implementation_password_validation(self, mocked_initialize_producer, mocked_disconnect):
        """
        Test the connect method validation for empty password.
        :param mocked_initialize_producer: Mocked _initialize_producer method from Amqp class
        :param mocked_disconnect: Mocked disconnect from Amqp class
        :return: None
        """

        # Check the implementation raising the ValueError for empty username
        with self.assertRaises(ValueError):
            # Encapsulate the authentication details
            self.identity = Identity(self.root_ca_cert, self.amqp_username, "", self.client_cert_file,
                                     self.client_key_file)

            # Create amqp client
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Amqp, "_initialize_producer")
    def test_connect_implementation_root_ca_path_validation(self, mocked_initialize_producer, mocked_disconnect):
        """
        Test the connect method validation for root_ca path exists.
        :param mocked_initialize_producer: Mocked _initialize_producer method from Amqp class
        :param mocked_disconnect: Mocked disconnect from Amqp class
        :return: None
        """

        # Check the implementation raising the ValueError for empty username
        with self.assertRaises(ValueError):
            # Encapsulate the authentication details
            self.identity = Identity(self.root_ca_cert, self.amqp_username, self.amqp_password, self.client_cert_file,
                                     self.client_key_file)

            # Create amqp client
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Amqp, "_initialize_producer")
    def test_connect_implementation_root_ca_empty_validation(self, mocked_initialize_producer, mocked_disconnect):
        """
        Test the connect method validation for empty root_ca path.
        :param mocked_initialize_producer: Mocked  _initialize_producer method from Amqp class
        :param mocked_disconnect: Mocked disconnect from Amqp class
        :return: None 
        """

        # Check the implementation raising the ValueError for empty username
        with self.assertRaises(ValueError):
            # Encapsulate the authentication details
            self.identity = Identity("", self.amqp_username, self.amqp_password, self.client_cert_file,
                                     self.client_key_file)

            # Create amqp client
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    def test_connect_implementation_client_cert_path_validation(self, mocked_exists,
                                                                mocked_initialize_producer, mocked_disconnect):
        """
        Test the connect method validation for client cert path exists.
        :param mocked_exists: Mocked exists from os.path
        :param mocked_initialize_producer: Mocked _initialize_producer from Amqp class
        :param mocked_disconnect: Mocked disconenct method from Amqp class
        :return: None
        """
        # Assign method to be invoked on mocked method call
        mocked_exists.side_effect = mocked_os_path_root_ca

        # Check the implementation raising the ValueError for empty username
        with self.assertRaises(ValueError):
            # Encapsulate the authentication details
            self.identity = Identity(self.root_ca_cert, self.amqp_username, self.amqp_password, self.client_cert_file,
                                     self.client_key_file)

            # Create amqp client
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    def test_connect_implementation_client_key_path_validation(self, mocked_exists,
                                                               mocked_initialize_producer, mocked_disconnect):
        """
        Test the connect method validation for client cert path.
        :param mocked_exists: Mocked exists from os.path
        :param mocked_initialize_producer: Mocked _initialize_producer from Amqp class
        :param mocked_disconnect: Mocked disconnect from Amqp class
        :return: None
        """

        # Assign method to be invoked on calling mocked method
        mocked_exists.side_effect = mocked_os_path_root_ca_and_client_crt

        # Check the implementation raising the ValueError for empty client cert.
        with self.assertRaises(ValueError):
            # Encapsulate the authentication details
            self.identity = Identity(self.root_ca_cert, self.amqp_username, self.amqp_password, self.client_cert_file,
                                     self.client_key_file)

            # Create amqp client
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    def test_connect_implementation_without_client_cert(self, mocked_exists,
                                                        mocked_initialize_producer, mocked_disconnect):
        """
        Test the connect method validation for valid client_key and empty client cert
        :param mocked_exists: Mocked exists from os.path 
        :param mocked_initialize_producer: Mocked _initialize_producer from Amqp class
        :param mocked_disconnect: Mocked disconnect from Amqp
        :return: None 
        """
        # Assign return value to os.path.exists
        mocked_exists.return_value = True

        # Check the implementation raising the ValueError for empty  client cert
        with self.assertRaises(ValueError):
            # Encapsulate the authentication details
            self.identity = Identity(self.root_ca_cert, self.amqp_username, self.amqp_password, "",
                                     self.client_key_file)

            # Create amqp client
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    def test_connect_implementation_without_client_key(self, mocked_exists,
                                                       mocked_initialize_producer, mocked_disconnect):
        """
        Test the connect method validation for valid client cert and empty client key.
        :param mocked_exists: Mocked exists from os.path
        :param mocked_initialize_producer: Mocked _initialize_producer method from Amqp class
        :param mocked_disconnect: Mocked disconnect method from Amqp class 
        :return: None 
        """

        # Assign return value to os.path.exists
        mocked_exists.return_value = True

        # Check the implementation raising the ValueError for empty  client key
        with self.assertRaises(ValueError):
            # Encapsulate the authentication details
            self.identity = Identity(self.root_ca_cert, self.amqp_username, self.amqp_password, self.client_cert_file,
                                     "")

            # Create amqp client
            self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                    self.connection_timeout_sec)

    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    def test_connect_implementation_with_ssl_username_password(self, mocked_init, mocked_exists,
                                                               _initialize_producer, disconnect):
        """
        Test the connect method implementation for connection with SSL and Username-Password.
        :param mocked_init: Mocked init method from Connection class
        :param mocked_exists: Mocked exists from os.path
        :param _initialize_producer: Mocked _initialize_producer from Amqp class
        :param disconnect: Mocked disconnect method from Amqp
        :return: None
        """

        # Assign return value to os.path.exists
        mocked_exists.return_value = True
        mocked_init.return_value = None

        # Create amqp client
        self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                self.connection_timeout_sec)

        # Setup ssl options
        ssl_details = {'ca_certs': self.identity.root_ca_cert,
                       'certfile': self.identity.cert_file,
                       'keyfile': self.identity.key_file,
                       'cert_reqs': getattr(ssl, self.tls_conf.cert_required),
                       'ssl_version': getattr(ssl, self.tls_conf.tls_version),
                       'ciphers': self.tls_conf.cipher}

        mocked_init.assert_called_with(hostname=self.url, port=self.port, transport="pyamqp",
                                       userid=self.identity.username, password=self.identity.password,
                                       ssl=ssl_details, connect_timeout=self.connection_timeout_sec)

    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(Connection, "__init__")
    def test_connect_implementation_without_ssl_username_password(self, mocked_init, _initialize_producer, disconnect):
        """
        Test the connect method for the connection without SSL and username-password configuration.
        :param mocked_init: Mocked init method from Connection class
        :param _initialize_producer: Mocked _initialize_producer method from Amqp class
        :param disconnect: Mocked disconnect method from Amqp class
        :return: None
        """
        # Assign return value to os.path.exists
        mocked_init.return_value = None

        # Create amqp client
        self.amqp_client = Amqp(self.url, self.port, None, None, False,
                                self.connection_timeout_sec)

        # Check Connection init called with following params
        mocked_init.assert_called_with(hostname=self.url, port=self.port, transport="pyamqp",
                                       userid=None, password=None,
                                       ssl=False, connect_timeout=self.connection_timeout_sec)

    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    def test_connect_implementation_exception_flow(self, mocked_init, mocked_exists, _initialize_producer,
                                                   disconnect):
        """
        Test the connect method exception flow.
        :param mocked_init: Mocked init method from Connection class 
        :param mocked_exists: Mocked exists method from os.path
        :param _initialize_producer: Mocked _initialize_producer method from Amqp class
        :param disconnect: Mocked disconnect method from Amqp class
        :return: None
        """

        # Assign return value to os.path.exists
        mocked_exists.return_value = True
        mocked_init.return_value = None
        mocked_init.side_effect = mock_exception

        with self.assertRaises(Exception):
            # Create amqp client
            self.amqp_client = Amqp(self.url, self.port, None, None, False, self.connection_timeout_sec)

    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Connection, "channel")
    @mock.patch.object(Resource, "acquire")
    @mock.patch.object(Producer, "__init__")
    def test_initialize_producer_implementation(self, mocked_producer_init, mocked_acquire, mocked_channel,
                                                mocked_disconnect, mocked_connection_init, mocked_exists):
        """
        Test the _initialize_producer method implementation.
        :param mocked_producer_init: Mocked init method from Producer class
        :param mocked_acquire: Mocked acquire from Resource class
        :param mocked_channel: Mocked channel from Connection class
        :param mocked_disconnect: Mocked disconnect from Amqp class
        :param mocked_connection_init: Mocked init from Connection class
        :param mocked_exists: Mocked exists method from os.path
        :return: None
        """

        # Assign return_value to mocked methods
        mocked_producer_init.return_value = None
        mocked_connection_init.return_value = None
        mocked_exists.return_value = True

        # Setup ssl options
        ssl_details = {'ca_certs': self.identity.root_ca_cert,
                       'certfile': self.identity.cert_file,
                       'keyfile': self.identity.key_file,
                       'cert_reqs': getattr(ssl, self.tls_conf.cert_required),
                       'ssl_version': getattr(ssl, self.tls_conf.tls_version),
                       'ciphers': self.tls_conf.cipher}

        amqp_connection = Connection(hostname=self.url, port=self.port, transport="pyamqp",
                                     userid=self.identity.username if self.enable_authentication else None,
                                     password=self.identity.password if self.enable_authentication else None,
                                     ssl=ssl_details if self.tls_conf else False,
                                     connect_timeout=self.connection_timeout_sec)

        # Return connection object from acquire
        mocked_acquire.return_value = amqp_connection

        # Return mock connection channel from channel
        mocked_channel.return_value = "Connection channel"

        # Create amqp client
        self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                self.connection_timeout_sec)

        # Check acquire method call has been made
        mocked_acquire.assert_called()

        # Check channel method call has been made
        mocked_channel.assert_called()

        # Check Producer object creation has been made
        mocked_producer_init.assert_called_with("Connection channel")

    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Resource, "acquire")
    def test_initialize_consumer_connection(self, mocked_acquire, mocked_disconnect, mocked_init, mocked_exists,
                                            mocked_initialize_producer):
        """
        Method to test the implementation of _initialize_consumer_connection method.
        :param mocked_acquire: Mocked acquire from Resource class
        :param mocked_disconnect: Mocked disconenct from Amqp class
        :param mocked_init: Mocked init from Connection class
        :param mocked_exists: Mocked exists from os.path
        :param mocked_initialize_producer: Mocked _initialize_producer from Amqp class
        :return: None
        """

        # Assign return value to mocked methods
        mocked_init.return_value = None

        amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf,
                           self.enable_authentication, self.connection_timeout_sec)

        # Call _initialize_consumer_connection method
        amqp_client._initialize_consumer_connection()

        # Check implementation calling the acquire method
        mocked_acquire.assert_called()

    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Resource, "acquire")
    def test_initialize_consumer_connection_exception_flow(self, mocked_acquire, mocked_disconnect, mocked_init,
                                                           mocked_exists, mocked_initialize_producer):
        """
        Method to test the implementation of _initialize_consumer_connection method for exception flow.
        :param mocked_acquire: Mocked acquire from Resouce class
        :param mocked_disconnect: Mocked disconnect from Amqp class
        :param mocked_init: Mocked init from Connection class
        :param mocked_exists: Mocked exists from os.path
        :param mocked_initialize_producer: Mocked _initialize_producer from Amqp class
        :return: None
        """

        # Assign None return type to mocked init method
        mocked_init.return_value = None

        # Raise exception on method call
        mocked_acquire.side_effect = mock_exception

        # Check underline implementation for Exception
        with self.assertRaises(Exception):
            # Create Amqp client
            amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf,
                               self.enable_authentication, self.connection_timeout_sec)

            # Initialise the _consumer to test the exception flow
            amqp_client._consumer = True

            # Initialise the consumer connection
            amqp_client._initialize_consumer_connection()

    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Resource, "acquire")
    def test_consumer_validation_exception_flow(self, mocked_acquire, mocked_disconnect, mocked_init,
                                                mocked_exists, mocked_initialize_producer):
        """
        Method to test the validation of _initialize_consumer_connection method for exception flow.
        :param mocked_acquire: Mocked acquire from Resource class
        :param mocked_disconnect: Mocked disconnect from Amqp class
        :param mocked_init: Mocked init from Connection class
        :param mocked_exists: Mocked exists from os.path
        :param mocked_initialize_producer: Mocked _initialize_producer from Amqp class
        :return: None
        """

        # Assign None return type to mocked init method
        mocked_init.return_value = None

        with self.assertRaises(TypeError):
            # Create Amqp client
            amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf,
                               self.enable_authentication, self.connection_timeout_sec)

            # Initialise the consumer connection
            amqp_client.consume(None)

    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Resource, "acquire")
    @mock.patch.object(ConsumerWorkerThread, "__init__")
    def test_consumer_implementation(self, mocked_worker_init, mocked_acquire, mocked_disconnect, mocked_init,
                                     mocked_exists, mocked_initialize_producer):
        """
        Test the implementation of consumer method.
        :param mocked_worker_init: Mocked init method from ConsumerWorkerThread class
        :param mocked_acquire: Mocked acquire method from Resource class
        :param mocked_disconnect: Mocked disconnect method from Amqp class
        :param mocked_init: Mocked init method from Connection class
        :param mocked_exists: Mocked exists from os.path
        :param mocked_initialize_producer: Mocked _initialize_producer from Amqp class
        :return: None
        """

        # Assign None return type to mocked init method
        mocked_init.return_value = None
        mocked_worker_init.return_value = None
        mocked_acquire.return_value = None

        # Create Amqp client
        amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf,
                           self.enable_authentication, self.connection_timeout_sec)

        # Initialise the consumer connection
        amqp_client.consume(self.amqp_msg_attr)

        kombu_queues = []
        callbacks = []
        prefetch_size_list = []
        prefetch_count_list = []

        consume_msg_attr_list = self.amqp_msg_attr

        for consume_msg_attr in consume_msg_attr_list:

            exchange = Exchange(name=consume_msg_attr.exchange_name, type=consume_msg_attr.exchange_type)
            exchange.durable = consume_msg_attr.exchange_durable
            # delivery_mode at exchange level is transient
            # However, publishers can publish messages with delivery_mode persistent
            exchange.delivery_mode = 1

            if not 'headers' == consume_msg_attr.exchange_type:
                kombu_queue = KombuQueue(name=consume_msg_attr.queue_name,
                                         # A queue can be bound with an exchange with one or more routing keys
                                         # creating a binding between exchange and routing_key
                                         bindings=[binding(exchange=exchange, routing_key=_)
                                                   for _ in consume_msg_attr.routing_keys
                                                   ]
                                         )
            else:
                kombu_queue = KombuQueue(name=consume_msg_attr.queue_name,
                                         exchange=exchange,
                                         binding_arguments=consume_msg_attr.header_args
                                         )
            kombu_queue.durable = consume_msg_attr.queue_durable
            kombu_queue.exclusive = consume_msg_attr.queue_exclusive
            kombu_queue.auto_delete = consume_msg_attr.queue_auto_delete
            kombu_queue.no_ack = consume_msg_attr.queue_no_ack

            kombu_queues.append(kombu_queue)
            callbacks.append(consume_msg_attr.callback)
            prefetch_size_list.append(consume_msg_attr.prefetch_size)
            prefetch_count_list.append(consume_msg_attr.prefetch_count)

        # Check underline implementation calling woker thread with following params
        mocked_worker_init.assert_called_with(None, kombu_queues, callbacks, prefetch_size_list, prefetch_count_list)

    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Resource, "acquire")
    @mock.patch.object(ConsumerWorkerThread, "__init__")
    def test_consumer_implementation_headers_exchange_type(self, mocked_worker_init, mocked_acquire, mocked_disconnect,
                                                           mocked_init, mocked_exists, mocked_initialize_producer):
        """
        Test the implementation of consumer method implementation for headers exchange type.
        :param mocked_worker_init: Mocked init from ConsumerWorkerThread class
        :param mocked_acquire: Mocked acquire from Resource class
        :param mocked_disconnect: Mocked disconnect from Amqp
        :param mocked_init: Mocked init from Connection class
        :param mocked_exists: Mocked exists from os.path
        :param mocked_initialize_producer: Mocked _initialize_producer from Amqp class
        :return: None
        """

        # Assign None return type to mocked init method
        mocked_init.return_value = None
        mocked_worker_init.return_value = None
        mocked_acquire.return_value = None

        # Create consume msg attributes
        self.amqp_msg_attr = [AmqpConsumeMessagingAttributes(exchange_name="test1_exchange", exchange_type="headers",
                                                             routing_keys=["test1", "test2"],
                                                             header_args=self.header_args),
                              AmqpConsumeMessagingAttributes(exchange_name="test2_exchange", exchange_type="headers",
                                                             routing_keys=["test1", "test2"],
                                                             header_args=self.header_args)]

        # Create Amqp client
        amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf,
                           self.enable_authentication, self.connection_timeout_sec)

        # Initialise the consumer connection
        amqp_client.consume(self.amqp_msg_attr)

        kombu_queues = []
        callbacks = []
        prefetch_size_list = []
        prefetch_count_list = []

        consume_msg_attr_list = self.amqp_msg_attr

        for consume_msg_attr in consume_msg_attr_list:

            exchange = Exchange(name=consume_msg_attr.exchange_name, type=consume_msg_attr.exchange_type)
            exchange.durable = consume_msg_attr.exchange_durable
            # delivery_mode at exchange level is transient
            # However, publishers can publish messages with delivery_mode persistent
            exchange.delivery_mode = 1

            if not 'headers' == consume_msg_attr.exchange_type:
                kombu_queue = KombuQueue(name=consume_msg_attr.queue_name,
                                         # A queue can be bound with an exchange with one or more routing keys
                                         # creating a binding between exchange and routing_key
                                         bindings=[binding(exchange=exchange, routing_key=_)
                                                   for _ in consume_msg_attr.routing_keys
                                                   ]
                                         )
            else:
                kombu_queue = KombuQueue(name=consume_msg_attr.queue_name,
                                         exchange=exchange,
                                         binding_arguments=consume_msg_attr.header_args
                                         )
            kombu_queue.durable = consume_msg_attr.queue_durable
            kombu_queue.exclusive = consume_msg_attr.queue_exclusive
            kombu_queue.auto_delete = consume_msg_attr.queue_auto_delete
            kombu_queue.no_ack = consume_msg_attr.queue_no_ack

            kombu_queues.append(kombu_queue)
            callbacks.append(consume_msg_attr.callback)
            prefetch_size_list.append(consume_msg_attr.prefetch_size)
            prefetch_count_list.append(consume_msg_attr.prefetch_count)

        # Check underline implementation creating the worker with following params
        mocked_worker_init.assert_called_with(None, kombu_queues, callbacks, prefetch_size_list, prefetch_count_list)

    @mock.patch.object(Exchange, "declare")
    def test_declare_publish_exchange_implementation(self, mocked_declare):
        """
        Test the implementation of declare_publish_exchange method.
        :param mocked_declare: Mocked declare from Exchange class 
        :return: None
        """

        # Call the declare_publish_exchange
        self.amqp_client.declare_publish_exchange(self.amqp_pub_msg_attr)

        # Check declare method call has been made
        mocked_declare.assert_called()

        # CHeck the value of is_exchange_declared variable
        self.assertTrue(self.amqp_pub_msg_attr.is_exchange_declared)

    def test_declare_publish_exchange_validation(self):
        """
        Test the validation of declare_publish_exchange method.
        :return: None 
        """

        # Check implementation raising the TypeError
        with self.assertRaises(TypeError):
            self.amqp_client.declare_publish_exchange(None)

    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Connection, "channel")
    @mock.patch.object(Resource, "acquire")
    @mock.patch.object(Producer, "__init__")
    @mock.patch.object(Producer, "publish")
    def test_publish_implementation(self, mocked_producer_publish, mocked_producer_init, mocked_acquire,
                                    mocked_channel, mocked_disconnect, mocked_connection_init, mocked_exists):
        """
        Test the publish method implementation.
        :param mocked_producer_publish: Mocked publish method from Producer class 
        :param mocked_producer_init: Mocked init method from Producer class
        :param mocked_acquire: Mocked acquire method from Resource class
        :param mocked_channel: Mocked channel from Connection class 
        :param mocked_disconnect: Mocked disconenct from Amqp class
        :param mocked_connection_init: Mocked init from Connection class
        :param mocked_exists: Mocked exists from os.path
        :return: None
        """

        # Assign the return values to mocked methods
        mocked_producer_publish.return_value = None
        mocked_connection_init.return_value = None
        mocked_exists.return_value = True
        mocked_producer_init.return_value = None

        # Setup ssl options
        ssl_details = {'ca_certs': self.identity.root_ca_cert,
                       'certfile': self.identity.cert_file,
                       'keyfile': self.identity.key_file,
                       'cert_reqs': getattr(ssl, self.tls_conf.cert_required),
                       'ssl_version': getattr(ssl, self.tls_conf.tls_version),
                       'ciphers': self.tls_conf.cipher}

        amqp_connection = Connection(hostname=self.url, port=self.port, transport="pyamqp",
                                     userid=self.identity.username if self.enable_authentication else None,
                                     password=self.identity.password if self.enable_authentication else None,
                                     ssl=ssl_details if self.tls_conf else False,
                                     connect_timeout=self.connection_timeout_sec)

        # Return connection object from acquire
        mocked_acquire.return_value = amqp_connection

        # Return mock connection channel from channel
        mocked_channel.return_value = "Connection channel"

        # Create amqp client
        self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                self.connection_timeout_sec)

        self.amqp_client.publish("Test exchange", "Test-route-key", "Test message", DEFAULT_PUBLISH_PROPERTIES)

        # Check publish called with following params
        mocked_producer_publish.assert_called_with(body="Test message", exchange="Test exchange",
                                                   routing_key="Test-route-key",
                                                   content_type=DEFAULT_PUBLISH_PROPERTIES['content_type'],
                                                   delivery_mode=DEFAULT_PUBLISH_PROPERTIES['delivery_mode'],
                                                   headers=DEFAULT_PUBLISH_PROPERTIES['headers'])

    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Connection, "channel")
    @mock.patch.object(Resource, "acquire")
    @mock.patch.object(Producer, "__init__")
    @mock.patch.object(Producer, "publish")
    def test_publish_implementation_exception_flow(self, mocked_production_publish, mocked_producer_init,
                                                   mocked_acquire, mocked_channel, mocked_disconnect,
                                                   mocked_connection_init, mocked_exists):
        """
        Test the publish method exception flow.
        :param mocked_production_publish: Mocked publish method from Producer class 
        :param mocked_producer_init: Mocked init from Producer class
        :param mocked_acquire: Mocked acquire from Resource class
        :param mocked_channel: Mocked channel from Connection class
        :param mocked_disconnect: Mocked disconnect from Amqp class
        :param mocked_connection_init: Mocked init from Connection class
        :param mocked_exists: Mocked exists from os.path
        :return: None
        """
        # Assign return value to mocked methods
        mocked_producer_init.return_value = None
        mocked_connection_init.return_value = None
        mocked_exists.return_value = True
        # Assign method to be invoked on calling the mocked method
        mocked_production_publish.side_effect = mock_exception

        # Setup ssl options
        ssl_details = {'ca_certs': self.identity.root_ca_cert,
                       'certfile': self.identity.cert_file,
                       'keyfile': self.identity.key_file,
                       'cert_reqs': getattr(ssl, self.tls_conf.cert_required),
                       'ssl_version': getattr(ssl, self.tls_conf.tls_version),
                       'ciphers': self.tls_conf.cipher}

        amqp_connection = Connection(hostname=self.url, port=self.port, transport="pyamqp",
                                     userid=self.identity.username if self.enable_authentication else None,
                                     password=self.identity.password if self.enable_authentication else None,
                                     ssl=ssl_details if self.tls_conf else False,
                                     connect_timeout=self.connection_timeout_sec)

        # Return connection object from acquire
        mocked_acquire.return_value = amqp_connection

        # Return mock connection channel from channel
        mocked_channel.return_value = "Connection channel"

        # Create amqp client
        self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                self.connection_timeout_sec)

        # Check Amqp not raising the exception
        self.amqp_client.publish("Test exchange", "Test-route-key", "Test message", DEFAULT_PUBLISH_PROPERTIES)

    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Connection, "channel")
    @mock.patch.object(Resource, "acquire")
    @mock.patch.object(Producer, "__init__")
    @mock.patch.object(Connection, "release")
    def test_disconnect_producer_implementation(self, mocked_connection_release, mocked_production_init, mocked_acquire,
                                                mocked_channel, mocked_disconnect, mocked_connection_init,
                                                mocked_exists):
        """
        Test the disconnect_producer method implementation.
        :param mocked_connection_release: Mocked release from Connection class 
        :param mocked_production_init: Mocked init from Producer class
        :param mocked_acquire: Mocked acquire from Resource class
        :param mocked_channel: Mocked channel from Connection class
        :param mocked_disconnect: Mocked disconnect from Amqp class
        :param mocked_connection_init: Mocked __init__ from Connection class
        :param mocked_exists: Mocked exists from os.path
        :return: None 
        """

        # Assign return value to mocked methods
        mocked_production_init.return_value = None
        mocked_connection_init.return_value = None
        mocked_exists.return_value = True

        # Setup ssl options
        ssl_details = {'ca_certs': self.identity.root_ca_cert,
                       'certfile': self.identity.cert_file,
                       'keyfile': self.identity.key_file,
                       'cert_reqs': getattr(ssl, self.tls_conf.cert_required),
                       'ssl_version': getattr(ssl, self.tls_conf.tls_version),
                       'ciphers': self.tls_conf.cipher}

        amqp_connection = Connection(hostname=self.url, port=self.port, transport="pyamqp",
                                     userid=self.identity.username if self.enable_authentication else None,
                                     password=self.identity.password if self.enable_authentication else None,
                                     ssl=ssl_details if self.tls_conf else False,
                                     connect_timeout=self.connection_timeout_sec)

        # Return connection object from acquire
        mocked_acquire.return_value = amqp_connection

        # Return mock connection channel from channel
        mocked_channel.return_value = "Connection channel"

        # Create amqp client
        self.amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf, self.enable_authentication,
                                self.connection_timeout_sec)

        # Call disconnect_producer
        self.amqp_client.disconnect_producer()

        # Check underline implementation making the release method call
        mocked_connection_release.assert_called()

        # Check _publisher_connection is None
        self.assertIsNone(self.amqp_client._publisher_connection)

        # Check _producer is None
        self.assertIsNone(self.amqp_client._producer)

    @mock.patch.object(Amqp, "_initialize_producer")
    @mock.patch.object(os.path, "exists")
    @mock.patch.object(Connection, "__init__")
    @mock.patch.object(Amqp, "disconnect")
    @mock.patch.object(Resource, "acquire")
    @mock.patch.object(ConsumerWorkerThread, "__init__")
    @mock.patch.object(ConsumerWorkerThread, "stop")
    @mock.patch.object(Connection, "release")
    def test_disconnect_consumer_implementation(self, mocked_connection_release, mocked_worker_stop, mocked_worker_init,
                                                mocked_acquire, mocked_disconnect, mocked_init, mocked_exists,
                                                mocked_initialize_producer):
        """
        Test the implementation of disconnect_consumer method implementation.
        :param mocked_connection_release: Mocked release method from Connection class 
        :param mocked_worker_stop: Mocked stop method from ConsumerWorkerThread class
        :param mocked_worker_init: Mocked init method from ConsumerWorkerThread class
        :param mocked_acquire: Mocked require method from Resource class
        :param mocked_disconnect: Mocked disconnect method from Amqp class
        :param mocked_init: Mocked init method from Connection class
        :param mocked_exists: Mocked exists method from os.path
        :param mocked_initialize_producer: Mocked _initialize_producer method from Amqp class
        :return: None
        """

        # Assign None return value to mocked methods
        mocked_init.return_value = None
        mocked_worker_init.return_value = None
        mocked_exists.return_value = True

        # Setup ssl options
        ssl_details = {'ca_certs': self.identity.root_ca_cert,
                       'certfile': self.identity.cert_file,
                       'keyfile': self.identity.key_file,
                       'cert_reqs': getattr(ssl, self.tls_conf.cert_required),
                       'ssl_version': getattr(ssl, self.tls_conf.tls_version),
                       'ciphers': self.tls_conf.cipher}

        amqp_connection = Connection(hostname=self.url, port=self.port, transport="pyamqp",
                                     userid=self.identity.username if self.enable_authentication else None,
                                     password=self.identity.password if self.enable_authentication else None,
                                     ssl=ssl_details if self.tls_conf else False,
                                     connect_timeout=self.connection_timeout_sec)

        # Return connection object from acquire
        mocked_acquire.return_value = amqp_connection

        # Create Amqp client
        amqp_client = Amqp(self.url, self.port, self.identity, self.tls_conf,
                           self.enable_authentication, self.connection_timeout_sec)

        # Initialise the consumer connection
        amqp_client.consume(self.amqp_msg_attr)

        # Call disconnect consumer method
        amqp_client.disconnect_consumer()

        # Check release method call has been made
        mocked_connection_release.assert_called()

        # Check the stop method call has been made
        mocked_worker_stop.assert_called()


if __name__ == '__main__':
    unittest.main(verbosity=1)
