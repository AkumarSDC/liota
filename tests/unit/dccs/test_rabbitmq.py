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
import pint

from liota.dcc_comms.amqp_dcc_comms import *
from liota.dccs.dcc import DataCenterComponent
from liota.dccs.rabbitmq import RabbitMQ
from liota.entities.edge_systems.dell5k_edge_system import Dell5KEdgeSystem
from liota.entities.metrics.metric import Metric
from liota.entities.metrics.registered_metric import RegisteredMetric
from liota.entities.registered_entity import RegisteredEntity

# Create a pint unit registry
ureg = pint.UnitRegistry()


# Sample function
def sampling_function():
    pass


# Callback function
def test_callback():
    pass


class RabbitMQTest(unittest.TestCase):
    """
    RabbitMQ unit test cases
    """

    def setUp(self):
        """
        Method to initialise the RabbitMQ parameters.
        :return: None
        """

        # EdgeSystem name
        self.edge_system = Dell5KEdgeSystem("TestEdgeSystem")

        # Mocking the DataCenterComponent init method
        with mock.patch("liota.dccs.rabbitmq.DataCenterComponent.__init__") as mocked_dcc:
            self.rabbitmq_client = RabbitMQ("Mocked connection object", enclose_metadata=False)

    def tearDown(self):
        """
        Method to cleanup the resource created during the execution of test case.
        :return: None
        """
        self.edge_system = None
        self.rabbitmq_client = None

    def test_implementation(self):
        """
        Test the implementation of init method
        :return: None 
        """

        # Check DataCenterComponent is base class of RabbitMQ
        self.assertTrue(issubclass(RabbitMQ, DataCenterComponent))

        # Check the enclosed_metadata
        self.assertFalse(self.rabbitmq_client.enclose_metadata)

    def test_validation_of_comms_parameter(self):
        """
        Test case to check the validation of RabbitMQ class for invalid connections object.
        :return: None
        """
        # Checking whether implementation raising the TypeError Exception for invalid comms object
        with self.assertRaises(TypeError):
            RabbitMQ("Invalid object")

    def test_implementation_register_entity(self):
        """
        Test case to check the implementation of register method of RabbitMQ class for entity registration.
        :return: None
        """

        # Register the edge
        registered_entity = self.rabbitmq_client.register(self.edge_system)

        # Check the returned object is of the class RegisteredEntity
        self.assertIsInstance(registered_entity, RegisteredEntity)

    def test_implementation_register_metric(self):
        """
        Test case to check the implementation of register method of RabbitMQ for metric registration.
        :return: None
        """

        # Creating test Metric
        test_metric = Metric(
            name="Test_Metric",
            unit=None,
            interval=10,
            aggregation_size=2,
            sampling_function=sampling_function
        )

        registered_metric = self.rabbitmq_client.register(test_metric)

        # Check the returned object is of the class RegisteredMetric
        self.assertIsInstance(registered_metric, RegisteredMetric)

    def test_implementation_create_relationship(self):
        """
        Test case to test RegisteredEntity as Parent and RegisteredMetric as child.
        RegisteredEdgeSystem->RegisteredMetric
        :return: None
        """

        # Register the edge
        registered_entity = self.rabbitmq_client.register(self.edge_system)

        #  Creating test Metric
        test_metric = Metric(
            name="Test_Metric",
            unit=None,
            interval=10,
            aggregation_size=2,
            sampling_function=sampling_function
        )

        registered_metric = self.rabbitmq_client.register(test_metric)

        # Creating the parent-child relationship
        self.rabbitmq_client.create_relationship(registered_entity, registered_metric)

        self.assertEqual(registered_metric.parent, registered_entity, "Check the implementation of create_relationship")

    def test_validation_create_relationship_metric_device(self):
        """
        Test case to test validation for RegisteredMetric as Parent and RegisteredEntity as child.
        RegisteredMetric->RegisteredMetric
        :return: None
        """

        # Register the edge
        registered_entity = self.rabbitmq_client.register(self.edge_system)

        #  Creating test Metric
        test_metric = Metric(
            name="Test_Metric",
            unit=None,
            interval=10,
            aggregation_size=2,
            sampling_function=sampling_function
        )

        registered_metric = self.rabbitmq_client.register(test_metric)

        with self.assertRaises(TypeError):
            # Test case to check validation for RegisteredMetric as Parent and RegisteredEntity as child.
            self.rabbitmq_client.create_relationship(registered_metric, registered_entity)

    def test_validation_create_relationship_child_entity(self):
        """
        Test case to check validation for RegisteredEntity as Parent and Child.
        RegisteredEdgeSystem->RegisteredEdgeSystem.
        :return: None
        """

        # Register the edge
        registered_entity = self.rabbitmq_client.register(self.edge_system)

        with self.assertRaises(TypeError):
            # Creating the parent-child relationship between Edge-System and Edge-System
            self.rabbitmq_client.create_relationship(registered_entity, registered_entity)

    @mock.patch("liota.lib.utilities.dcc_utility.get_formatted_data")
    def test_format_data(self, mocked_get_formatted_data=None):
        """
        Test the implementation of _format_data.
        :param mocked_get_formatted_data: Mocked get_formatted_data from dcc_utilities
        :return: None
        """

        # Assign return value to get_formatted_data
        mocked_get_formatted_data.return_value = None

        # Creating test Metric
        test_metric = Metric(
            name="Test_Metric",
            unit=None,
            interval=10,
            aggregation_size=2,
            sampling_function=sampling_function
        )

        # Mock the get_formatted_data
        with mock.patch("liota.dccs.rabbitmq.get_formatted_data") as mocked_get_formatted_data:

            registered_metric = self.rabbitmq_client.register(test_metric)

            # Call _format_data
            self.rabbitmq_client._format_data(reg_metric=registered_metric)

            # Check get_formatted_data called with following
            mocked_get_formatted_data.assert_called_with(registered_metric, False)

    def test_consume_implementation(self):
        """
        Test the consumer method implementation.        
        :return: None 
        """

        # Create Amap consumer messaging attributes
        amqp_consumer_msg_attr = AmqpConsumeMessagingAttributes(exchange_name="test_exchange",
                                                                routing_keys=["test"])

        # Create amqp client
        with mock.patch.object(AmqpDccComms, "__init__") as mocked_amqp_init, \
                mock.patch.object(AmqpDccComms, "receive") as mocked_amqp_consume:

            mocked_amqp_init.return_value = None

            # Create mocked AmqpDCCComms object
            amqp_client = AmqpDccComms()

            # Create RabbitMQ client object
            self.rabbitmq_client = RabbitMQ(amqp_client)

            # Call consume method
            self.rabbitmq_client.consume(amqp_consumer_msg_attr, test_callback)

            # Check mocked receive called with following params
            mocked_amqp_consume.assert_called_with(amqp_consumer_msg_attr, test_callback)

    def test_stop_consumers(self):
        """
        Test the stop_consumers implementation.
        :return: None
        """

        # Create amqp client
        with mock.patch.object(AmqpDccComms, "__init__") as mocked_amqp_init, \
                mock.patch.object(AmqpDccComms, "stop_receiving") as mocked_amqp_stop_receiving:
            mocked_amqp_init.return_value = None

            # Create mocked AmqpDCCComms object
            amqp_client = AmqpDccComms()

            # Create RabbitMQ client object
            self.rabbitmq_client = RabbitMQ(amqp_client)

            # Call stop_consumers
            self.rabbitmq_client.stop_consumers()

            # Call stop_receiving
            mocked_amqp_stop_receiving.assert_called()

    def test_set_properties(self):
        """
        Test the set_properties implementation.
        :return: None
        """
        # Check set_properties raising the NotImplementedError
        self.assertRaises(NotImplementedError, self.rabbitmq_client.set_properties, None, None)

    def test_unregister(self):
        """
        Test the unregister implementation.
        :return: None
        """
        # Check set_properties raising the NotImplementedError
        self.assertRaises(NotImplementedError, self.rabbitmq_client.unregister, None)

if __name__ == '__main__':
    unittest.main(verbosity=1)
