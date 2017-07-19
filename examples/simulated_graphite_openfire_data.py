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

import Queue
import random

from liota.device_comms.example_xmpp import get_data
from liota.dcc_comms.socket_comms import SocketDccComms
from liota.dccs.graphite import Graphite
from liota.entities.metrics.metric import Metric
from liota.entities.edge_systems.simulated_edge_system import SimulatedEdgeSystem

# getting values from conf file
# config = {}
# execfile('sampleProp.conf', config)

# Store temperature values in Queue
openfire_data = Queue.Queue()

fire_otpt = get_data()
print fire_otpt
openfire_data.put(str(fire_otpt))


# Extract data from Queue
def get_value(queue):
    return queue.get(block=True)


# ---------------------------------------------------------------------------
# In this example, we demonstrate how data for a simulated metric receives data
# from Openfire can be directed to graphite data center component using Liota.
# The program illustrates the ease of use Liota brings to IoT application
# developers.

if __name__ == '__main__':
    edge_system = SimulatedEdgeSystem('EdgeSystemName')

    # Sending data to Graphite data center component
    # Socket is the underlying transport used to connect to the Graphite
    # instance
    graphite = Graphite(SocketDccComms(ip='127.0.0.1',
                                       port=80))
    graphite_reg_edge_system = graphite.register(edge_system)

    metric_name = 'OpenfireData'
    openfire_metric = Metric(name=metric_name, interval=10,
                             sampling_function=lambda: get_value(openfire_data))
    reg_openfire_metric = graphite.register(openfire_metric)
    graphite.create_relationship(graphite_reg_edge_system, reg_openfire_metric)
    reg_openfire_metric.start_collecting()
