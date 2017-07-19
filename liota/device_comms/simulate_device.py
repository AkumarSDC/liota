import random
import time
from liota.device_comms.xmpp_device_comms import XmppDeviceComms

# getting values from conf file
#config = {}
#execfile('samplePropXmpp.conf', config)

# class varstatic:
#     a=0
#     x = int(a)

# Random number generator, simulating random metric readings.

def simulated_event_device():
    xmpp_conn = XmppDeviceComms("bob@127.0.0.1", "m1ndst1x", "127.0.0.1", "5222")
    xmpp_conn.create_node("pubsub.127.0.0.1", "/vmware11")
    while True:
        time.sleep(random.randint(1,10))
        random_no = random.randint(1,300)
        xmpp_conn.publish("pubsub.127.0.0.1", "/vmware11", "hello"+str(random_no))

# def publish_data(vary):
#     #xmpp_conn = XmppDeviceComms(jid=config['JID'], password=config['PASS'], host=config['host'], port=config['port'])
#     #xmpp_conn.create_node(server=config['SERVER'], node=config['NODE'])
#     #xmpp_conn.publish(server=config['SERVER'], node=config['NODE'], data="hello world")
#
#     xmpp_conn = XmppDeviceComms("bob@127.0.0.1", "m1ndst1x", "127.0.0.1", "5222")
#     #xmpp_conn.create_node("pubsub.127.0.0.1", "/vmware5")
#     xmpp_conn.publish("pubsub.127.0.0.1", "/vmware1", vary)
#
# if __name__ == "__main__":
#
#     y = varstatic()
#     z = y.x + 1
#
#     try:
#         while y<10:
#             publish_data(z)
#     except:
#         print "failed"

if __name__ == "__main__":
    try:
        simulated_event_device()
    except:
        print "failed"
