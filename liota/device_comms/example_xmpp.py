# to subscribe to node
# to collect data from openfire server
# Added handling event for continous receiving of data.

from xmpp_device_comms import XmppDeviceComms


# getting values from conf file
# config = {}
# execfile('samplePropXmpp.conf', config)


def subscribe_data():
    # xmpp_conn = XmppDeviceComms(jid=config['JID'], password=config['PASS'], host=config['host'], port=config['port'])
    # xmpp_conn.create_node(server=config['SERVER'], node=config['NODE'])
    # xmpp_conn.publish(server=config['SERVER'], node=config['NODE'], data="hello world")
    xmpp_conn = XmppDeviceComms("alice@127.0.0.1/sub", "m1ndst1x", "127.0.0.1", "5222")
    xmpp_conn.subscribe("pubsub.127.0.0.1/sub", "/vmware3")

# not the exact needed implementation
# def get_data():
#     # xmpp_conn = XmppDeviceComms(jid=config['JID'], password=config['PASS'], host=config['host'], port=config['port'])
#     # xmpp_conn.create_node(server=config['SERVER'], node=config['NODE'])
#     # xmpp_conn.publish(server=config['SERVER'], node=config['NODE'], data="hello world")
#     xmpp_conn = XmppDeviceComms("bob@127.0.0.1/sub", "m1ndst1x", "127.0.0.1", "5222")
#     print xmpp_conn
#     xmpp_conn.getdata("pubsub.127.0.0.1/sub", "/vmware3")


if __name__ == "__main__":

    try:
        subscribe_data()
        #get_data()
    except:
        print "failed"
