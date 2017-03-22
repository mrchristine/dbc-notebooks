import sys, json, argparse
from pubnub.pnconfiguration import PNConfiguration
from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNOperationType, PNStatusCategory
from pubnub.pubnub import PubNub
from kafka import KafkaConsumer, KafkaProducer

# v4 of the API docs, which are different from v3 of the api found online
# API Docs: https://www.pubnub.com/docs/python/api-reference

#PUBNUB_PUBLISH_KEY = os.environ['PUBNUB_PUBLISH_KEY']
#PUBNUB_SUBSCRIBE_KEY = os.environ['PUBNUB_SUBSCRIBE_KEY']
#PUBNUB_SECRET_KEY = os.environ['PUBNUB_SECRET_KEY']
#pnconf.publish_key = PUBNUB_PUBLISH_KEY
#pnconf.secret_key = PUBNUB_SECRET_KEY

# Channel definitions https://www.pubnub.com/developers/realtime-data-streams/
channels = {}
channels['twitter'] = {'sub_key': 'sub-c-78806dd4-42a6-11e4-aed8-02ee2ddab7fe',
                       'channel': 'pubnub-twitter'}
channels['web_traffic'] = {'sub_key': 'e19f2bb0-623a-11df-98a1-fbd39d75aa3f',
                           'channel': 'rts-xNjiKP4Bg4jgElhhn9v9-geo-map'}
channels['hackernews'] = {'sub_key': 'sub-c-c00db4fc-a1e7-11e6-8bfd-0619f8945a4f',
                          'channel': 'hacker-news'}
channels['sensors'] = {'sub_key': 'sub-c-5f1b7c8e-fbee-11e3-aa40-02ee2ddab7fe',
                          'channel': 'pubnub-sensor-network'}
channels['market_orders'] = {'sub_key': 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe',
                             'channel': 'pubnub-market-orders'}

class MySubscribeCallback(SubscribeCallback):
    def status(self, pubnub, status):
        # The status object returned is always related to subscribe but could contain
        # information about subscribe, heartbeat, or errors
        # use the operationType to switch on different options
        if status.operation == PNOperationType.PNSubscribeOperation \
            or status.operation == PNOperationType.PNUnsubscribeOperation:
            if status.category == PNStatusCategory.PNConnectedCategory:
                pass
                # This is expected for a subscribe, this means there is no error or issue whatsoever
            elif status.category == PNStatusCategory.PNReconnectedCategory:
                pass
                # This usually occurs if subscribe temporarily fails but reconnects. This means
                # there was an error but there is no longer any issue
            elif status.category == PNStatusCategory.PNDisconnectedCategory:
                pass
                # This is the expected category for an unsubscribe. This means there
                # was no error in unsubscribing from everything
            elif status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
                pass
                # This is usually an issue with the internet connection, this is an error, handle
                # appropriately retry will be called automatically
            elif status.category == PNStatusCategory.PNAccessDeniedCategory:
                pass
                # This means that PAM does allow this client to subscribe to this
                # channel and channel group configuration. This is another explicit error
            else:
                pass
                # This is usually an issue with the internet connection, this is an error, handle appropriately
                # retry will be called automatically
        elif status.operation == PNOperationType.PNSubscribeOperation:
            # Heartbeat operations can in fact have errors, so it is important to check first for an error.
            # For more information on how to configure heartbeat notifications through the status
            # PNObjectEventListener callback, consult <link to the PNCONFIGURATION heartbeart config>
            if status.is_error():
                pass
                # There was an error with the heartbeat operation, handle here
            else:
                pass
                # Heartbeat operation was successful
        else:
            pass
            # Encountered unknown status type

    def presence(self, pubnub, presence):
        pass  # handle incoming presence data

    def message(self, pubnub, msg):
        if msg.message:
            event_bytes = json.dumps(msg.message).encode('utf-8')
        if push_to_kafka:
            producer.send(my_topic, event_bytes)
        if PRINT_TERM:
            print("\nChannel {0}:\n{1}".format(msg.channel, event_bytes))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="""
    Kafka producer reading records from PubNub public streaming data sources.
    """)
    parser.add_argument('channel_number', type=int,
                        help='Channel number from 1-5. \
                            1: web_traffic\n2: twitter\n3: hackernews\n4: sensors\n5: market_orders\n"')
    parser.add_argument('--kafka', dest='kafka',
                        help='List of kafka servers as comma separate host:port, e.g. host1:9092,host2:9092')
    parser.add_argument('--debug', dest='debug', action='store_true')

    args = parser.parse_args()
    STREAM = args.channel_number
    print(STREAM)
    if STREAM > 5 or STREAM < 1:
        raise ValueError('Required numeric value as arg to define stream content. Pick 1-5')
    if STREAM == 1:
        stream_info = channels['web_traffic']
        my_topic = 'web_traffic'
    elif STREAM == 2:
        stream_info = channels['twitter']
        my_topic = "twitter"
    elif STREAM == 3:
        stream_info = channels['hackernews']
        my_topic = "hackernews"
    elif STREAM == 4:
        stream_info = channels['sensors']
        my_topic = "sensors"
    elif STREAM == 5:
        stream_info = channels['market_orders']
        my_topic = "market_orders"
    else:
        raise ValueError('Stream not specified within 1-5 range')

    push_to_kafka = False
    try:
        kafka_hosts = args.kafka
        print("Writing to kafka ...")
        print(my_topic)
        producer = KafkaProducer(bootstrap_servers=kafka_hosts)
        push_to_kafka = True
        if args.debug:
            PRINT_TERM = True
        else:
            PRINT_TERM = False
    except:
        print("Printing to console. Will not write to kafka.")
        PRINT_TERM = True

    if PRINT_TERM:
        print("Print pubnub events to terminal.")
    else:
        print("Will not print events to terminal.")

    # bootstrap the config object
    pnconf = PNConfiguration()
    PUBNUB_SUBSCRIBE_KEY = stream_info['sub_key']
    CHANNEL = stream_info['channel']

    pnconf.subscribe_key = PUBNUB_SUBSCRIBE_KEY
    pnconf.ssl = False
    # create the pub / sub client
    pubnub = PubNub(pnconf)

    pubnub.add_listener(MySubscribeCallback())
    pubnub.subscribe().channels(CHANNEL).execute()
