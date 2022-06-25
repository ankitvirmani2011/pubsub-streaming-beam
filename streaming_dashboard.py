!pip install datashader holoviews hvplot notebook numpy pandas panel param requests streamz
import operator as op
import numpy as np
import pandas as pd
import requests
import param
import panel as pn
import hvplot.pandas
import hvplot.streamz
import holoviews as hv
from holoviews.element.tiles import EsriImagery
from holoviews.selection import link_selections
from datashader.utils import lnglat_to_meters
from streamz.dataframe import PeriodicDataFrame

import os
import ast
import json
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
import time
import pandas as pd

# GCP topic, project & subscription ids

####Please enter values for your the below for your project and uncommment
#PUB_SUB_TOPIC = "pubsub_sink"
#PUB_SUB_PROJECT = "snoflake-dlp-test"
#PUB_SUB_SUBSCRIPTION = "pubsub_sink-sub"

#######################################################
# Pub/Sub consumer timeout
timeout = 5.0

# callback function for processing consumed payloads 
# prints recieved payload
def process_payload(message):
    payload_str=message.data.decode("UTF-8")
    payload_dict=eval(payload_str)
    #print(type(payload_dict))
    time.sleep(5)
    print(payload_dict) 
    message.ack()

'''
# producer function to push a message to a topic
def push_payload(payload, topic, project):        
        publisher = pubsub_v1.PublisherClient() 
        topic_path = publisher.topic_path(project, topic)        
        data = json.dumps(payload).encode("utf-8")           
        future = publisher.publish(topic_path, data=data)
        print("Pushed message to topic.")   
'''

# consumer function to consume messages from a topics for a given timeout period
def consume_payload(project, subscription, callback, period):
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project, subscription)
        #print(f"Listening for messages on {subscription_path}..\n")
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.                
                streaming_pull_future.result(timeout=period)
            except TimeoutError:
                streaming_pull_future.cancel()

# loop to test producer and consumer functions with a 3 second delay
while(True):    
    print("===================================")
    #payload = {"data" : "Payload data", "timestamp": time.time()}
    #print(f"Sending payload: {payload}.")
    #push_payload(payload, PUB_SUB_TOPIC, PUB_SUB_PROJECT)
    consume_payload(PUB_SUB_PROJECT, PUB_SUB_SUBSCRIPTION, process_payload, timeout)
