import logging
import time
from datetime import datetime

import apache_beam as beam
from apache_beam import Create, FlatMap, Map, ParDo, Filter, Flatten, Partition
from apache_beam import Keys, Values, GroupByKey, CoGroupByKey, CombineGlobally, CombinePerKey
from apache_beam import pvalue, window, WindowInto
from apache_beam.transforms.combiners import Top, Mean, Count, MeanCombineFn

from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib

import logging
from utils.solutions import solutions

import apache_beam as beam
from apache_beam import  Map
from apache_beam.transforms.combiners import Count
from apache_beam.io.textio import ReadFromText, WriteToText

from apache_beam.testing.util import assert_that
from apache_beam.testing.util import matches_all, equal_to

from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib

import logging
import json
import time
import traceback

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options import pipeline_options
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io import WriteToText

from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
#import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.runners import DataflowRunner

import google.auth

#from utils.utils import publish_to_topic
from IPython.core.display import display, HTML


###Writing from one topic to another as is- working!
########################################################
########################################################

import apache_beam as beam

TOPIC_PATH = "projects/pubsub-public-data/topics/taxirides-realtime"   ###please change this to your input topic
OUTPUT_PATH = "projects/snoflake-dlp-test/topics/paccar_sink"    ### please change this to your outpute receiver topic


class ParseInputTopic(beam.DoFn):
    def process(self, element):
        element_json=json.loads(element)
        vehicle={}
        vehicle['vin']=element_json['ride_id']
        vehicle['latitude']=element_json['latitude']
        vehicle['longitude']=element_json['longitude']
        vehicle['timestamp']=element_json['timestamp']
        vehicle['speed']=element_json['meter_reading']
        speeding_vehicles={}
        
        if float(vehicle['speed'])<50:
            key = 'speed under control'
            value={}
            value['vin']=vehicle['vin']                              
            value['speed']=vehicle['speed']
            value['timestamp']=vehicle['timestamp']
            value['latitude']=vehicle['latitude']
            value['longitude']=vehicle['longitude']
            speeding_vehicles[key]=value
            speeding_vehicles_encoded= str(speeding_vehicles).encode('utf-8')
            #print(type(speeding_vehicles_encoded))
            return [speeding_vehicles_encoded]
        
        elif float(vehicle['speed'])>=50 and float(vehicle['speed'])<60:
            key = 'speed between 50 and 60'
            value={}
            value['vin']=vehicle['vin']                              
            value['speed']=vehicle['speed']
            value['timestamp']=vehicle['timestamp']
            value['latitude']=vehicle['latitude']
            value['longitude']=vehicle['longitude']
            speeding_vehicles[key]=value
            speeding_vehicles_encoded= str(speeding_vehicles).encode('utf-8')
            #print(type(speeding_vehicles_encoded))
            return [speeding_vehicles_encoded]
                                             
        elif float(vehicle['speed'])>=60 and float(vehicle['speed'])<70:
            key = 'speed between 60 and 70'
            value={}
            value['vin']=vehicle['vin']                              
            value['speed']=vehicle['speed']
            value['timestamp']=vehicle['timestamp']
            value['latitude']=vehicle['latitude']
            value['longitude']=vehicle['longitude']
            speeding_vehicles[key]=value
            speeding_vehicles_encoded= str(speeding_vehicles).encode('utf-8')
            #print(type(speeding_vehicles_encoded))
            return [speeding_vehicles_encoded]
                                               
        elif float(vehicle['speed'])>=70 and float(vehicle['speed'])<80:
            key = 'speed between 70 and 80'
            value={}
            value['vin']=vehicle['vin']                              
            value['speed']=vehicle['speed']
            value['timestamp']=vehicle['timestamp']
            value['latitude']=vehicle['latitude']
            value['longitude']=vehicle['longitude']
            speeding_vehicles[key]=value
            speeding_vehicles_encoded= str(speeding_vehicles).encode('utf-8')
            #print(type(speeding_vehicles_encoded))
            return [speeding_vehicles_encoded]
                                               
        elif float(vehicle['speed'])>=80 and float(vehicle['speed'])<90:
            key = 'speed between 80 and 90'
            value={}
            value['vin']=vehicle['vin']                              
            value['speed']=vehicle['speed']
            value['timestamp']=vehicle['timestamp']
            value['latitude']=vehicle['latitude']
            value['longitude']=vehicle['longitude']
            speeding_vehicles[key]=value
            speeding_vehicles_encoded= str(speeding_vehicles).encode('utf-8')
            #print(type(speeding_vehicles_encoded))
            return [speeding_vehicles_encoded]
                                               
        elif float(vehicle['speed'])>=90 and float(vehicle['speed'])<100:
            key = 'speed between 90 and 100'
            value={}
            value['vin']=vehicle['vin']                              
            value['speed']=vehicle['speed']
            value['timestamp']=vehicle['timestamp']
            value['latitude']=vehicle['latitude']
            value['longitude']=vehicle['longitude']
            speeding_vehicles[key]=value
            speeding_vehicles_encoded= str(speeding_vehicles).encode('utf-8')
            #print(type(speeding_vehicles_encoded))
            return [speeding_vehicles_encoded]
                                               
        else:
            key = 'highly overspeeding- over 100'
            value={}
            value['vin']=vehicle['vin']                              
            value['speed']=vehicle['speed']
            value['timestamp']=vehicle['timestamp']
            value['latitude']=vehicle['latitude']
            value['longitude']=vehicle['longitude']
            speeding_vehicles[key]=value
            speeding_vehicles_encoded= str(speeding_vehicles).encode('utf-8')
            #print(type(speeding_vehicles_encoded))
            return [speeding_vehicles_encoded]           
################################################################
################################################################
################################################################

        
def date2unix(string):
    unix = int(time.mktime(datetime.strptime(string, "%Y-%m-%d %H:%M").timetuple()))
    return unix

        
################################################################
        

def run():

    options = pipeline_options.PipelineOptions(
    streaming=True,
    project='snoflake-dlp-test'   ###Please change this to your project!
    )
    p = beam.Pipeline(options=options)

    #print("I reached here")
    # # Read from PubSub into a PCollection.
    data_without_windows = p | beam.io.ReadFromPubSub(topic=TOPIC_PATH) | beam.ParDo(ParseInputTopic()) | beam.io.WriteToPubSub(topic=OUTPUT_PATH)
    
    #| Map(lambda x: window.TimestampedValue(x, date2unix(x["timestamp"]))) | beam.WindowInto(window.FixedWindows(5)) | GroupByKey()
                
    # Don't forget to run the pipeline!
    result = p.run()
    result.wait_until_finish()

run()
