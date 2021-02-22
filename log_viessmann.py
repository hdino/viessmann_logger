#!/usr/bin/env python3

import simplejson as json
from simplejson import JSONDecodeError

from datetime import datetime
import time

from influxdb_client import WritePrecision, InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from PyViCare.PyViCare.PyViCareService import ViCareService

influxdb_token = "YOUR_INFLUX_DB_TOKEN"
influxdb_org = "USERNAME"
influxdb_bucket = "Heizung"

def getResponseFromFile(filepath):
    with open(filepath) as f:
        return json.load(f)

def dumpResponse(filepath, response):
    with open(filepath, 'w') as f:
        json.dump(response, f)

vicare = ViCareService('VIESSMANN_USERNAME', 'VIESSMANN_PASSWORD')


def getMeasurements():
    #api_response = getResponseFromFile('temp.json')
    api_response = vicare.getProperty('')
    
    if 'entities' not in api_response:
        print('Entry "entities" in API response missing. Dumping it...')
        now = datetime.now()
        log_filename = now.strftime("%Y-%m-%d_%H%M%S.%f.log")
        dumpResponse(log_filename, api_response)
        raise KeyError()
    
    entities = api_response['entities']
    
    response_dict = {}
    
    for entity in entities:
        if entity['class'][1] != 'feature':
            # log error
            continue
        
        if not entity['properties']: # and not entity['actions']
            continue
        
        #print(entity['class'][0])
        entity_path = entity['class'][0].split('.')
        response_sub_dict = response_dict
        for entity_path_element in entity_path:
            if entity_path_element not in response_sub_dict:
                response_sub_dict[entity_path_element] = {}
            response_sub_dict = response_sub_dict[entity_path_element]
        
        for p in entity['properties']:
            #print('   ', p)
            
            p_type = entity['properties'][p]['type']
            p_value = entity['properties'][p]['value']
            response_sub_dict[p] = p_value
    
    return response_dict

def getWriteData(response_dict):
    heating = response_dict['heating']
    circuit = heating['circuits']['0']
    dhw = heating['dhw']
    sensors = heating['sensors']
    power = heating['power']['consumption']
    burner = heating['burner']
    gas = heating['gas']['consumption']
    
    write_data = 'heater' + \
        ' circuit_active_program="' + circuit['operating']['programs']['active']['value'] + '"' + \
        ',circuit_active_mode="' + circuit['operating']['modes']['active']['value'] + '"' + \
        ',circuit_supply_temperature=' + str(circuit['sensors']['temperature']['supply']['value']) + \
        ',circuit_curve_shift=' + str(circuit['heating']['curve']['shift']) + \
        ',circuit_curve_slope=' + str(circuit['heating']['curve']['slope']) + \
        ',circuit_circulation_pump="' + circuit['circulation']['pump']['status'] + '"' + \
        ',dhw_circulation_pump="' + dhw['pumps']['circulation']['status'] + '"' + \
        ',dhw_active=' + str(dhw['active']) + \
        ',dhw_one_time_charge=' + str(dhw['oneTimeCharge']['active']) + \
        ',dhw_hot_water_storage_temperature=' + str(dhw['sensors']['temperature']['hotWaterStorage']['value']) + \
        ',dhw_temperature=' + str(dhw['temperature']['value']) + \
        ',dhw_temperature_main=' + str(dhw['temperature']['main']['value']) + \
        ',outside_temperature=' + str(sensors['temperature']['outside']['value']) + \
        ',supply_pressure=' + str(sensors['pressure']['supply']['value']) + \
        ',volumetric_flow=' + str(sensors['volumetricFlow']['return']['value']) + \
        ',power_consumption_total_year=' + str(round(power['total']['year'][0], 1)) + \
        ',power_consumption_heating_year=' + str(round(power['heating']['year'][0], 1)) + \
        ',power_consumption_dhw_year=' + str(round(power['dhw']['year'][0], 1)) + \
        ',burner_modulation=' + str(burner['modulation']['value']) + \
        ',burner_hours=' + str(burner['statistics']['hours']) + \
        ',burner_starts=' + str(burner['statistics']['starts']) + 'u' + \
        ',burner_active=' + str(burner['active']) + \
        ',gas_consumption_total_year=' + str(round(gas['total']['year'][0], 1)) + \
        ',gas_consumption_heating_year=' + str(round(gas['heating']['year'][0], 1)) + \
        ',gas_consumption_dhw_year=' + str(round(gas['dhw']['year'][0], 1)) + \
        ',flue_temperature=' + str(heating['flue']['sensors']['temperature']['main']['value']) + \
        ',heat_production_year=' + str(round(heating['heat']['production']['year'][0], 1)) + \
        ',boiler_temperature=' + str(heating['boiler']['temperature']['value']) + \
        ',boiler_common_supply_temperature=' + str(heating['boiler']['sensors']['temperature']['commonSupply']['value']) + \
        ',errors_active_new_count=' + str(len(heating['errors']['active']['entries']['new'])) + \
        ',errors_active_current_count=' + str(len(heating['errors']['active']['entries']['current'])) + \
        ',errors_active_gone_count=' + str(len(heating['errors']['active']['entries']['gone'])) + \
        ',errors_history_new_count=' + str(len(heating['errors']['history']['entries']['new'])) + \
        ',errors_history_current_count=' + str(len(heating['errors']['history']['entries']['current'])) + \
        ',errors_history_gone_count=' + str(len(heating['errors']['history']['entries']['gone']))
    
    return write_data

#dumpResponse('temp.json', api_response)

influxdb_client = InfluxDBClient(url="http://localhost:8086", token=influxdb_token)
influxdb_write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)

counter = 0
current_mode = 0 # 0 = normal, 1 = heating dhw, 2 = heating dhw done, but still waiting
t_stop_waiting_for_dhw = 0.0
sleep_time = 120
while True:
    counter += 1
    
    try:
        measurement_dict = getMeasurements()
        write_data = getWriteData(measurement_dict)
        influxdb_write_api.write(influxdb_bucket, influxdb_org, write_data)
        
        volumetric_flow = measurement_dict['heating']['sensors']['volumetricFlow']['return']['value']
        
        if current_mode == 0 and volumetric_flow >= 600:
            print('Starting heating DHW', datetime.now())
            current_mode = 1
            sleep_time = 60
        
        if current_mode == 1 and volumetric_flow < 600:
            print('Heating DHW done, waiting 600 s ...', datetime.now())
            current_mode = 2
            t_stop_waiting_for_dhw = time.monotonic() + 600
            sleep_time = 60
        
        if current_mode == 2 and time.monotonic() >= t_stop_waiting_for_dhw:
            print('Waiting done', datetime.now())
            current_mode = 0
            sleep_time = 120
        
    except KeyError:
        print('A key error occured')
    except JSONDecodeError:
        print('A JSON decode error occured')
    
    if counter >= 30:
        print('Alive at', datetime.now())
        counter = 0
    
    time.sleep(sleep_time)
