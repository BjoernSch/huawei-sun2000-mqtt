import asyncio
import logging
import os
import sys
import time
from datetime import datetime

from huawei_solar import HuaweiSolarBridge
from huawei_solar import register_names as rn
from dotenv import load_dotenv
import paho.mqtt.publish as mqtt_publish
import asyncio
import aiomqtt
from aioinflux import InfluxDBClient

class Huawei2MQTT():
    # Poll fast register in shorter intervals for load regulation
    registers_fast_primary = [
        rn.POWER_METER_ACTIVE_POWER,
        rn.GRID_ACCUMULATED_ENERGY,
        rn.ACTIVE_GRID_A_CURRENT,
        rn.ACTIVE_GRID_B_CURRENT,
        rn.ACTIVE_GRID_C_CURRENT,
        rn.STORAGE_UNIT_1_CHARGE_DISCHARGE_POWER,
        rn.STORAGE_UNIT_1_STATE_OF_CAPACITY,
        rn.STORAGE_UNIT_1_TOTAL_CHARGE,
        rn.STORAGE_UNIT_1_TOTAL_DISCHARGE,
        # rn.STORAGE_UNIT_2_CHARGE_DISCHARGE_POWER,
        # rn.STORAGE_UNIT_2_STATE_OF_CAPACITY,
        # rn.STORAGE_UNIT_2_TOTAL_CHARGE,
        # rn.STORAGE_UNIT_2_TOTAL_DISCHARGE,
    ]
    registers_fast_secondary = [
    ]
    registers_fast_common = [
        rn.INPUT_POWER,
        rn.ACTIVE_POWER,
        rn.ACCUMULATED_YIELD_ENERGY,
    ]
    # Poll other register in longer intervals for statistics
    registers_primary = [
        rn.STORAGE_UNIT_1_BUS_VOLTAGE,
        rn.STORAGE_UNIT_1_BUS_CURRENT,
        rn.STORAGE_UNIT_1_BATTERY_TEMPERATURE,
        rn.GRID_A_VOLTAGE,
        rn.GRID_B_VOLTAGE,
        rn.GRID_C_VOLTAGE,
        rn.ACTIVE_GRID_FREQUENCY,
        rn.GRID_EXPORTED_ENERGY,
        rn.ACTIVE_GRID_A_POWER,
        rn.ACTIVE_GRID_B_POWER,
        rn.ACTIVE_GRID_C_POWER,
        rn.STORAGE_RATED_CAPACITY,
        rn.STORAGE_BUS_VOLTAGE,
        rn.STORAGE_BUS_CURRENT,
        rn.STORAGE_UNIT_1_BATTERY_PACK_1_STATE_OF_CAPACITY,
        rn.STORAGE_UNIT_1_BATTERY_PACK_1_CHARGE_DISCHARGE_POWER,
        rn.STORAGE_UNIT_1_BATTERY_PACK_1_VOLTAGE,
        rn.STORAGE_UNIT_1_BATTERY_PACK_1_CURRENT,
        rn.STORAGE_UNIT_1_BATTERY_PACK_2_STATE_OF_CAPACITY,
        rn.STORAGE_UNIT_1_BATTERY_PACK_2_CHARGE_DISCHARGE_POWER,
        rn.STORAGE_UNIT_1_BATTERY_PACK_2_VOLTAGE,
        rn.STORAGE_UNIT_1_BATTERY_PACK_2_CURRENT
    ]
    registers_secondary = []
    registers_common = [
            rn.PV_01_VOLTAGE,
            rn.PV_01_CURRENT,
            rn.PV_02_VOLTAGE,
            rn.PV_02_CURRENT,
            rn.PHASE_A_CURRENT,
            rn.PHASE_B_CURRENT,
            rn.PHASE_C_CURRENT,
            rn.REACTIVE_POWER,
            rn.POWER_FACTOR,
            rn.GRID_FREQUENCY,
            rn.INTERNAL_TEMPERATURE,
            rn.TOTAL_DC_INPUT_POWER,
            rn.CUMULATIVE_DC_ENERGY_YIELD_MPPT1,
            rn.CUMULATIVE_DC_ENERGY_YIELD_MPPT2,
    ]
    
    def __init__(self) -> None:
        self.logger = logging.getLogger()
        self.mqtt_host = os.environ.get('HUAWEI_MODBUS_MQTT_BROKER')
        self.huawei_host = os.environ.get('HUAWEI_MODBUS_HOST')
        self.huawei_port = int(os.environ.get('HUAWEI_MODBUS_PORT', 502))
        self.primary_slave_id = int(os.environ.get('HUAWEI_MODBUS_DEVICE_ID_PRIMARY', 1))
        if os.environ.get('HUAWEI_MODBUS_DEVICE_ID_SECONDARY', None) != None:
            self.logger.info()
            self.secondary_slave_id = int(os.environ.get('HUAWEI_MODBUS_DEVICE_ID_SECONDARY'))
            self.logger.info()
        else:
            self.secondary_slave_id = None
        self.topic = os.environ.get('HUAWEI_MODBUS_MQTT_TOPIC')

        if os.getenv('INFLUXDB_HOST') != None:
            self.influx_host = os.getenv('INFLUXDB_HOST')
            self.influx_port = int(os.getenv('INFLUXDB_PORT', '8086')) 
            self.influx_username = os.getenv('INFLUXDB_USER', 'meter_reader')
            self.influx_password = os.getenv('INFLUXDB_PASSWORD')
            self.influx_database = os.getenv('INFLUXDB_DATABASE', 'meter_reader')
        else:
            self.influx_host = None

    async def create(self):
        self.primary_bridge = await HuaweiSolarBridge.create(
            self.huawei_host, self.huawei_port,
            slave_id=self.primary_slave_id,
        )
        if self.secondary_slave_id != None:
            self.secondary_bridge = await HuaweiSolarBridge.create_extra_slave(
                self.primary_bridge,
                slave_id=self.secondary_slave_id
        )


    async def mqtt_publish_data(self, data):
        async with aiomqtt.Client(self.mqtt_host) as client:
            for (key, value) in data.items():
                self.logger.debug(f'K: {key}, V: {value}')
                await client.publish(topic=key, payload=value)
        

    async def influx_publish_data(self, data, timestamp):
        async with InfluxDBClient(
                host=self.influx_host, port=self.influx_port,
                username=self.influx_username, password=self.influx_password,
                database=self.influx_database) as client:
            await client.write({
                'time': timestamp.isoformat(),
                'measurement': self.topic,
                'fields': {key.lstrip(self.topic + '/').replace('/','.'): value
                           for (key, value) in data.items()}
            })


    def transform_result(self, data, topic):
        return_data = {}
        for (key, value) in data.items():
            return_data[topic + '/' + key] = value.value

        for i in range(20):
            string_prefix = topic + '/' + f'pv_{i+1:02d}_'
            if string_prefix + 'voltage' in data and string_prefix + 'current' in data:
                return_data[string_prefix + 'power'] = self.calculate_power(
                    self.get_value(data.get(string_prefix + 'voltage')),
                    self.get_value(data.get(string_prefix + 'current')))
            else:
                break

        return return_data

    def calculate_power(self, voltage, current):
        if voltage is None or current is None:
            return None

        return voltage * current

    def get_value(self, record):
        if record is None:
            return None

        return record.value

    async def update(self, fast_update=False):
        timestamp = datetime.now()
        update_data = {}

        registers_primary = self.registers_fast_primary + self.registers_fast_common
        registers_secondary = self.registers_fast_secondary + self.registers_fast_common

        if not fast_update:
            registers_primary += self.registers_primary + self.registers_common
            registers_secondary += self.registers_secondary + self.registers_common

        if self.secondary_slave_id != None:
            self.logger.debug("Getting multiple inverter data")
            self.logger.debug("Retrieving from primary inverter")
            data = await self.primary_bridge.batch_update(registers_primary)
            update_data.update(self.transform_result(data, self.topic + '/primary'))
            self.logger.debug("Retrieving from secondary inverter")
            data = await self.secondary_bridge.batch_update(registers_secondary)
            update_data.update(self.transform_result(data, self.topic + '/secondary'))
        else:
            self.logger.debug("Getting single inverter data")
            data = await self.primary_bridge.batch_update(registers_primary)
            update_data.update(self.transform_result(data, self.topic))

        self.logger.debug("Sending data to MQTT")
        await self.mqtt_publish_data(update_data)
        
        if self.influx_host:
            self.logger.debug("Sending data to InfluxDB")
            await self.influx_publish_data(update_data, timestamp)
    
    async def run(self):
        await huawei.create()
        # Run in loop
        last_run = 0
        wait = 10
        full_interval = 6
        countdown = 0
        while True:
            if last_run > 0 and (time.time() - last_run < wait):
                sleep_time = max(2, int(wait - (time.time() - last_run)))
                logging.debug("Sleeping for %d seconds", sleep_time)
                await asyncio.sleep(sleep_time)
            last_run = time.time()

            if countdown == 0:
                self.logger.debug('Performing long update')
                countdown = full_interval
                fast_update = False
            else:
                self.logger.debug('Performing fast update')
                fast_update = True

            await self.update(fast_update)
            countdown -= 1


if __name__ == "__main__":
    # Load (default) settings from .env file
    load_dotenv()
    # Init logging
    loglevel = logging.INFO
    if os.environ.get('HUAWEI_MODBUS_DEBUG') == 'yes':
        loglevel = logging.DEBUG

    logger = logging.getLogger()
    logger.setLevel(loglevel)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    formatter.datefmt = '%Y-%m-%dT%H:%M:%S%z'

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    huawei = Huawei2MQTT()
    asyncio.run(huawei.run())

