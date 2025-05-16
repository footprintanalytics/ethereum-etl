import collections
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import multiprocessing
from multiprocessing import Pool

from kafka import KafkaProducer

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter

# 全局函数，用于多进程调用
def process_items_batch(batch_data):
    connection_url, topic_prefix, topic_mapping, items = batch_data
    
    # 在每个进程中创建独立的producer
    producer = KafkaProducer(bootstrap_servers=connection_url)
    
    try:
        processed_count = 0
        start_time = datetime.now()
        logging.info(f"Process {multiprocessing.current_process().name} started at {start_time} with {len(items)} items")
        
        for item in items:
            item_type = item.get('type')
            if item_type is not None and item_type in topic_mapping:
                data = json.dumps(item).encode('utf-8')
                producer.send(topic_prefix + topic_mapping[item_type], value=data)
                processed_count += 1
            else:
                logging.warning(f'Topic for item type "{item_type}" is not configured.')
        
        # 确保所有消息发送
        producer.flush()
        
        end_time = datetime.now()
        logging.info(f"Process {multiprocessing.current_process().name} completed {processed_count} items in {end_time - start_time}")
        return processed_count
    finally:
        producer.close()


class KafkaItemExporter:

    def __init__(self, output, item_type_to_topic_mapping, converters=()):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.connection_url = self.get_connection_url(output)
        self.topic_prefix = self.get_topic_prefix(output)
        print(self.connection_url, self.topic_prefix)
        print(self.connection_url)
        self.producer = KafkaProducer(bootstrap_servers=self.connection_url)

    def get_connection_url(self, output):
        try:
            return output.split('/')[1]
        except IndexError:
            raise Exception(
                'Invalid kafka output param, It should be in format of "kafka/127.0.0.1:9092" or "kafka/127.0.0.1:9092/<topic-prefix>"')

    def get_topic_prefix(self, output):
        try:
            return output.split('/')[2]
        except IndexError:
            return ''

    def open(self):
        pass

    def export_items(self, items):
        total_items = len(items)
        logging.info(f"Exporting {total_items} items using multiprocessing, {datetime.now()}")
        
        # 使用固定的4个进程，避免创建过多Kafka实例
        process_count = 4
        
        if total_items == 0:
            logging.warning("No items to export")
            return
            
        # 根据固定进程数平均分配消息
        chunk_size = max(1, total_items // process_count)  # 确保至少为1
        logging.info(f"Using {process_count} processes with ~{chunk_size} items per process")
        
        # 准备批次数据 - 将所有消息平均分配到各个进程
        batches = []
        for i in range(0, total_items, chunk_size):
            end_idx = min(i + chunk_size, total_items)
            batch_items = items[i:end_idx]
            # 参数顺序: connection_url, topic_prefix, topic_mapping, items
            batches.append((
                self.connection_url,
                self.topic_prefix,
                self.item_type_to_topic_mapping,
                batch_items
            ))
        
        # 使用进程池处理批次
        start_time = datetime.now()
        total_processed = 0
        
        with Pool(processes=min(process_count, len(batches))) as pool:
            results = pool.map(process_items_batch, batches)
            total_processed = sum(results)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # 输出结果统计
        logging.info(f"Multiprocessing export completed: {total_processed}/{total_items} items in {duration}")
        logging.info(f"Average speed: {total_processed/duration.total_seconds() if duration.total_seconds() > 0 else 0:.2f} items/second")

    def export_item(self, item):
        item_type = item.get('type')
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            data = json.dumps(item).encode('utf-8')
            logging.debug(data)
            return self.producer.send(self.topic_prefix + self.item_type_to_topic_mapping[item_type], value=data)
        else:
            logging.warning('Topic for item type "{}" is not configured.'.format(item_type))

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        pass


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result
