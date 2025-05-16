import collections
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import multiprocessing
from multiprocessing import Pool
import time
import signal
import traceback
import sys

from kafka import KafkaProducer
from kafka.errors import KafkaError

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter

# 设置超时处理，防止进程卡死
def timeout_handler(signum, frame):
    raise TimeoutError("Processing timed out")

# 全局函数，用于多线程调用而不是多进程
def process_items_batch(batch_data):
    connection_url, topic_prefix, topic_mapping, items, batch_id = batch_data
    
    # 设置线程级别超时保护 - 实际上在线程中SIGALRM不起作用，但保留代码结构
    try:
        processed_count = 0
        error_count = 0
        start_time = datetime.now()
        tid = batch_id  # 在线程模式下使用batch_id作为标识
        logging.info(f"Worker {tid} started at {start_time} with {len(items)} items")
        
        # 创建独立的producer - 但使用更保守的配置
        producer = KafkaProducer(
            bootstrap_servers=connection_url,
            max_block_ms=30000,       # 最大阻塞30秒
            request_timeout_ms=20000,  # 请求超时20秒
            max_in_flight_requests_per_connection=5,
            connections_max_idle_ms=60000,  # 空闲连接在60秒后关闭
            reconnect_backoff_ms=1000,
            reconnect_backoff_max_ms=10000
        )
        
        # 进度报告频率
        report_interval = max(1, len(items) // 10)  # 10%的项目报告一次进度
        last_report_time = start_time
        
        for i, item in enumerate(items):
            if i > 0 and i % report_interval == 0:
                now = datetime.now()
                elapsed = (now - last_report_time).total_seconds()
                progress_pct = (i / len(items)) * 100
                items_per_second = report_interval / elapsed if elapsed > 0 else 0
                logging.info(f"Worker {tid} progress: {progress_pct:.1f}% ({i}/{len(items)}) - {items_per_second:.1f} items/sec")
                last_report_time = now
            
            try:
                item_type = item.get('type')
                if item_type is not None and item_type in topic_mapping:
                    data = json.dumps(item).encode('utf-8')
                    producer.send(topic_prefix + topic_mapping[item_type], value=data)
                    processed_count += 1
                    # 定期刷新避免积压太多消息
                    if processed_count % 100 == 0:
                        producer.flush(timeout=5)
                else:
                    logging.warning(f'Topic for item type "{item_type}" is not configured.')
            except Exception as e:
                error_count += 1
                logging.error(f"Worker {tid} error processing item {i}: {str(e)}")
                if error_count >= 100:  # 如果错误太多，提前退出
                    logging.error(f"Worker {tid} too many errors, aborting batch")
                    break
        
        # 确保所有消息发送，但添加超时
        try:
            producer.flush(timeout=30)  # 30秒超时
        except Exception as e:
            logging.error(f"Worker {tid} error during flush: {str(e)}")
        
        # 关闭producer
        producer.close(timeout=10)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Worker {tid} completed {processed_count} items with {error_count} errors in {duration:.1f} seconds ({processed_count/duration if duration > 0 else 0:.1f} items/sec)")
        
        return processed_count
    except Exception as e:
        logging.error(f"Worker {batch_id} unexpected error: {str(e)}")
        logging.error(traceback.format_exc())
        return 0


class KafkaItemExporter:

    def __init__(self, output, item_type_to_topic_mapping, converters=()):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.connection_url = self.get_connection_url(output)
        self.topic_prefix = self.get_topic_prefix(output)
        print(self.connection_url, self.topic_prefix)
        print(self.connection_url)
        # 使用更保守的Kafka配置
        self.producer = KafkaProducer(
            bootstrap_servers=self.connection_url,
            max_block_ms=30000,
            max_in_flight_requests_per_connection=5,
            connections_max_idle_ms=60000,
            reconnect_backoff_ms=1000
        )

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
        logging.info(f"Exporting {total_items} items, {datetime.now()}")
        
        # 使用更保守的线程数，而不是进程
        thread_count = 4
        
        if total_items == 0:
            logging.warning("No items to export")
            return
            
        # 根据线程数平均分配消息
        chunk_size = max(1, total_items // thread_count)
        logging.info(f"Using {thread_count} workers with ~{chunk_size} items per worker")
        
        # 准备批次数据
        batches = []
        for i in range(0, total_items, chunk_size):
            batch_id = len(batches) + 1
            end_idx = min(i + chunk_size, total_items)
            batch_items = items[i:end_idx]
            batches.append((
                self.connection_url,
                self.topic_prefix,
                self.item_type_to_topic_mapping,
                batch_items,
                batch_id
            ))
        
        # 使用线程池而不是进程池
        start_time = datetime.now()
        total_processed = 0
        
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            # 提交所有批次任务
            future_to_batch = {executor.submit(process_items_batch, batch): i for i, batch in enumerate(batches)}
            
            # 收集结果
            for future in future_to_batch:
                try:
                    result = future.result(timeout=600)  # 10分钟超时
                    total_processed += result
                except Exception as e:
                    logging.error(f"Error in thread processing: {str(e)}")
        
        end_time = datetime.now()
        duration = end_time - start_time
        seconds = duration.total_seconds()
        
        # 输出结果统计
        logging.info(f"Export completed: {total_processed}/{total_items} items in {seconds:.1f} seconds")
        if seconds > 0:
            logging.info(f"Average speed: {total_processed/seconds:.1f} items/second")

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
