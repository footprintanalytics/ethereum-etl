import collections
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import multiprocessing
from multiprocessing import Pool, Manager
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

# 混合方案：每个进程内使用线程池处理批次
def process_worker(worker_data):
    connection_url, topic_prefix, topic_mapping, all_items, worker_id, num_threads = worker_data
    
    try:
        start_time = datetime.now()
        pid = multiprocessing.current_process().pid
        logging.info(f"Process {pid} (worker {worker_id}) started at {start_time} with {len(all_items)} items")
        
        # 在进程内创建一个共享的Kafka生产者
        producer = KafkaProducer(
            bootstrap_servers=connection_url,
            max_block_ms=30000,      
            request_timeout_ms=20000,
            max_in_flight_requests_per_connection=5,
            connections_max_idle_ms=60000,
            reconnect_backoff_ms=1000,
            batch_size=16384,         # 增加批处理大小
            linger_ms=10,             # 短暂等待以收集更多消息
            buffer_memory=33554432    # 32MB缓冲区
        )
        
        # 准备本进程内的线程数据
        batch_size = max(1, len(all_items) // num_threads)
        thread_batches = []
        
        for i in range(0, len(all_items), batch_size):
            thread_id = len(thread_batches) + 1
            end_idx = min(i + batch_size, len(all_items))
            thread_batches.append((
                all_items[i:end_idx],
                thread_id,
                worker_id,
                pid,
                producer,  # 共享同一个producer
                topic_prefix,
                topic_mapping
            ))
        
        # 在进程内使用线程池处理
        total_processed = 0
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_batch = {executor.submit(process_thread_batch, batch): i 
                              for i, batch in enumerate(thread_batches)}
            
            for future in future_to_batch:
                try:
                    result = future.result(timeout=300)  # 5分钟超时
                    total_processed += result
                except Exception as e:
                    logging.error(f"Error in thread processing: {str(e)}")
        
        # 确保所有消息发送
        try:
            producer.flush(timeout=60)
        except Exception as e:
            logging.error(f"Process {pid} (worker {worker_id}) error during final flush: {str(e)}")
        
        # 关闭producer
        producer.close(timeout=10)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Process {pid} (worker {worker_id}) completed {total_processed} items in {duration:.1f} seconds ({total_processed/duration if duration > 0 else 0:.1f} items/sec)")
        
        return total_processed
    except Exception as e:
        logging.error(f"Worker {worker_id} unexpected error: {str(e)}")
        logging.error(traceback.format_exc())
        return 0

# 线程处理函数，使用共享的Kafka生产者
def process_thread_batch(batch_data):
    items, thread_id, worker_id, pid, producer, topic_prefix, topic_mapping = batch_data
    
    try:
        processed_count = 0
        error_count = 0
        start_time = datetime.now()
        logging.info(f"Thread {thread_id} in process {pid} (worker {worker_id}) started with {len(items)} items")
        
        # 进度报告频率
        report_interval = max(1, len(items) // 5)  # 20%的项目报告一次进度
        last_report_time = start_time
        batch_data = []  # 用于批量处理
        
        for i, item in enumerate(items):
            # 进度报告
            if i > 0 and i % report_interval == 0:
                now = datetime.now()
                elapsed = (now - last_report_time).total_seconds()
                progress_pct = (i / len(items)) * 100
                items_per_second = report_interval / elapsed if elapsed > 0 else 0
                logging.info(f"Thread {thread_id} in process {pid} progress: {progress_pct:.1f}% - {items_per_second:.1f} items/sec")
                last_report_time = now
            
            try:
                item_type = item.get('type')
                if item_type is not None and item_type in topic_mapping:
                    data = json.dumps(item).encode('utf-8')
                    producer.send(topic_prefix + topic_mapping[item_type], value=data)
                    processed_count += 1
                    
                    # 批量处理: 每100条消息刷新一次
                    if processed_count % 100 == 0:
                        producer.flush(timeout=5)
                else:
                    logging.warning(f'Topic for item type "{item_type}" is not configured.')
            except Exception as e:
                error_count += 1
                if error_count <= 5:  # 只记录前几个错误，避免日志爆炸
                    logging.error(f"Error processing item {i}: {str(e)}")
                if error_count >= 100:
                    logging.error(f"Too many errors, aborting batch")
                    break
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Thread {thread_id} in process {pid} completed {processed_count} items with {error_count} errors in {duration:.1f} seconds")
        
        return processed_count
    except Exception as e:
        logging.error(f"Thread {thread_id} in process {pid} unexpected error: {str(e)}")
        return 0


class KafkaItemExporter:

    def __init__(self, output, item_type_to_topic_mapping, converters=()):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.connection_url = self.get_connection_url(output)
        self.topic_prefix = self.get_topic_prefix(output)
        print(self.connection_url, self.topic_prefix)
        print(self.connection_url)
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
        logging.info(f"Exporting {total_items} items with hybrid multiprocessing+multithreading approach, {datetime.now()}")
        
        if total_items == 0:
            logging.warning("No items to export")
            return
        
        # 配置混合并行策略
        cpu_count = multiprocessing.cpu_count()
        process_count = min(4, cpu_count)  # 最多4个进程，避免过多Kafka连接
        threads_per_process = 4  # 每个进程4个线程
        
        logging.info(f"Using {process_count} processes with {threads_per_process} threads each (total {process_count*threads_per_process} workers)")
        
        # 按进程数分割项目
        chunk_size = max(1, total_items // process_count)
        process_batches = []
        
        for i in range(0, total_items, chunk_size):
            worker_id = len(process_batches) + 1
            end_idx = min(i + chunk_size, total_items)
            process_batches.append((
                self.connection_url,
                self.topic_prefix,
                self.item_type_to_topic_mapping,
                items[i:end_idx],
                worker_id,
                threads_per_process
            ))
        
        # 使用进程池
        start_time = datetime.now()
        total_processed = 0
        
        # 使用进程上下文管理器确保安全
        try:
            ctx = multiprocessing.get_context('spawn')  # 使用spawn而不是fork，更安全
            with ctx.Pool(processes=len(process_batches)) as pool:
                results = pool.map_async(process_worker, process_batches)
                
                # 等待结果，避免永久阻塞
                while not results.ready():
                    logging.info(f"Waiting for {len(process_batches)} processes to complete...")
                    if results.wait(timeout=30):  # 每30秒检查一次
                        break
                
                # 获取结果
                processed_results = results.get(timeout=10)
                total_processed = sum(processed_results)
        except Exception as e:
            logging.error(f"Error in parallel processing: {str(e)}")
            logging.error(traceback.format_exc())
        
        end_time = datetime.now()
        duration = end_time - start_time
        seconds = duration.total_seconds()
        
        # 输出结果统计
        logging.info(f"Hybrid export completed: {total_processed}/{total_items} items in {seconds:.1f} seconds")
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
