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

# 全局函数，用于多进程调用
def process_items_batch(batch_data):
    connection_url, topic_prefix, topic_mapping, items, batch_id = batch_data
    
    # 设置进程级别超时保护
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(60)  # 5分钟超时
    
    # 在每个进程中创建独立的producer，添加错误处理
    try:
        producer = KafkaProducer(
            bootstrap_servers=connection_url,
            max_block_ms=60000,  # 最大阻塞60秒
            request_timeout_ms=30000,  # 请求超时30秒
            api_version_auto_timeout_ms=30000,
            reconnect_backoff_ms=1000,
            reconnect_backoff_max_ms=10000
        )
    except Exception as e:
        logging.error(f"Process {batch_id} failed to create Kafka producer: {str(e)}")
        return 0
    
    try:
        processed_count = 0
        error_count = 0
        start_time = datetime.now()
        pid = multiprocessing.current_process().pid
        logging.info(f"Process {pid} (batch {batch_id}) started at {start_time} with {len(items)} items")
        
        # 进度报告频率
        report_interval = max(1, len(items) // 10)  # 10%的项目报告一次进度
        last_report_time = start_time
        
        for i, item in enumerate(items):
            if i > 0 and i % report_interval == 0:
                now = datetime.now()
                elapsed = (now - last_report_time).total_seconds()
                progress_pct = (i / len(items)) * 100
                items_per_second = report_interval / elapsed if elapsed > 0 else 0
                logging.info(f"Process {pid} (batch {batch_id}) progress: {progress_pct:.1f}% ({i}/{len(items)}) - {items_per_second:.1f} items/sec")
                last_report_time = now
            
            try:
                item_type = item.get('type')
                if item_type is not None and item_type in topic_mapping:
                    data = json.dumps(item).encode('utf-8')
                    # 添加发送超时和错误捕获
                    future = producer.send(topic_prefix + topic_mapping[item_type], value=data)
                    # 尝试立即获取结果来检测可能的错误，但设置超时为1秒防止阻塞
                    try:
                        future.get(timeout=1)
                        processed_count += 1
                    except KafkaError as ke:
                        error_count += 1
                        logging.warning(f"Process {pid} (batch {batch_id}) Kafka error: {str(ke)}")
                        # 如果遇到大量错误，尝试重新连接
                        if error_count > 10 and error_count % 10 == 0:
                            producer.close(10)  # 10秒超时关闭
                            time.sleep(2)  # 等待一会儿再重连
                            producer = KafkaProducer(bootstrap_servers=connection_url)
                else:
                    logging.warning(f'Topic for item type "{item_type}" is not configured.')
            except Exception as e:
                error_count += 1
                logging.error(f"Process {pid} (batch {batch_id}) error processing item {i}: {str(e)}")
                if error_count >= 100:  # 如果错误太多，提前退出
                    logging.error(f"Process {pid} (batch {batch_id}) too many errors, aborting batch")
                    break
        
        # 确保所有消息发送，但添加超时
        try:
            producer.flush(timeout=30)  # 30秒超时
        except Exception as e:
            logging.error(f"Process {pid} (batch {batch_id}) error during flush: {str(e)}")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Process {pid} (batch {batch_id}) completed {processed_count} items with {error_count} errors in {duration:.1f} seconds ({processed_count/duration if duration > 0 else 0:.1f} items/sec)")
        
        # 取消超时警报
        signal.alarm(0)
        return processed_count
    except TimeoutError:
        logging.error(f"Process {batch_id} timed out after 5 minutes")
        return 0
    except Exception as e:
        logging.error(f"Process {batch_id} unexpected error: {str(e)}")
        logging.error(traceback.format_exc())
        return 0
    finally:
        # 始终确保关闭producer，即使发生错误
        try:
            producer.close(timeout=10)  # 10秒超时
        except:
            pass  # 忽略关闭时的错误


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
            batch_id = len(batches) + 1
            end_idx = min(i + chunk_size, total_items)
            batch_items = items[i:end_idx]
            # 参数顺序: connection_url, topic_prefix, topic_mapping, items, batch_id
            batches.append((
                self.connection_url,
                self.topic_prefix,
                self.item_type_to_topic_mapping,
                batch_items,
                batch_id
            ))
        
        # 使用进程池处理批次
        start_time = datetime.now()
        total_processed = 0
        
        # 添加整体超时保护
        try:
            # 设置更短的超时以获取结果，防止整体假死
            with Pool(processes=min(process_count, len(batches))) as pool:
                results = pool.map_async(process_items_batch, batches)
                
                # 等待结果，但添加超时检查
                while not results.ready():
                    logging.info(f"Waiting for {len(batches)} batch processes to complete...")
                    if results.wait(timeout=60):  # 每分钟检查一次结果
                        break
                
                # 获取结果
                processed_results = results.get(timeout=10)  # 10秒超时获取结果
                total_processed = sum(processed_results)
        except Exception as e:
            logging.error(f"Error in parallel processing: {str(e)}")
            logging.error(traceback.format_exc())
        
        end_time = datetime.now()
        duration = end_time - start_time
        seconds = duration.total_seconds()
        
        # 输出结果统计
        logging.info(f"Multiprocessing export completed: {total_processed}/{total_items} items in {seconds:.1f} seconds")
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
