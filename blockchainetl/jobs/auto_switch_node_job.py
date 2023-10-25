import logging
from time import sleep

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.streaming.web3_provider_selector import Web3ProviderSelector
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.utils import get_provider_uri
from ethereumetl.web3_utils import build_web3


class AutoSwitchNodeJob(BaseJob):
    def __init__(self, web3_provider_selector):
        self.web3_provider_selector = web3_provider_selector

    def run(self):
        try:
            self._start()
            self._export()
        except Exception as e:
            logging.exception('_export() error, enter auto switch node for loop')
            self.export_with_auto_switch_node()
        finally:
            self._end()

    def _export_batch_with_auto_switch_node(self, items):
        retry = True
        counter = 0
        while retry:
            try:
                self._export_function(items)
                retry = False
            except Exception as e:
                sleep_s = 280
                if counter > len(self.web3_provider_selector.provider_uri_range)*3:
                    logging.info(f'try too must, retry after sleep {sleep_s}s ')
                    sleep(sleep_s)
                logging.exception('An exception occurred. Trying another uri')
                counter += 1
                sleep(5)
                if not self.web3_provider_selector:
                    self.web3_provider_selector = ThreadLocalProxy(lambda: Web3ProviderSelector(get_provider_uri()))
                self.batch_web3_provider = self.web3_provider_selector.select_provider()
                self.web3 = ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider))
                # self._export_batch_with_auto_switch_node(items)

    def _export_function(self, items):
        pass

    def export_with_auto_switch_node(self):
        retry = True
        counter = 0
        while retry:
            self.batch_web3_provider = self.web3_provider_selector.select_provider()
            self.web3 = ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider))
            try:
                self._export()
                retry = False
            except Exception as e:
                sleep_s = 280
                if counter > len(self.web3_provider_selector.provider_uri_range)*3:
                    logging.info(f'try too must, retry after sleep {sleep_s}s ')
                    sleep(sleep_s)
                counter += 1
                logging.exception('An exception occurred. Trying another uri')
                sleep(5)
                # self.export_with_auto_switch_node()


def select_web3_provider_run_request(text, web3_provider_selector):
    if not web3_provider_selector:
        web3_provider_selector = Web3ProviderSelector(get_provider_uri())
        batch_web3_provider = web3_provider_selector.batch_web3_provider
    else:
        logging.exception('An exception occurred. Trying another uri')
        batch_web3_provider = web3_provider_selector.select_provider()
    try:
        response = batch_web3_provider.make_batch_request(text)
        return response
    except:
        select_web3_provider_run_request(text, web3_provider_selector)