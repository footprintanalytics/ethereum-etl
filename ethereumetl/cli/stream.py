# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging
import random
from time import sleep

import click
from blockchainetl.streaming.streaming_utils import configure_signals, configure_logging
from ethereumetl.enumeration.entity_type import EntityType

from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.item_exporter_creator import create_item_exporters
from ethereumetl.streaming.web3_provider_selector import Web3ProviderSelector
from ethereumetl.thread_local_proxy import ThreadLocalProxy


def stream(last_synced_block_file, lag, provider_uri, output, start_block, entity_types,
           period_seconds=10, batch_size=2, block_batch_size=10, max_workers=5, log_file=None, pid_file=None):
    """Streams all data types to console or Google Pub/Sub."""
    configure_logging(log_file)
    configure_signals()
    entity_types = parse_entity_types(entity_types)

    from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
    from blockchainetl.streaming.streamer import Streamer

    # TODO: Implement fallback mechanism for provider uris instead of picking randomly
    # provider_uri = pick_random_provider_uri(provider_uri)
    provider_uri_0 = parse_provider_uri(provider_uri)[0]
    logging.info('Using new' + provider_uri_0)

    streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri_0, batch=True)),
        web3_provider_selector=ThreadLocalProxy(lambda: Web3ProviderSelector(provider_uri)),
        item_exporter=create_item_exporters(output),
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=entity_types
    )
    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        period_seconds=period_seconds,
        block_batch_size=block_batch_size,
        retry_errors=False,
        pid_file=pid_file
    )
    streamer.stream()


def parse_entity_types(entity_types):
    entity_types = [c.strip() for c in entity_types.split(',')]

    # validate passed types
    for entity_type in entity_types:
        if entity_type not in EntityType.ALL_FOR_STREAMING:
            raise click.BadOptionUsage(
                '--entity-type', '{} is not an available entity type. Supply a comma separated list of types from {}'
                    .format(entity_type, ','.join(EntityType.ALL_FOR_STREAMING)))

    return entity_types


def pick_random_provider_uri(provider_uri):
    provider_uris = parse_provider_uri(provider_uri)
    return random.choice(provider_uris)


def parse_provider_uri(provider_uri):
    return [uri.strip() for uri in provider_uri.split(',')]


if __name__ == '__main__':
    stream(last_synced_block_file='/Users/fp/PycharmProjects/ethereum-etl/ethereumetl/cli/synced_block/aaa.txt', lag=10,
           provider_uri='https://solemn-purple-uranium.optimism.quiknode.pro/8c1c1c839b13597f1d9b55ed397d431dfe8bd577/',
           output=None, start_block=None, entity_types='geth_traces',
           period_seconds=10, batch_size=1, block_batch_size=1, max_workers=1, log_file=None, pid_file=None)

