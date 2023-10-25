# MIT License
#
# Copyright (c) 2018 Evgeniy Filatov, evgeniyfilatov@gmail.com
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
import json

import click
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter

from ethereumetl.jobs.export_geth_traces_job import ExportGethTracesJob
from ethereumetl.jobs.exporters.geth_traces_item_exporter import geth_traces_item_exporter
from blockchainetl.logging_utils import logging_basic_config
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy

logging_basic_config()


def export_geth_traces(start_block, end_block, batch_size, output, max_workers, provider_uri):
    """Exports traces from geth node."""
    exporter = InMemoryItemExporter(item_types=['geth_trace'])
    job = ExportGethTracesJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        max_workers=max_workers,
        item_exporter=exporter
    )

    job.run()
    geth_traces = exporter.get_items('geth_trace')
    if isinstance(geth_traces, list):
        traces_iterable = (json.loads(trace) for trace in geth_traces)
    print(geth_traces)

if __name__ == '__main__':
    export_geth_traces(start_block=111269878, end_block=111269879, batch_size=1, output='/Users/fp/PycharmProjects/bsc-etl-airflow/dags/bscetl/cli/geth_traces.json', max_workers=1, provider_uri='https://solemn-purple-uranium.optimism.quiknode.pro/8c1c1c839b13597f1d9b55ed397d431dfe8bd577/')
