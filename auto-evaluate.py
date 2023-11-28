#!/usr/bin/env python3

import argparse
import os
import tempfile
import subprocess
from typing import Optional, TextIO
import time


class SiteInfo:
    def __init__(self, prefix: str, site_id: int, port: int):
        self.site_id = site_id
        self.port = port
        history_file_name = f'site-{site_id}-history.txt'
        self.history_file_path = os.path.join(prefix, history_file_name)

    def __str__(self):
        return f"""SiteInfo(site_id={self.site_id}, port={self.port})"""


class ClientInfo:
    def __init__(self, prefix: str, site_id: int, client_id: int, conn_str: str):
        self.site_id = site_id
        self.client_id = client_id
        self.conn_str = conn_str
        transactions_file_name = f'site-{self.site_id}-client-{self.client_id}-transactions.sql'
        self.transactions_file_path = os.path.join(prefix, transactions_file_name)
        output_file_name = f'site-{self.site_id}-client-{self.client_id}-output.txt'
        self.output_file_path = os.path.join(prefix, output_file_name)

    def __str__(self):
        return f"""ClientInfo(site_id={self.site_id}, client_id={self.client_id})"""

    def get_site_id(self):
        return self.site_id

    def get_client_id(self):
        return self.client_id

    def get_transaction_path(self):
        return self.transactions_file_path


def ports_generator(start: int, count: int):
    val = start
    while val < count:
        yield val
        val += 1


def files_prefix(keep_files: bool) -> str:
    if keep_files:
        return os.path.join(os.getcwd(), 'results')
    else:
        return os.path.join(tempfile.gettempdir(), 'results')


def setup_database(db_path: str, schema_path: str):
    cmd_line = ['sqlite3', '-init', schema_path, db_path, '.quit']
    print(f'setting up database: {cmd_line}...')
    handle = subprocess.run(cmd_line)
    handle.check_returncode()
    print('database is set up')


def generate_site_specs(prefix: str, site_count: int) -> list[SiteInfo]:
    site_infos = []
    port = 50052
    for site_id in range(0, site_count):
        site_infos.append(SiteInfo(prefix, site_id, port))
        port += 1
    return site_infos


def generate_client_specs(prefix: str, sites: list[SiteInfo], client_count: int) -> list[ClientInfo]:
    clients = []
    for site in sites:
        for client_id in range(0, client_count):
            clients.append(ClientInfo(prefix, site.site_id, client_id, f'0.0.0.0:{site.port}'))
    return clients


def generate_transaction_files(client_specs: list[ClientInfo],
                               db_file: str,
                               transaction_count: int,
                               build_version: str):
    for client in client_specs:
        executable = os.path.join(os.getcwd(), 'target', build_version, 'sql-trans-gen')
        print(f'Running {[executable, '-o', client.get_transaction_path(), '-c', str(transaction_count), db_file]}')
        handle = subprocess.run(
            [executable, '-o', client.get_transaction_path(), '-c', str(transaction_count), db_file],
            capture_output=True,
        )
        handle.check_returncode()


def start_concurrency_controller(prefix: str, build_version: str) -> tuple[subprocess.Popen[str], TextIO]:
    executable = os.path.join(os.getcwd(), 'target', build_version, 'sddms-central')
    output_path = os.path.join(prefix, 'sddms-central-output.txt')
    output_file = open(output_path, 'w')
    return subprocess.Popen(executable, stdout=output_file, stderr=subprocess.STDOUT, encoding='utf-8'), output_file


def start_sites(prefix: str, site_infos: list[SiteInfo], db_path: str, build_version: str) -> list[tuple[subprocess.Popen[str], TextIO]]:
    handles = []
    for site in site_infos:
        executable = os.path.join(os.getcwd(), 'target', build_version, 'sddms-site')
        command_line = [
            executable, '-p', str(site.port), '--history-file', site.history_file_path, db_path, '0.0.0.0:50051'
        ]
        output_path = os.path.join(prefix, f'sddms-site-{site.site_id}-output.txt')
        output_file = open(output_path, 'w')
        handle = subprocess.Popen(command_line, stdout=output_file, stderr=subprocess.STDOUT, encoding='utf-8')
        handles.append((handle, output_file))
        print(f'Started site {site.site_id} listening on port {site.port} with process id {handle.pid}')
    return handles


def start_clients(prefix: str, client_infos: list[ClientInfo], build_version: str) -> list[tuple[subprocess.Popen[str], TextIO]]:
    handles = []
    for client in client_infos:
        executable = os.path.join(os.getcwd(), 'target', build_version, 'sddms-client')
        command_line = [
            executable, '-i', client.transactions_file_path, client.conn_str,
        ]
        output_path = os.path.join(prefix, f'sddms-client-{client.site_id}-{client.client_id}-output.txt')
        output_file = open(output_path, 'w')
        handle = subprocess.Popen(command_line, stdout=output_file, stderr=subprocess.STDOUT, encoding='utf-8')
        handles.append((handle, output_file))
        print(f'Started client {client.site_id}:{client.client_id} with process id {handle.pid}')
    return handles


def main(database_path: str, site_count: int, client_count: int, transaction_count: int, keep_files: bool,
         build_version: str, schema_file: Optional[str]):

    print(f'''
Config:
DB path: {database_path}
site count: {site_count}
client count: {client_count}
transaction count: {transaction_count}
keep_files: {keep_files}
build: {build_version}
schema file: {schema_file}
''')

    # get the prefix that all result files should be created relative to
    prefix = files_prefix(keep_files)
    print(f'Writing result files in {prefix}')

    # set up the database file if there's a schema
    if schema_file is not None:
        setup_database(database_path, schema_file)

    # Make a set of the sites
    site_infos = generate_site_specs(prefix, site_count)
    print(site_infos)
    client_infos = generate_client_specs(prefix, site_infos, client_count)
    print(client_infos)

    # generate all the transaction files
    print('Generating transaction files...')
    generate_transaction_files(client_infos, database_path, transaction_count, build_version)
    print('Done generating transaction files')

    # Start the concurrency controller
    cc_handle, cc_output_file = start_concurrency_controller(prefix, build_version)
    time.sleep(1)
    print(f"Started the concurrency controller with process id {cc_handle.pid}")

    # start the different sites
    site_handles = start_sites(prefix, site_infos, database_path, build_version)
    time.sleep(1)
    print('started sites...')

    # Start the sites and wait for them to finish
    client_handles = start_clients(prefix, client_infos, build_version)
    for handle, output_file in client_handles:
        ret = handle.wait()
        print(f'Client process {handle.pid} exited with return code {ret}')

    # Wait on sites to finish up
    for handle, output_path in site_handles:
        print(f'killing site process {handle.pid}...')
        handle.terminate()
        ret_code = handle.wait()
        print(f'Process {handle.pid} finished with code {ret_code}')

    # Wait for concurrency controller to finish up
    print('Killing concurrency controller...')
    cc_handle.terminate()
    cc_exit_code = cc_handle.wait()
    print(f'Concurrency controller finished with exit code {cc_exit_code}')

    print('done!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='auto-evaluate.py', description='Automates the evaluation steps for SDDMS')
    parser.add_argument('-d', '--database', help='The database to execute each site on', required=True)
    parser.add_argument('-s', '--site-count', help='The number of sites to use', default=4)
    parser.add_argument('-c', '--client-count', help='How many clients to run per site', default=1)
    parser.add_argument('-t', '--transaction-count', help='The number of transactions to invoke', default=100)
    parser.add_argument('-k', '--keep-files', help="If set, don't delete intermediate files", default=False, action='store_true')
    parser.add_argument('-b', '--build', help='Which build to use', choices=['release', 'debug'], default='release')
    parser.add_argument('--schema', help='A schema file to setup a fresh database with', required=False)

    args = parser.parse_args()
    main(args.database, args.site_count, args.client_count, args.transaction_count, args.keep_files, args.build,
         args.schema)
