import argparse
import json
import logging
import os
import time
from datetime import timedelta

import pandas as pd

from admin.admin import start_admin
from db.db import DBManager, get_session
from server.s3 import S3Manager
from server.server import update_info, update_utts
from multiprocessing import Process

log = logging.getLogger(__file__)

parser = argparse.ArgumentParser()

parser.add_argument('mode', help='select a routine', type=str, choices={'server', 'poller', 'dpa_dumper', 'drop_tables'})
parser.add_argument('-p', '--port', help='select admin port', type=int, default=5000)
parser.add_argument('-ac', '--amazon-container', help='http of container to get additional info', type=str)


def verify_config(config):
    bad_keys = [k for k, v in config.items() if v == '']
    print(bad_keys)
    if bad_keys:
        raise ValueError(f'Following parameters at config file are empty: {", ".join(bad_keys)}')


def main():
    args = parser.parse_args()
    # TODO: make proper path handling
    with open('core/config.json') as config_file:
        config = json.load(config_file)
    db_config = config['DB']
    db_config['user'] = db_config.get('user') or os.getenv('DB_USER')
    db_config['password'] = db_config.get('password') or os.getenv('DB_PASSWORD')
    db_config['host'] = db_config.get('host') or os.getenv('DB_HOST')
    db_config['dbname'] = db_config.get('dbname') or os.getenv('DB_NAME')
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    dialog_dumps_bucket = os.getenv('DIALOG_DUMPS_BUCKET')
    verify_config(db_config)
    session = get_session(db_config['user'], db_config['password'], db_config['host'], db_config['dbname'])

    if args.mode == 'poller':
        s3 = S3Manager(aws_access_key_id, aws_secret_access_key, dialog_dumps_bucket, 'alexaprize', '263182626354', True)
        db = DBManager(session)
        last_utt_time = db.get_last_utterance_time()
        if last_utt_time is not None:
            last_utt_time = last_utt_time - timedelta(hours=6)
        update_utts(s3, db, last_utt_time, False)

    def dpa_dumper():
        from server.dump_new_dialogs_from_dpagent import dump_new_dialogs
        log.info('starting dumping new dialogs')
        dump_new_dialogs(session, dpagent_base_url=args.amazon_container)
        log.info('new dialogs dumped')
        if aws_access_key_id:
            s3 = S3Manager(aws_access_key_id, aws_secret_access_key, '', 'alexaprize', '263182626354', True)
            db = DBManager(session)
            last_utt_time = db.get_last_utterance_time()
            try:
                update_info(s3, db, last_utt_time)
            except pd.errors.EmptyDataError as err:
                print(repr(err))

    def dump():
        while True:
            dpa_dumper()
            time.sleep(3600)

    if args.mode == 'server':
        p = Process(target=dump)
        p.start()
        admin = config['admin']
        admin['user'] = admin.get('user') or os.getenv('ADMIN_USER')
        admin['password'] = admin.get('password') or os.getenv('ADMIN_PASSWORD')
        start_admin(session, admin['user'], admin['password'], args.port, args.amazon_container)

    if args.mode == 'dpa_dumper':
        dpa_dumper()

    if args.mode == "drop_tables":
        from db.db import drop_all_tables
        drop_all_tables(session)

if __name__ == "__main__":
    main()
