import time
import re
import datetime
from clickhouse_driver import Client
from CallManager import CallManager
from configparser import ConfigParser
import os


def write_to_log(text):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    file_log = open('log.txt', 'a')
    file_log.write(str(now) + ' ' + text + '\n')
    file_log.close()


def queue_status(clickhouse_client, ami_host, ami_port, ami_username, ami_secret):
    manager = CallManager(ami_host,ami_port,ami_username,ami_secret)


    prev_arr_insert_queueMembers = []
    prev_arr_insert_queueStatuses = []

    while True:
        time.sleep(1)

        try:
            manager.connect()
        except:
            pass

        now = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')


        try:
            max_batch_id_clickhouse = clickhouse_client.execute('SELECT max(batchID) from sandbox.telephonyQueueMembers')[0][0]
        except:
            continue

        max_batch_id_clickhouse = int(max_batch_id_clickhouse if max_batch_id_clickhouse != None else 0)

        try:
            queues_details = manager.get_queue_status('9950')
            channels_details = manager.get_channels_status()
            log_dict = {}
            log_dict['logDateTime'] = datetime.datetime.now()
            log_dict['queueStatusResult'] = str(queues_details)
            log_dict['sipShowChannelsResult'] = str(channels_details)
            log_arr = []
            log_arr.append(log_dict)
            clickhouse_client.execute('INSERT INTO logs.asteriskLog '
                                      '(logDateTime, queueStatusResult, sipShowChannelsResult) VALUES', log_arr)
        except:
            try:
                manager.ping()
            except:
                pass
            continue

        try:
            arr_insert_queueMembers = []
            arr_insert_queueMembers_clickhouse = []

            arr_insert_queueStatuses = []
            arr_insert_queueStatuses_clickhouse = []

            counter_states = 0
            counter_states_calls = 0

            #insert_string = ''
            calls_dict = {}
            for call in channels_details:
                if 'Tx' or 'Rx' in call['Last_Message'] and call['User/ANR'].isdigit():
                    calls_dict[call['User/ANR']] = ('Incoming' if 'Tx' in call['Last_Message'] else 'Outcoming')


            for member_dict in queues_details:

                if  member_dict.get('Event') == 'QueueParams':
                    continue
                else:
                    if counter_states < len(prev_arr_insert_queueMembers) and len(prev_arr_insert_queueMembers) > 0:
                        previous_state_queue_member = {}
                        previous_state_queue_member['statusID'] = prev_arr_insert_queueMembers[counter_states]['statusID']
                        previous_state_queue_member['isPaused'] = prev_arr_insert_queueMembers[counter_states]['isPaused']
                        previous_state_queue_member['pausedReason'] = prev_arr_insert_queueMembers[counter_states][
                            'pausedReason']
                        previous_state_queue_member['dateLastEvent'] = prev_arr_insert_queueMembers[counter_states][
                            'dateLastEvent']
                        previous_state_queue_member['callType'] = prev_arr_insert_queueMembers[counter_states][
                            'callType']
                        counter_states = counter_states + 1
                    else:
                        previous_state_queue_member = None

                    if counter_states_calls < len(prev_arr_insert_queueStatuses) and len(prev_arr_insert_queueStatuses) > 0:
                        previous_call_in_queue = {}
                        previous_call_in_queue['Uniqueid'] = prev_arr_insert_queueStatuses[counter_states_calls]['Uniqueid']
                    else:
                        previous_call_in_queue = None

                    if member_dict.get('Event') == 'QueueEntry':
                        prev_call = None
                        for prev in prev_arr_insert_queueStatuses:
                            if member_dict.get('Uniqueid') == prev['Uniqueid']:
                                prev_call = prev
                        current_call_in_queue = {}
                        current_call_in_queue['batchID'] = max_batch_id_clickhouse + 1
                        current_call_in_queue['externalPhoneNumber'] = member_dict.get('CallerIDNum')
                        current_call_in_queue['Uniqueid'] = member_dict.get('Uniqueid')
                        current_call_in_queue['queuePosition'] = int(member_dict.get('Position'))
                        if previous_call_in_queue != None and prev_call != None:
                            current_call_in_queue['queuingTime'] = prev_call['queuingTime']
                        else:
                            current_call_in_queue['queuingTime'] = now

                        arr_insert_queueStatuses.append(current_call_in_queue)
                        current_call_in_queue_clickhouse = current_call_in_queue.copy()
                        current_call_in_queue_clickhouse['batchID'] = max_batch_id_clickhouse + 1
                        del current_call_in_queue_clickhouse['Uniqueid']
                        arr_insert_queueStatuses_clickhouse.append(current_call_in_queue_clickhouse)

                    if member_dict.get('Event') == 'QueueMember':
                        current_queue_member = {}
                        current_queue_member['batchID'] = max_batch_id_clickhouse + 1
                        current_queue_member['memberName'] = member_dict.get('Name')
                        current_queue_member['internalPhoneNumber'] = re.search(r'\d\d\d', member_dict.get('Location'))[0]
                        if int(member_dict.get('LastCall')) > 0:
                            current_queue_member['dateLastCall'] = datetime.datetime.utcfromtimestamp(
                                int(member_dict.get('LastCall'))) + datetime.timedelta(hours=3)
                        else:
                            current_queue_member['dateLastCall'] = datetime.datetime(1971, 1, 1)

                        current_queue_member['countTakenCalls'] = int(member_dict.get('CallsTaken'))
                        current_queue_member['statusID'] = int(member_dict.get('Status'))
                        current_queue_member['isPaused'] = int(member_dict.get('Paused'))

                        if member_dict.get('PausedReason') == '':
                            current_queue_member['pausedReason'] = 'Not defined'
                        else:
                            current_queue_member['pausedReason'] = member_dict.get('PausedReason')

                        if previous_state_queue_member != None \
                                and current_queue_member['statusID'] == previous_state_queue_member['statusID'] \
                                and current_queue_member['isPaused'] == previous_state_queue_member['isPaused'] \
                                and current_queue_member['pausedReason'] == previous_state_queue_member['pausedReason']:
                            current_queue_member['dateLastEvent'] = previous_state_queue_member['dateLastEvent']
                        else:
                            current_queue_member['dateLastEvent'] = now
                        type_ = calls_dict.get(current_queue_member['internalPhoneNumber'])
                        type_ = (type_ if current_queue_member['statusID'] == 2 else None)
                        current_queue_member['callType'] = (type_ if type_ != None else 'Not defined')

                        arr_insert_queueMembers.append(current_queue_member)

                        current_queue_member_clickhouse = current_queue_member.copy()
                        current_queue_member_clickhouse['batchID'] = max_batch_id_clickhouse + 1
                        arr_insert_queueMembers_clickhouse.append(current_queue_member_clickhouse)

            is_insert = False
            for prev, cur in zip(prev_arr_insert_queueStatuses, arr_insert_queueStatuses):
                if (prev['queuePosition'] != cur['queuePosition'] or prev['queuingTime'] != cur['queuingTime']
                    or len(prev_arr_insert_queueStatuses)!= len(arr_insert_queueStatuses)) and len(
                        arr_insert_queueStatuses) > 0:
                    is_insert = True

            for prev, cur in zip(prev_arr_insert_queueMembers, arr_insert_queueMembers):
                if prev['memberName'] != cur['memberName'] or prev['internalPhoneNumber'] != cur['internalPhoneNumber'] or \
                        prev['dateLastCall'] != cur['dateLastCall'] or prev['countTakenCalls'] != cur['countTakenCalls'] or \
                        prev['statusID'] != cur['statusID'] or prev['isPaused'] != cur['isPaused'] or \
                        prev['callType'] != cur['callType'] or prev['pausedReason'] != cur['pausedReason']:
                    is_insert = True
            if len(prev_arr_insert_queueMembers) == 0 and len(prev_arr_insert_queueStatuses) == 0:
                is_insert = True

            if is_insert:
                try:
                    if len(arr_insert_queueStatuses_clickhouse) > 0:
                        clickhouse_client.execute(
                            'INSERT INTO sandbox.telephonyQueueStatuses (batchID, externalPhoneNumber, queuePosition, queuingTime) VALUES',
                            arr_insert_queueStatuses_clickhouse)
                    clickhouse_client.execute('INSERT INTO sandbox.telephonyQueueMembers (batchID, memberName, internalPhoneNumber, countTakenCalls, dateLastCall, statusID, isPaused, pausedReason, dateLastEvent, callType) VALUES', arr_insert_queueMembers_clickhouse)
                except:
                    continue


            prev_arr_insert_queueMembers = arr_insert_queueMembers
            prev_arr_insert_queueStatuses = arr_insert_queueStatuses

        except:
            print('Became something interesting -_-')
            continue

        manager.close()



def main():
    db_config_path = os.environ['ETL_CONFIG_DIR'] + 'db.ini'
    ami_config_path = os.environ['ETL_CONFIG_DIR'] + 'asterisk-ami.ini'

    cfg = ConfigParser()
    cfg.read(db_config_path)

    ch_host = cfg.get("dwh_clickhouse", "host")
    ch_user = cfg.get("dwh_clickhouse", "user")
    ch_password = cfg.get("dwh_clickhouse", "password")

    clickhouse_client = Client(ch_host, user=ch_user, password=ch_password, database='sandbox')

    cfg.read(ami_config_path)
    ami_host = cfg.get("asterisk-ami", "host")
    ami_port = cfg.get("asterisk-ami", "port")
    ami_username = cfg.get("asterisk-ami", "username")
    ami_secret = cfg.get("asterisk-ami", "secret")
    queue_status(clickhouse_client, ami_host, ami_port, ami_username, ami_secret)


if __name__ == '__main__':
    main()

