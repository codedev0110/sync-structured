# 
#  Import record to current server 
# 

import os
import sys

PROJECT_PATH = os.path.abspath( os.path.join(os.path.dirname(__file__),os.pardir) ) 
sys.path.insert(0, PROJECT_PATH)

import argparse
import shutil
from datetime import datetime, timedelta
from time import sleep

# import numpy as np
import common.db_conn.db  as db
import common.db_conn.db1 as db1
import common.db_conn.db2 as db2
import common.db_conn.db3 as db3
import common.db_conn.db4 as db4
import common.db_conn.db5 as db5
import common.db_conn.db6 as db6
import common.db_conn.db7 as db7
import common.db_conn.db8 as db8
import common.db_conn.db9 as db9
import common.utils as ut

curr_dt = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

# server_order = [5, 6, 3, 2, 4]

def get_stream_type_sql(stream_type):

    if stream_type == 'audio':
        stream_type_sql = f' and stream_type = 1 '
    elif stream_type == 'video':
        stream_type_sql = f' and stream_type = 2 '
    else:
        stream_type_sql = ''

    return stream_type_sql



def parse_servers_order(server_order_str):
    servers = server_order_str.split(',')
    srv_order = []
    for svr in servers:
        if svr :
            try:
                srv_order.append(int(svr.strip()))
            except Exception as e:
                print('Exception on get_server_order : ', e)   

    return srv_order  



def get_servers_order(stream_type):
    server_order_str = ut.get_parameter(db, f'server_order_{stream_type}_records_import')
    if not server_order_str :
        return []
    servers_order_general = parse_servers_order(server_order_str)

    servers_order = {}

    streams = db.select(f"select * from streams where enabled=true {get_stream_type_sql(stream_type)} order by id")

    for stream in streams:
        stream_servers_orders = parse_servers_order(stream['server_import_order'])
        if stream_servers_orders:
            servers_order[stream['id']] = stream_servers_orders
        else:
            servers_order[stream['id']] = servers_order_general

    return servers_order




def select_path_prefix(svr, svr_local):
    v_path = ''
    if svr==svr_local:
        v_path = '/home/neurotime/stream_analyse/recording/' 
    else :
        v_path = f'/mnt/fs_svr{svr}/recording/'

    # elif arg=='2':
    #     v_path = '/mnt/fs_svr2/recording/'
    # elif arg=='3':
    #     v_path = '/mnt/fs_svr3/recording/'
    # elif arg=='4':
    #     v_path = '/mnt/fs_svr4/recording/'
    
    return v_path


def select_db(server_id):
    if server_id==1:
        return db1
    elif server_id==2:
        return db2
    elif server_id==3:
        return db3
    elif server_id==4:
        return db4
    elif server_id==5:
        return db5
    elif server_id==6:
        return db6
    elif server_id==7:
        return db7
    elif server_id==8:
        return db8
    elif server_id==9:
        return db9

 

def add_record_to_imported(server_id, record_id, imported_ids):
    # server_id = record['imported_source_id']
    if server_id in imported_ids:
        imported_ids[server_id].append(record_id)
    else:
        imported_ids[server_id] = [record_id]   
    
    return imported_ids





def insert_record(db, result, source_svr_id):

    

    sql = """insert into records (stream_id, path, started_at, ended_at, duration, duration_recorded, return_code, stream_type, url_index, 
                                  is_record_approved, processed, converted_to_mp3, converted_to_low, record_rate, imported_record_id, imported_source_id, 
                                  sampling_rate, frame_width, shape, fps, frame_step, v_shape, is_preprocessed) 
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    r = db.insert(sql, (result['stream_id'], 
                        result['path'], 
                        result['started_at'], 
                        result['ended_at'], 
                        result['duration'], 
                        result['duration_recorded'], 
                        result['return_code'], 
                        result['stream_type'], 
                        result['url_index'],
                        True,
                        False, 
                        result['converted_to_mp3'],
                        result['converted_to_low'],
                        result['record_rate'],              #calc_record_rate(result)
                        result['id'],
                        source_svr_id,
                        result['sampling_rate'], 
                        result['frame_width'], 
                        result['shape'],
                        result['fps'], 
                        result['frame_step'], 
                        result['v_shape'],
                        result['is_preprocessed']
                        )
                    )

    return r



def update_record_not_approved(db, records_list) :   
    if records_list:
        # s_records_list = ','.join(str(id) for id in records_list)
        r = db.update( "update records set is_record_approved = %s where id in %s ", ( False, tuple(records_list) ) )
    else:
        r = True

    return r



def disable_results(db, records_list) :
    if records_list:
        # s_records_list = ','.join(str(id) for id in records_list)
        r = db.update( "update results set is_approved = %s, active_status=%s where record_id in %s ", ( False, 7, tuple(records_list) ) )   
    else:    
        r = True
    return r



# def calc_record_rate(record):
#     record_rate = record['duration'] / ((record['ended_at'] - record['started_at']).total_seconds() / 60) 
#     return min(1.0, round(record_rate,1) )



def join_record_periods(recorded_periods, start_time, end_time) :

    # find time period for join, if found - join , else add to list
    is_found = False
    for rec_time in recorded_periods :
        if         (rec_time['start'] < start_time and start_time < rec_time['end'])   \
                or (rec_time['start'] < end_time   and end_time   < rec_time['end'])   \
                or (start_time <= rec_time['start'] and rec_time['end'] <= end_time) :

            rec_time['start'] = min(start_time, rec_time['start'])
            rec_time['end']   = max(end_time, rec_time['end'])
            is_found = True
            break

    if not is_found :
        recorded_periods.append({'start': start_time, 'end': end_time})

    # print('\n',start_time, end_time, is_found)
    # print(recorded_times)

    return recorded_periods



def append_according_record_periods(not_recorded_periods, not_rec_start, not_rec_end):
    """ append non recorded periods according record periods (61 min)"""
    
    dh=(not_rec_end - not_rec_start).total_seconds()/3600
    n=int(dh)+2
    # print(not_rec_start, not_rec_end, dh, n)

    for i in range(n):
        dt=not_rec_start + timedelta(hours=i)
        d1 = datetime(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour, minute=0, second=0,  microsecond=0)
        d2 = d1 + timedelta(minutes=61)
        
        d1 = max(d1,not_rec_start)
        d2 = min(d2,not_rec_end)       
        # print(d1,d2, (dt2-d2).total_seconds())

        not_recorded_periods.append({'start': d1, 'end': d2})
        
        if (not_rec_end-d2).total_seconds() < 1 :
            break    

    return not_recorded_periods



def extract_non_recorded_periods(recorded_periods, start_time, end_time):
    """ Extract non recorded periods from recorded periods"""

    not_recorded_periods = []

    if len(recorded_periods)==0 :
        # not_recorded_periods = [{'start': start_time, 'end': end_time}]
        not_recorded_periods = append_according_record_periods(not_recorded_periods, start_time, end_time )

    else:
        # if (recorded_periods[0]['start'] - start_time).total_seconds() > 0 : 
        if recorded_periods[0]['start'] > start_time :
            # not_recorded_periods.append({'start': start_time, 'end': recorded_periods[0]['start']})
            not_recorded_periods = append_according_record_periods(not_recorded_periods, start_time, recorded_periods[0]['start'] )

        if len(recorded_periods)>1 :
            for i in range(len(recorded_periods)-1) :   
                # not_recorded_periods.append({'start': recorded_periods[i]['end'], 'end': recorded_periods[i+1]['start']})
                not_recorded_periods = append_according_record_periods(not_recorded_periods, recorded_periods[i]['end'], recorded_periods[i+1]['start'] )

        if end_time > recorded_periods[-1]['end'] : 
            # not_recorded_periods.append({'start': recorded_periods[-1]['end'], 'end': end_time })
            not_recorded_periods = append_according_record_periods(not_recorded_periods, recorded_periods[-1]['end'], end_time )

    return not_recorded_periods




def get_recording_status_in_period_by_stream_id(db, stream_id, sync_time_start, sync_time_end) :

    # we need to check 61min before to check time xx:00 - xx:[start_recording], for ex 00:00 - 00:30
    # usually, it was recorded in record, started in previous hour

    sync_time_start_1 = sync_time_start - timedelta(minutes=61)

    sql = f"""select * from records 
                where 
                started_at > '{sync_time_start_1}' and started_at < '{sync_time_end}' 
                and stream_id = {stream_id}
                and is_record_approved=true
                order by stream_id, started_at
            """
    # print(sql)

    records = db.select(sql)
    # time_schedule = { 'start': datetime.strptime(sync_time_start, '%Y-%m-%d %H:%M:%S'), 
    #                   'end'  : datetime.strptime(sync_time_end, '%Y-%m-%d %H:%M:%S')  }

    recorded_periods = []
    

    for record in records :
        recorded_periods = join_record_periods(recorded_periods, record['started_at'], record['ended_at'])

    # clear periods before sync_time_start
    if recorded_periods :
        for rec_time in recorded_periods.copy() :
            if rec_time['start']>=sync_time_start :
                break
            elif rec_time['end']<=sync_time_start :
                recorded_periods.remove(rec_time)
            else :
                rec_time['start']=sync_time_start
                break


    # print(recorded_periods)

    #non_recorded_periods = extract_non_recorded_periods(recorded_periods, datetime.strptime(sync_time_start, '%Y-%m-%d %H:%M:%S'), datetime.strptime(sync_time_end, '%Y-%m-%d %H:%M:%S'))
    non_recorded_periods = extract_non_recorded_periods(recorded_periods, sync_time_start, sync_time_end)


    # print(recorded_periods)
    # print(non_recorded_periods)

    return recorded_periods, non_recorded_periods



def get_recording_status_in_period(db, sync_time_start, sync_time_end, stream_type, stream_id) :

    sql_stream_id   = '' if stream_id<0 else f' and id={stream_id} '
    sql_stream_type = get_stream_type_sql(stream_type)

    streams = db.select(f"select * from streams where enabled=true {sql_stream_type} {sql_stream_id} order by id")

    recorded_periods      = {}
    non_recorded_periods  = {}

    for stream in streams :       
        recorded_periods_for_stream, non_recorded_periods_for_stream = get_recording_status_in_period_by_stream_id(db, stream['id'], sync_time_start, sync_time_end) 
        recorded_periods[stream['id']]     = recorded_periods_for_stream
        non_recorded_periods[stream['id']] = non_recorded_periods_for_stream

    # print(recorded_periods)
    # print(non_recorded_periods)

    return recorded_periods, non_recorded_periods



def sort_periods_by_start(periods):
    sorted_periods = []
    for stream_id in periods:
        for period in periods[stream_id]:
            period['stream_id'] = stream_id
            sorted_periods.append(period)

    sorted_periods = sorted(sorted_periods, key=lambda x: x['start'])

    return sorted_periods



def is_record_in_imported(server_id, record_id, imported_ids):

    # print(server_id,record_id)
    if server_id in imported_ids:
        return (record_id in imported_ids[server_id])
    else:
        return False



def is_similar_record_exists_db(record):

    sql = f"""select id from records 
              where     started_at between '{record['started_at'] - timedelta(seconds=10)}' and '{record['started_at'] + timedelta(seconds=10)}'
                    and ended_at   between '{record['ended_at'] - timedelta(seconds=10)}' and '{record['ended_at'] + timedelta(seconds=10)}'
                    and record_rate between {record['record_rate']-0.01} and {record['record_rate']+0.01}
                    and stream_id = {record['stream_id']}
                    and is_record_approved = True
            """
    # print('is_similar_record_exists_db', sql)

    records = db.select(sql)

    return len(records)>0



def get_covered_records(record):

    sql = f"""select id from records 
              where     started_at > '{record['started_at'] - timedelta(seconds=15)}' 
                    and ended_at < '{record['ended_at'] + timedelta(seconds=15)}' 
                    and stream_id = {record['stream_id']}
                    and is_record_approved = True
            """
    records = db.select(sql)

    return [record['id'] for record in records]



def is_record_in_db(imported_record_id, server_id):

    sql = f"""select id from records 
              where     imported_source_id = {server_id}
                    and imported_record_id = {imported_record_id}
                    and is_record_approved = True

            """
    # print(sql)
    records = db.select(sql)

    return len(records)>0
    


def copy_records(server_local_id, disabled_record_id, server_id, record, imported_ids, is_sync_mode) :

    status=''
    s=''

    disabled_records_list = [disabled_record_id]
    disabled_records_list = [id for id in disabled_records_list if id>0]


    # if record['id'] in imported_ids :
    print(imported_ids)
    is_record_imported = is_record_in_imported(server_id, record['id'], imported_ids)
    if not is_record_imported :
        is_similar_record_exists = is_similar_record_exists_db(record)

    if  is_record_imported or is_similar_record_exists :

        if is_record_imported:
            s = 'record already imported'
        elif is_similar_record_exists:
            s = 'similar record exists'
    
        print('  >> NOT NEED IMPORT :', s)
        # n_no_need += 1
        status = 'no_need'
        # if is_sync_mode:
        #     disabled_records_list = disabled_records_list + get_covered_records(record)
        #     update_record_not_approved(db, disabled_records_list) 
        #     disable_results(db, disabled_records_list)

    else :

        # src = ./recorded/Radio/2022-10-17/2022-10-17-20-00-06_106fm_52min.mp3
        src = select_path_prefix(server_id, server_local_id)        + record['path'].replace('./','')
        dst = select_path_prefix(server_local_id, server_local_id)  + record['path'].replace('./','') 
        print('  >> copy ', src)

        if is_sync_mode:

            if is_record_in_db(record['id'], int(server_id)) :
                status = 'no_need'
                print('  > record already imported')    

            else :        

                src = os.path.splitext(src)[0] +'*'
                dst = os.path.dirname(dst)
                copy_result = ut.copy_files_to_dir(src, dst, is_overwrite=False, is_print_log=True)

                # if success copy

                if copy_result:

                    # n_updated += 1
                    status = 'updated'
                    print('  > success copy to ' + dst)

                    start_db_time = datetime.now()
                    disabled_records_list = disabled_records_list + get_covered_records(record)
                    insert_record(db, record, int(server_id))   
                    print(f'    disabled_records_list = {disabled_records_list}')
                    update_record_not_approved(db, disabled_records_list) 
                    disable_results(db, disabled_records_list)
                    print(f'  > db update duration : {datetime.now()-start_db_time}')

                    # imported_ids.append(record['id'])
                    imported_ids = add_record_to_imported(server_id, record['id'], imported_ids)

                else:
                    # n_no_success += 1
                    status = 'no_success'
                    print('  > error copy from ' + src)

        else:
            # need for view results in non sync mode
            # n_updated += 1
            status = 'updated'
            # imported_ids.append(record['id'])
            imported_ids = add_record_to_imported(server_id, record['id'], imported_ids)

    return status, imported_ids



def get_records_according_servers_order(srv_orders, sql):

    records = []
    server_id=-1
    for srv in srv_orders :
        records = select_db(srv).select(sql)
        server_id=srv
        if records :
            break

    return server_id, records



def add_records_from_other_servers(stream_type, server_local_id, record, imported_ids, servers_order, is_sync_mode):

    started_at   = ut.begin_of_hour(record['started_at'])
    ended_at     = ut.end_of_hour(started_at) + timedelta(minutes=3)
    stream_id    = record['stream_id'] 

    if stream_type=='video' :
        sql_condition = ' and converted_to_low = True '    # is_preprocessed
    else:
        sql_condition = ''


    sql =  f"""select * from records 
                where 
                ( '{started_at}' < started_at   and   ended_at < '{ended_at}' )
                and duration > 0
                and stream_id = {stream_id} 
                and is_record_approved = True
                and converted_to_mp3 = True
                {sql_condition}
                order by started_at
            """
    # print(sql)
    srv, records = get_records_according_servers_order(servers_order[stream_id], sql)
  
    status=''

    if records :
        print(f"  > Start copy any records from server {srv} between '{started_at}' and '{ended_at}' (add mode)")
        for record in records :
            status, imported_ids = copy_records(server_local_id, -1, srv, record, imported_ids, is_sync_mode)

    else:
        print(f"  > Can't find any records from another servers between '{started_at}' and '{ended_at}'")
        status = 'no_find'
        

    return status, imported_ids



def get_period_rate(record, period_start, period_end):
    ''' get rate (0-not cover, 1-total cover) for covering record time of gived period '''
    dt1 = max(record['started_at'], period_start)
    dt2 = min(record['ended_at'],   period_end)
    rate = (dt2 - dt1).total_seconds() / (period_end - period_start).total_seconds()
    return max(0, rate)



def get_records_from_server(server_id, sql, started_at, ended_at):
    record = None
    print('start select get_records_from_server') 
    start_process_time = datetime.now()                                  
    records = select_db(server_id).select(sql)
    print(f'end DB select with len = {len(records)},    process duration : {datetime.now()-start_process_time}')                                                                                                              
    # if records and calc_record_rate(records[0])>0.8:
    if records :
        max_id = 0
        if len(records)>1:
            rate_max = 0
            max_id = -1    
            i=-1
            for record in records:
                i += 1

                rate = get_period_rate(record, started_at, ended_at)
                # print(rate)
                if rate > rate_max:
                    rate_max = rate
                    max_id = i

          
        record = records[max_id]
        print(f'  > db-{server_id}, found records =',len(records), '  max rate id = ', max_id)
        print(' ',record['id'], ' ', record['path'], ' ', record['duration'], 'min  ', record['record_rate'], ' ',   
                                          record['started_at'].strftime("%Y-%m-%d %H:%M:%S"), '  ', record['ended_at'].strftime("%Y-%m-%d %H:%M:%S"))

    return record


'''
    if is_non_recorded_period :

        sql =  f"""select * from records 
                    where 
                        ( started_at < '{start2}' 
                            and  
                        '{ended_at}' < LEAST(ended_at, started_at + (interval '1 min' * duration)) + (interval '1 sec' * {delta_sec}) 
                        ) 
                        and duration > 0
                        and started_at >= '{started_hour}'
                        and stream_id = {stream_id} 
                        and is_record_approved = True
                        and converted_to_mp3 = True
                        and is_deleted = False
                        and return_code=0
                        {sql_condition}
                    order by duration desc
                """

    else:

        sql =  f"""select * from records 
                    where 
                        ( 
                            ( 
                                ( ( ((started_at - (interval '1 sec' * {delta_sec})) < '{started_at}')  or ( started_at between '{start1}' and '{start2}' )   )
                                and  
                                '{ended_at}' < LEAST(ended_at, started_at + (interval '1 min' * duration)) + (interval '1 sec' * {delta_sec}) ) 
                            ) 
                        )
                    and duration > {duration}
                    and started_at >= '{started_hour}'
                    and stream_id = {stream_id} 
                    and record_rate > {record_rate} 
                    and is_record_approved = True
                    and converted_to_mp3 = True
                    and is_deleted = False
                    and return_code=0
                    {sql_condition}
                    order by duration desc
                """


'''
      

def sync_records_from_other_servers(stream_type, server_local_id, record, imported_ids, servers_order, is_non_recorded_period, is_sync_mode ) :

    delta_sec  = 10  #  max time delay to start recording, all record try to start at same time according stream ID

    # print(record)
    record_id       = record['id']
    record_rate     = min(0.999, record['record_rate'] + 0.001)
    # imported_source = record['imported_source_id']
    started_at      = record['started_at']
    started_hour    = ut.begin_of_hour(started_at) 
    # ended_at        = min( record['ended_at'], record['started_at'] + timedelta(minutes=record['duration'])  )
    ended_at        = record['ended_at']
    duration        = min(60, record['duration'])  # not more 60 min
    stream_id       = record['stream_id'] 
    start1          = started_at - timedelta(seconds=delta_sec)
    start2          = started_at + timedelta(seconds=delta_sec)
    end1            = ended_at - timedelta(seconds=delta_sec)
    end2            = ended_at + timedelta(seconds=delta_sec)




#                         ( started_at  between  '{start1}' and '{start2}' )
#                        or  
#               and duration > {duration}
#               and extract(hour from started_at) = {started_at.hour}


# check also sql query for add period 
    if stream_type=='video' :
        sql_condition = ' and converted_to_low = True '    # is_preprocessed
    else:
        sql_condition = ''

    if is_non_recorded_period :
        # see old version above procedure
        # 						and (extract('seconds' from (ended_at - '{started_at}')) > 10)
        #                       and (extract('seconds' from ('{ended_at}' - started_at)) > 10) 

        
        sql =  f"""select * from records 
                    where 
                            started_at < '{ended_at}'  
                        and ended_at > '{started_at}'   
                        and started_at >= '{started_hour}'
                        and stream_id = {stream_id} 
                        and is_record_approved = True
                        and converted_to_mp3 = True
                        and is_deleted = False
                        and return_code=0
                        {sql_condition}
                    order by duration desc, started_at
                """

    else:

        sql =  f"""select * from records 
                    where 
                        ( 
                            ( 
                                ( ( ((started_at - (interval '1 sec' * {delta_sec})) < '{started_at}')  or ( started_at between '{start1}' and '{start2}' )   )
                                and  
                                '{ended_at}' < LEAST(ended_at, started_at + (interval '1 min' * duration)) + (interval '1 sec' * {delta_sec}) ) 
                            ) 
                        )
                    and duration > {duration}
                    and record_rate > {record_rate}
                    and started_at >= '{started_hour}'
                    and stream_id = {stream_id} 
                    and is_record_approved = True
                    and converted_to_mp3 = True
                    and is_deleted = False
                    and return_code=0
                    {sql_condition}
                    order by duration desc, started_at
                """
    print(sql)

    results={}

    for server_id in servers_order[stream_id] :
        result = get_records_from_server(server_id, sql, started_at, ended_at)
        if result:
            results[server_id] = result


    # # if Yurd FM, not need to improve internet version with 60 min duration (see below too)
    # if (is_non_recorded_period==False) and (stream_id==21) and (duration==60) :
    #     results={}
    #     print('  > No need to improve internet version for Yurd FM')
    
    if results :
        max_rate = 0
        max_server_id = -1
        for id, result in results.items() :
            # print(result)
            compare_rate =  result['record_rate'] * get_period_rate(result, started_at, ended_at) # result['duration'] 
            compare_rate = round(compare_rate, 2)
            print(f"       {result['id']}  {compare_rate=}  {result['path']}  {result['record_rate']}  {result['started_at'].strftime('%Y-%m-%d %H:%M:%S')}  {result['ended_at'].strftime('%Y-%m-%d %H:%M:%S')} ")
 
            if  compare_rate > max_rate : 
                max_rate = compare_rate
                max_server_id = id

        if max_rate > 0 :
            # # if Yurd FM, get internet version in case usual record (record w/o problem ) (see above too)
            # if (stream_id==21) and (6 in results) and (3 in results) and results[6]['duration']==60  and results[3]['duration']==61 :
            #     max_server_id = 6
            #     print('  >> Yurd FM: get internet version due to high quality')
            
            print('  >> get max result from db', max_server_id)
            result = results[max_server_id]

            status, imported_ids = copy_records(server_local_id, record_id, max_server_id, result, imported_ids, is_sync_mode)
        else:
            print('  > Can''t find records from any server with record_rate>0')
            status = 'no_find'


    else:
        print('  > Can''t find any records from another servers with more duration time')
        # n_no_find += 1
        status = 'no_find'
        
    return status, imported_ids





def start_record_processing(args, period_type):

    start_processing = datetime.now()
    print('\nSTARTED at ', start_processing)

    is_sync_mode = args.sync
    stream_id    = args.stream_id
    stream_type  = args.stream_type
    is_sync_mode = args.sync
    is_add_mode  = args.add_mode
    is_no_task   = args.no_task

    server_local_id = ut.get_parameter(db, 'server_number')

    servers_order = get_servers_order(stream_type)
    print('servers order :', servers_order)     
    print()    
    if not servers_order:
        print('Not defined servers order')
        return    

    if stream_type=='video':
        if (ut.get_parameter(db,f'is_video_processing') != 1) and (ut.get_parameter(db,f'is_band_processing') != 1):
            print(f"Current server is not process {stream_type} files and can't import {stream_type} records ")
            return
    else:     
        if ut.get_parameter(db,f'is_{stream_type}_processing') != 1 :
            print(f"Current server is not process {stream_type} files and can't import {stream_type} records ")
            return

    if (is_sync_mode) and (not is_no_task):
        task_id = ut.create_task(db, 'records_sync', is_check_running=True)        
        if task_id<0 :
            print('\n Another records sync process is running \n')
            return
    else:
        # no need create task for non-sync mode (it's test mode) and no tack mode
        task_id=-1


    sync_time_start = datetime.now()
    sync_time_end   = datetime.now()

    if period_type=='period' :
        sync_time_start = args.start_datetime
        sync_time_end   = args.end_datetime

    elif period_type=='auto' :
        
        if args.auto_days != None :
            dt1 = datetime.now() - timedelta(days=args.auto_days)
            dt2 = datetime.now() - timedelta(days=1)
            sync_time_start = ut.begin_of_day(dt1)
            sync_time_end   = ut.end_of_day(dt2)
        elif args.auto_hours != None :
            dt1 = datetime.now() - timedelta(hours=args.auto_hours)
            dt2 = datetime.now() - timedelta(hours=1)
            sync_time_start = ut.begin_of_hour(dt1)
            sync_time_end   = ut.end_of_hour(dt2)



    
    print('local_server      = ', server_local_id)
    print('task_id           = ', task_id)
    print('start sync time   = ', sync_time_start)
    print('end sync time     = ', sync_time_end)
    print('stream_id         = ', stream_id)
    print('stream_type       = ', stream_type)
    print('sync_mode         = ', is_sync_mode)

    sleep(5)




    sql_stream_id    = '' if stream_id<0 else f' and stream_id={stream_id} '
    sql_stream_type  = get_stream_type_sql(stream_type)


    sql = f"""select * from records 
                where 
                started_at > '{sync_time_start}' and started_at < '{sync_time_end}' 
                {sql_stream_id}
                {sql_stream_type}
                %s 
                order by started_at, stream_id
            """
            # 
    
    # if is_add_mode:
    #     sql_query = sql % " and is_record_approved=true and (duration<61 or record_rate<1) "
    # else:
    #     sql_query = sql % " and is_record_approved=true and (duration<61 or record_rate<1) and imported_record_id=0 "
        
    sql_query = sql % " and is_record_approved=true and (duration<61 or record_rate<1) "
    print(sql_query)
    records1 = db.select(sql_query)


    imported_records = db.select(sql % " and (is_record_approved=true or is_record_checked=true) and imported_record_id>0 ")
    
    imported_ids = {}
    for record in imported_records:
        imported_ids = add_record_to_imported(record['imported_source_id'], record['imported_record_id'], imported_ids)

    print('Already imported : ', imported_ids)



    # imported_ids = [record['imported_record_id'] for record in imported_records]



    print('\n\n------------------------------------------------------------------------------------------------------------------------------------')
    print(f'Start processing recorded periods at ', datetime.now())
    print('------------------------------------------------------------------------------------------------------------------------------------')
    
    print(f'\nStart processing {len(records1)} records')
    

    n = n_updated = n_no_need = n_no_find = n_no_success = 0
    nn = len(records1)

    ut.update_completion_percentage(db, task_id, 1)

    for record in records1:

        n+=1

        print('\n\n',f'process records : {n} of {nn}')
        print(' ', record['id'], ' ', record['path'], ' ', record['duration'], 'min  ', record['record_rate'], ' ', 
                                      record['started_at'].strftime("%Y-%m-%d %H:%M:%S"), '  ', record['ended_at'].strftime("%Y-%m-%d %H:%M:%S") )


        # print(record)
        if record['is_record_approved'] :
            status, imported_ids = sync_records_from_other_servers(stream_type, server_local_id, record, imported_ids, servers_order, False, is_sync_mode )
        else:
            # may be record is disabled by previous import
            print('  >> NO need process : record already disabled by previous import')
            status='no_need'

        if status=='updated':
            n_updated+=1
        elif status=='no_need':
            n_no_need+=1
        elif status=='no_find':
            n_no_find+=1
        elif status=='no_success':
            n_no_success+=1

        ut.update_completion_percentage(db, task_id, 50 * n/nn)


        # break      

    

    # add not recorded periods to searched period and search again
    
    recorded_periods, non_recorded_periods = get_recording_status_in_period(db, sync_time_start, sync_time_end, stream_type, stream_id)  
    print('\n\n------------------------------------------------------------------------------------------------------------------------------------')
    print(f'Start processing for non-recorded periods at ', datetime.now())
    print('------------------------------------------------------------------------------------------------------------------------------------')
    print(imported_ids)

    non_recorded_periods = sort_periods_by_start(non_recorded_periods)

    nn = nn + len(non_recorded_periods)

    for period in non_recorded_periods:
        n+=1
        # print(period)
        print('\n\n',f'process non_recorded_periods : {n}  of {nn}')
        start_process_time = datetime.now()
        print(' stream_id =', period['stream_id'], '  ', ut.get_stream_name_by_id(db,period['stream_id']), ' ', period['start'].strftime("%Y-%m-%d %H:%M:%S"), '  ', period['end'].strftime("%Y-%m-%d %H:%M:%S"), '  duration =', (period['end']-period['start']))

        if (period['end']-period['start']).total_seconds()<20 :
            status='no_need'
            print('  >> NOT NEED IMPORT')
        else:
            record = {  'id'            : -1,
                        'started_at'    : period['start'],
                        'ended_at'      : period['end'],
                        'duration'      : (period['end'] - period['start']).total_seconds()/60,
                        'stream_id'     : period['stream_id'],
                        'record_rate'   : 0
                        }
            status, imported_ids = sync_records_from_other_servers(stream_type, server_local_id, record, imported_ids, servers_order, True, is_sync_mode)

            if status == 'no_find' and is_add_mode:
                status, imported_ids = add_records_from_other_servers(stream_type, server_local_id, record, imported_ids, servers_order, is_sync_mode)
                
                
        if status=='updated':
            n_updated+=1
        elif status=='no_need':
            n_no_need+=1
        elif status=='no_find':
            n_no_find+=1
        elif status=='no_success':
            n_no_success+=1

        ut.update_completion_percentage(db, task_id, 100 * n/nn )

        print(f' process duration : {datetime.now()-start_process_time}')

        # break      

    ut.update_completion_percentage(db, task_id, 100)
    
        
    print('\n\n')

    print('Done!')    
    print('=============================================================')

    print('local_server      = ', server_local_id)
    print('task_id           = ', task_id)
    print('start sync time   = ', sync_time_start)
    print('end sync time     = ', sync_time_end)
    print('stream_id         = ', stream_id)
    print('stream_type       = ', stream_type)
    print('sync_mode         = ', is_sync_mode)
    print()


    print('Total records with problems  = ', n            )
    print('Updated records              = ', n_updated    )
    print('No need update records       = ', n_no_need    )
    print("Can't find records           = ", n_no_find    )
    print("No success sync              = ", n_no_success )

    print('=============================================================')

    print('STARTED   at ', start_processing)
    print('FINISHED  at ', datetime.now())
    print('DURATION  = ',  datetime.now() - start_processing )


    return


def valid_date_type(arg_date_str):
    """custom argparse *date* type for user dates values given from the command line"""
    try:
        return datetime.strptime(arg_date_str, "%Y-%m-%d")
    except ValueError:
        msg = "Given Date ({0}) not valid! Expected format, YYYY-MM-DD!".format(arg_date_str)
        raise argparse.ArgumentTypeError(msg)
        
def valid_datetime_type(arg_datetime_str):
    """custom argparse type for user datetime values given from the command line"""
    try:
        return datetime.strptime(arg_datetime_str, "%Y-%m-%d %H:%M")
    except ValueError:
        msg = "Given Datetime ({0}) not valid! Expected format, 'YYYY-MM-DD HH:mm'!".format(arg_datetime_str)
        raise argparse.ArgumentTypeError(msg)        



def check_stream_type(value):
    if value not in ('audio','video'):   # value!='all' and 
        raise argparse.ArgumentTypeError(
            'stream type - `audio` or `video` ')
    else:
        return value



def main():

    parser = argparse.ArgumentParser(description='This program sync records to local(current) server from other servers according import orders') 

    subparsers = parser.add_subparsers(help='sub-command help')

    parser_period = subparsers.add_parser('period', help='Define exact period')    

    parser_period.add_argument('-start', '--start',
                        dest     = 'start_datetime',
                        type     = valid_datetime_type,
                        default  = None,
                        required = True,
                        help     = 'start datetime in format "YYYY-MM-DD HH:mm"')

    parser_period.add_argument('-end', '--end',
                        dest     = 'end_datetime',
                        type     = valid_datetime_type,
                        default  = None,
                        required = True,
                        help     = 'end datetime in format "YYYY-MM-DD HH:mm"')

    parser_auto = subparsers.add_parser('auto', help='Define auto period')

    group = parser_auto.add_mutually_exclusive_group(required=True)

    group.add_argument('-days', '--days',
                        dest     = 'auto_days',
                        type     = int,
                        default  = None,
                        help     = 'set days before for auto period')
 
    group.add_argument('-hours', '--hours',
                        dest     = 'auto_hours',
                        type     = int,
                        default  = None,
                        help     = 'set hours before for auto period')




    parser.add_argument('-stream_type', '--stream_type',
                        dest='stream_type',
                        type=check_stream_type,
                        # default='all',
                        required=True,
                        help='stream type - `audio` or `video` ')  
    
    parser.add_argument('-stream_id', '--stream_id',
                        dest='stream_id',
                        type=int,
                        default=-1,
                        required=False,
                        help='sync only stream with id')

    parser.add_argument('--sync', action='store_true', 
                        help="sync mode : update target database and copy files")

    parser.add_argument('--add_mode', action='store_true', 
                        help="add mode : add all records from another servers ")

    parser.add_argument('--no_task', action='store_true', 
                        help="no task mode : don't create task for sync process and not check other sync process")


    if len(sys.argv) < 3:
        print('Error: No argument specified.\n')
        parser.print_help()
        sys.exit(1)


    args = parser.parse_args()

    if 'period' in sys.argv :
        period_type = 'period'
    elif 'auto' in sys.argv :
        period_type = 'auto'
    else :
        period_type = ''


    print(sys.argv)
    print(args)

    
    start_record_processing(args, period_type)    

    return




if __name__ == '__main__':
    main()