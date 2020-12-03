# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.incubator.apache.org/tutorial.html)
"""
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'bi-team',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2021, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    'PoC-bi-team',
    default_args=default_args,
    description='PoC',
    schedule_interval=timedelta(days=1),
)

# create_bi_table, load_into_bi_table, duplicate_check and export_bi_table are examples of tasks created by instantiating operators
create_bi_table = BashOperator(
    task_id='create_bi_table',
    bash_command='sleep 5',
    dag=dag,
)



load_into_bi_table = BashOperator(
    task_id='load_into_bi_table',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag,
)



duplicate_check = BashOperator(
    task_id='duplicate_check',
    depends_on_past=False,
    bash_command='sleep 10',
    retries = 0,
    dag=dag,
)

export_bi_table = BashOperator(
    task_id='export_bi_table',
    depends_on_past=True,
    bash_command='sleep 15',
    dag=dag,
)
# downstream can be replaced with bits like >>, upstream is analog
create_bi_table >> duplicate_check
duplicate_check >> load_into_bi_table
load_into_bi_table >> duplicate_check
duplicate_check >> export_bi_table
