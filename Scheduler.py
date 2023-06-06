# Databricks notebook source

if __package__:
  from ara.pyspark import spark
  from ara.scheduler.DatabricksJobsService import DatabricksJobsService, RunTask
  from ara.common.DeltaLakeManagement import DeltaLakeManagement
  from ara.scheduler.SchedulerComponent import SchedulerComponent
  from ara.common.AutoLoaderNotificationService import AutoLoaderNotificationService
  from ara.scheduler.SchedulerState import SchedulerState
  from ara.common.DataPipelineContext import DataPipelineContext
  from ara.common.Configuration import Configuration
  from ara.utils.GroupFitter import GroupFitter
  from ara.utils.Stopwatch import Stopwatch
  from ara.scheduler.ClusterReuse import ClusterReuse

# COMMAND ----------



import os
import logging
import time
import dask
import yaml

from functools import lru_cache
from datetime import datetime, timedelta
from typing import Dict, AbstractSet, Optional
from collections import OrderedDict
from pyspark.sql.types import StructType, StructField, StringType, TimestampType



from delta.tables import DeltaTable


# COMMAND ----------

class Scheduler:
  @property
  def error_penalties(self) -> list:
    return self.cfg['error_penalties']

  @property
  @lru_cache
  def endpoints_limits(self):
    return self.get_endpoints_limits()
  
  @property
  @lru_cache
  def pipelines_endpoint_fit_cost(self):
    return self.get_pipelines_endpoint_fit_cost()

  @property
  def upstream_dependency_wait_depth(self):
    return self.cfg['upstream_dependency_wait_depth']

  def __init__(self):
    self.djs = DatabricksJobsService()
    self.job_starttime = datetime.now()
    self.logger = logging.getLogger(__name__)

    self._next_technical_tables_optimize = datetime.now() + timedelta(minutes=10)
    self.state: Optional[SchedulerState] = None
    self.active_run_tasks: AbstractSet[RunTask] = set()
    self.active_cluster_pipelines: Dict[str, AbstractSet[RunTask]] = {}
    self.active_reuse_clusters: Dict[str, RunTask] = {}
    self._scheduler_components: Dict[str, SchedulerComponent] = {}
    self._pipeline_fingerprint_map: Dict[str, tuple] = {}
    self._pipeline_context: Dict[str, DataPipelineContext] = {}
    self.cfg = self.get_scheduler_configuration()
    self._queue_using_autoloader_data = None
    self._queue_using_autoloader_data_cached = {}
    self._queue_using_dependencies_data = None
    self._not_to_run_pipelines = None
    self._queue_using_unresolved_dependencies_data = None
    self.logger.debug(yaml.dump({"Scheduler configuration": self.cfg}))
    self._pipelines_scheduling_priorities = {}
    self._upstream_dependency_wait_depth_map = {}
    self._cluster_reuse_handler = ClusterReuse()
    self.heartbeat_setup_functions = []
    self.heartbeat_callbacks = []
    self._batch_time = None
    
    self._queued_not_run_pipelines = []
    self._queued_not_run_pipelines_monitor = {}
    self._delayed_pipelines_runs = {}
    self._not_queued_pipelines = {}
    self._run_pipelines = {}

    self._queued_not_run_pipelines_monitor_opt_runs = 0
    self._running_tasks_on_ui_cluster = {}
    self._running_tasks_on_ui_cluster_id = {}

  def run(self):
    """
    This function is responsible for running tasks. 
    - It starts by requesting a lock to terminate the scheduler, then it tries to acquire a heartbeat scheduler lock. 
    - If the termination request has been activated, it will terminate. Otherwise, it will load the config from pipelines and get the pipelines scheduling priorities, upstream dependency wait depth map, etc.
    - Then it enters a loop which periodically checks if the termination lock has been activated. 
      - If not, it will acquire an exclusive lock, sync pipelines and active run tasks, 
        queue errors, queue using cron triggers, queue using autoloader queues, queue using dependencies and unresolved dependencies, monitor run tasks and start new tasks. 
      - It also calls a heartbeat callback before sleeping for the configured loop time. 
      - Finally, it optimizes internal tables and downloads data.
    """
    termination_request_lock = SchedulerState.get_scheduler_termination_request_distributed_lock()
    self.logger.debug('Requesting scheduler to terminate... (it make take a while)')
    termination_request_lock.acquire()
    
    self.logger.debug('Trying to acquire heartbeat scheduler lock...')
    with SchedulerState.get_scheduler_heartbeat_distributed_lock():
      termination_request_lock.release()
      self.logger.debug('Lock Acquired...')
      self.call_heartbeat_setup()
      #why do it here? loading config pipelines takes a while... 
      #so if there is lock placed, let's get out early
      if termination_request_lock.locked():
        self.logger.debug('Termination lock request has been activated. Terminating.')
        return

      self._load_config_from_pipelines()
      self._clear_scheduler_logs()
      self._pipelines_scheduling_priorities = self._get_pipelines_scheduling_priorities()
      self._upstream_dependency_wait_depth_map = self._get_upstream_dependency_wait_depth_map(self.upstream_dependency_wait_depth)
      last_state_delta_version = None
      stop_watch = None

      # clear scheduler logs that exceed the configured retention period
      
      
      while True:
        state_tainted = False
        
        #periodically check if termination lock is not placed
        if termination_request_lock.locked():          
          self.logger.debug('Termination lock request has been activated. Terminating.')
          return

        #acquire exclusive lock
        self.state = SchedulerState()
        with self.state:
          if self.state.state_table_delta_version != last_state_delta_version:
            self.logger.debug("Scheduler state was externally changed... syncing...")
            state_tainted = True
            self._sync_pipelines()
            self._sync_active_run_tasks()
            self._queue_using_autoloader_data = None
            self._queue_using_dependencies_data = None
            self._queue_using_unresolved_dependencies_data = None

          self._batch_time = datetime.now()            
          self._queue_errors()
          self._queue_using_cron_triggers()
          self._queue_using_autoloader_queues()
          self._queue_using_dependencies()
          self._queue_using_unresolved_dependencies()

          self._monitor_run_tasks()
          self._get_running_tasks_on_ui_cluster()
          self._start_new_tasks()
          self._write_scheduler_logs()

          self.call_heartbeat_callback()

        last_state_delta_version = self.state.state_table_delta_version
        
        if stop_watch:
          t = stop_watch.get_time()
          #sleep remaining time
          if not state_tainted and t < 0:
            time.sleep(-t)
        
        stop_watch = Stopwatch.start_new(start_time_offset=self.cfg['loop_time'])

        self._optimize_internal_tables()
        
        self.logger.debug("downloading data...")
        dask.compute([
            dask.delayed(self._queue_using_dependencies_data_download)()
            ,dask.delayed(self._queue_using_autoloader_queues_data_download)()
            ,dask.delayed(self._queue_using_unresolved_dependencies_data_download)()
        ])
        self.logger.debug("data download complete")


  def _write_scheduler_logs(self):
    """
    This method writes the scheduler logs to the configured location.
    It includes the following logs:
    - queued_not_run_pipelines
    - not_queued_pipelines
    - run_pipelines
    """
    if not self.cfg['monitor']['enabled']:
      return

    # Get the metadata paths for the logs
    metadata_paths = [
        os.path.join(self.cfg['monitor']['location'], sub_path)
        for sub_path in ['queued_not_run_pipelines', 'not_queued_pipelines', 'run_pipelines']
    ]

    # Write the queued_not_run_pipelines logs
    queued_not_run_logs = list(self._delayed_pipelines_runs.values()) + list(self._queued_not_run_pipelines_monitor.values())
    if queued_not_run_logs:
      spark.createDataFrame(
          queued_not_run_logs,
          'ara_pipeline_name:string, batch_time:timestamp, reason:string, scheduled_priority:integer, total_queue_len:integer, pipeline_pos_in_queue:integer, additional_info:string, status:string'
      ).write.format('delta').mode('append').option('mergeSchema', 'true').save(metadata_paths[0])
      self._queued_not_run_pipelines_monitor.clear()
      self._delayed_pipelines_runs.clear()

    # Write the not_queued_pipelines logs
    not_queued_logs = self._not_queued_pipelines.values()
    if not_queued_logs:
      spark.createDataFrame(
          not_queued_logs,
          'ara_pipeline_name:string, batch_time:timestamp, queue_func:string, reason:string, additional_info:string, status:string'
      ).write.format('delta').mode('append').option('mergeSchema', 'true').save(metadata_paths[1])
      self._not_queued_pipelines.clear() 

    run_pipelines_logs = self._run_pipelines.values()
    if run_pipelines_logs:
      spark.createDataFrame(
          run_pipelines_logs,
          'ara_pipeline_name:string, batch_time:timestamp, additional_info:string, status:string'
      ).write.format('delta').mode('append').option('mergeSchema', 'true').save(metadata_paths[2])
      self._run_pipelines.clear()
    
  def _clear_scheduler_logs(self):
    """
    This method is used to clear scheduler logs that exceed the configured retention period.
    """
    if not self.cfg['monitor']['enabled']:
      return
    try:
      log_paths = DeltaLakeManagement.get_delta_tables_from_path(self.cfg['monitor']['location'])
    except:
      return
    retention_period = self.cfg['monitor']['retention']
    for log_path in log_paths:
      log_path = log_path.replace('dbfs:', '')
      try:
        spark.sql("DELETE FROM delta.`%s` WHERE batch_time < date_add(current_date(), -%s)", (log_path, retention_period))
      except Exception as e:
        self.logger.error(f"Failed to delete logs from {log_path}. Error: {str(e)}")
        
  def _get_running_tasks_on_ui_cluster(self):
    """
    This method is used to get the number of running tasks on each UI cluster. 
    It is used to determine if a cluster context is available for a pipeline.
    """
    self._running_tasks_on_ui_cluster = {}
    self._running_tasks_on_ui_cluster_id = {}
    for run_task in self.active_run_tasks:
      if run_task.is_running and self.djs.get_cluster_by_id(run_task.cluster_id).get('cluster_source') == 'UI':
        cluster_name = self.djs.get_cluster_by_id(run_task.cluster_id).get('cluster_name')
        if cluster_name not in self._running_tasks_on_ui_cluster:
          self._running_tasks_on_ui_cluster[cluster_name] = 1
          self._running_tasks_on_ui_cluster_id[cluster_name] = run_task.cluster_id
        else:
          self._running_tasks_on_ui_cluster[cluster_name] += 1

  def _get_idle_reuse_clusters_map(self):
    
    idle_cluster_ids = self.active_reuse_clusters.keys() - self.active_cluster_pipelines.keys()
    
    data = {}
    for cluster_id in idle_cluster_ids:
      fp = self._cluster_reuse_handler.get_fingerprint_for_cluster(cluster_id)
      data[fp] = data.get(fp, set())
      data[fp].add(cluster_id)

    return data

  def _load_config_from_pipelines(self):
    self.logger.debug("Loading pipelines configuration... (it make take a while)")
    self._scheduler_components: Dict[str, SchedulerComponent] = {}

    if Configuration.get_git_branch():
      raise NotImplementedError("Scheduler is not yet capable of running of a git branch configuration")

    configs = [
      x.asDict(True) 
      for x in spark.sql('''

      select ara_pipeline_name, settings
      from metadata.pipeline_config
      where size(settings.scheduler.triggers) > 0

      ''').collect()
    ]

    self._pipeline_context = {
      cfg['ara_pipeline_name']: DataPipelineContext.from_dict(
          cfg['ara_pipeline_name']
          ,{
            'settings': cfg['settings']
          }
        )
      for cfg in configs
    }

    self._scheduler_components = {
      ara_pipeline_name: SchedulerComponent(ctx)
      for ara_pipeline_name, ctx in self._pipeline_context.items()
    }

  def _sync_pipelines(self):
    ##
    # add new pipelines
    ##
    for name, _ in self._scheduler_components.items():
      self.state.create_if_does_not_exist(name)
      
    ##
    # delete not used setup
    ##
    to_delete = []

    for name, _ in self.state.items():
      if name not in self._scheduler_components:
        to_delete.append(name)

    for name in to_delete:
      self.state.delete(name)

  def _add_pipeline_or_cluster_reuse_task(self, r: RunTask):
    if r.is_cluster_reuse_keep_alive_task:
      self.active_reuse_clusters[r.cluster_id] = r
    
    if r.ara_pipeline_name:
      pipelines = self.active_cluster_pipelines.get(r.cluster_id, set())
      pipelines.add(r)
      self.active_cluster_pipelines[r.cluster_id] = pipelines

  def _remove_pipeline_or_cluster_reuse_task(self, r: RunTask):
    if r.is_cluster_reuse_keep_alive_task:
      self.active_reuse_clusters.pop(r.cluster_id, None)
    
    if r.ara_pipeline_name:
      pipelines = self.active_cluster_pipelines.get(r.cluster_id)
      if pipelines is not None:
        pipelines.discard(r)

        if len(pipelines) == 0:
          self.active_cluster_pipelines.pop(r.cluster_id, None)

  def _sync_active_run_tasks(self):
    self.active_run_tasks = set()

    for _, s in self.state.items():
      #verify state of non terminal statuses
      if s['dbr_run_state'] == 'RUNNING' and s['dbr_run_id']:
        r = RunTask(s['dbr_run_id'])
        self.active_run_tasks.add(r)
        self._add_pipeline_or_cluster_reuse_task(r)

    for r in self.djs.get_run_tasks(active_only=True):
      if (r.ara_pipeline_name or r.is_cluster_reuse_keep_alive_task) and r not in self.active_run_tasks:
        self.active_run_tasks.add(r)
        self._add_pipeline_or_cluster_reuse_task(r)

  @classmethod
  def _get_upstream_dependency_wait_depth_map(cls, depth):
    return { 
      x[0]: set(x[1]) 
      for x in spark.sql(f"""

      select node, collect_set(depends_on_node)
      from metadata.lineage_upstream_dependency
      where depends_on_node like 'pipelines/%'
      and node like 'pipelines/%'
      and depth <= {depth*2}
      group by node

    """).collect() 
    }

  @classmethod
  def _get_pipelines_scheduling_priorities(cls):
    """
    This classmethod is used to get pipelines scheduling priorities.
    The priorities are computed using SQL statement:
    - it selects several columns and computing a priority ranking for each row. The columns being selected are:
      - d.ara_pipeline_name: The name of a pipeline
      - depth: The maximum depth of upstream dependencies for a pipeline
      - direct_downstream_pipelines: A list of direct downstream pipelines for a pipeline
      - dense_rank() OVER (ORDER BY depth desc, direct_downstream_pipelines) as priority: A priority ranking computed using the dense_rank() window function, which assigns a unique rank to each row based on the values of the depth and direct_downstream_pipelines columns. 
    
    Returns:
    - A dictionary of pipeline names (key) and their priorities (value)
    """
    df = spark.sql("""
      select 
        d.ara_pipeline_name
        ,depth
        ,direct_downstream_pipelines
        ,dense_rank() OVER (
          ORDER BY depth desc, direct_downstream_pipelines
        ) as priority
      from (
        select 
          depends_on_node as ara_pipeline_name
          ,max(depth) as depth
        from metadata.lineage_upstream_dependency
        where depends_on_node like 'pipelines/%'
        group by depends_on_node
        order by depth desc
      ) as d
      left join (
        select
          depends_on_node as ara_pipeline_name
          ,collect_set(node) as direct_downstream_pipelines
        from metadata.lineage_upstream_dependency
        where depth = 2
        and depends_on_node like 'pipelines/%'
        group by depends_on_node
      ) as p
      on d.ara_pipeline_name = p.ara_pipeline_name
      order by depth desc, direct_downstream_pipelines
    """)

    return { r.ara_pipeline_name: r.priority for r in df.collect() }

  
  
  def _queue_errors(self):
    """
    This method is used to queue pipelines that are in ERROR state.
    It checks if the pipeline is in ERROR state and if the next run timestamp is in the past. If both conditions are met, the pipeline is queued.
    
    """
    self.logger.debug("queue errors...")
    for _, s in self.state.items():
      if not s['enabled']:
        continue

      if s['dbr_run_state'] == 'ERROR':
        if datetime.now() >= (s['error_next_run_ts'] or datetime(1900, 1, 1)):
          self.state.enqueue(s, 'error handling')
        else:
          self._not_queued_pipelines[s['ara_pipeline_name']] = (s['ara_pipeline_name'], 
                                                     self._batch_time, 
                                                     '_queue_errors',
                                                     'error penalty', 
                                                     f'next run: {s["error_next_run_ts"]}, error count: {s["error_count"]}',
                                                     'not queued'
                                                     )

  def _queue_using_cron_triggers(self):
    """
    This method is used to queue pipelines that are in SUCCESS state and have a cron trigger.
    It checks if the pipeline is in SUCCESS state and if the next run timestamp is in the past. If both conditions are met, the pipeline is queued.    
    """
    self.logger.debug("queue using cron...")
    for name, s in self.state.items():
      if not s['enabled']:
        continue
      
      if s['dbr_run_state'] in [None, 'SUCCESS']:
        if s['next_run_ts'] is None:
          t = self._scheduler_components[name].get_next_trigger_timestamp()
          if t:
            s['next_run_ts'] = t
            self.state.update(s, reason='cron trigger')
          else:
            continue

        #TODO: handle the timezone stuff...
        if datetime.now() >= s['next_run_ts']: 
          self.state.enqueue(s, 'cron trigger')

  def _queue_using_unresolved_dependencies_data_download(self):
    self._queue_using_unresolved_dependencies_data = [
      x.ara_pipeline_name
      for x in spark.sql("""
          SELECT distinct explode(waiting_for.ara_pipeline_name) as ara_pipeline_name
          FROM metadata.scheduler_dependency_existance_check
          WHERE source_format = 'delta'
          AND source_engine = 'spark'
          AND exist_table_count <> table_count
          AND ara_pipeline_name IN (
            SELECT distinct ara_pipeline_name
            FROM metadata.pipeline_scheduler_triggers_flattened
          )
        """).collect()
      ]

  def _queue_using_unresolved_dependencies(self):
    """
    This method is used to queue pipelines that are in None state and have unresolved dependencies.
    It checks if the pipeline is in None state and if it has unresolved dependencies. If both conditions are met, the pipeline is queued.
    """
    if self._queue_using_unresolved_dependencies_data:
      for name in self._queue_using_unresolved_dependencies_data:
        current_state = self.state.get(name)

        if not current_state or not current_state['enabled']:
          continue

        if current_state['dbr_run_state'] is None:
          self.state.enqueue(current_state, 'unresolved dependencies resolution')

  def _queue_using_autoloader_queues_data_download(self):
    if not self._queue_using_autoloader_data_cached:
      self._queue_using_autoloader_data_cached['services'] = AutoLoaderNotificationService.get_ara_pipelines_autoloader_services()

    self._queue_using_autoloader_data = AutoLoaderNotificationService.get_ara_pipelines_autoloader_queue_state(
      self._queue_using_autoloader_data_cached['services']
    )

  def _queue_using_autoloader_queues(self):
    """
    This method is used to queue pipelines that are in SUCCESS state and have data in their autoloader queues or staging area has data.
    It checks if the pipeline is in SUCCESS state and if the autoloader queue or staging area has data. If both conditions are met, the pipeline is queued.
    """
    if not self._queue_using_autoloader_data:
      return
      
    self.logger.debug("queue using autoloader...")
    
    for q in self._queue_using_autoloader_data:
      if q['has_data'] or q['stg_has_data'] or not q['exists']:
        current_state = self.state.get(q['ara_pipeline_name'])

        if not current_state or not current_state['enabled']:
          continue

        if self._are_upstream_dependencies_active(q['ara_pipeline_name']):
          dependencies = {d: self._is_pipeline_active(d) for d in self._upstream_dependency_wait_depth_map.get(q["ara_pipeline_name"])}
          self._not_queued_pipelines[q['ara_pipeline_name']] = (q['ara_pipeline_name'], 
                                                     self._batch_time, 
                                                     '_queue_using_autoloader_queues',
                                                     '_are_upstream_dependencies_active', 
                                                     f'dependencies: [{dependencies}]',
                                                     'not queued'
                                                     )
          continue

        if current_state['dbr_run_state'] in [None, 'SUCCESS']:
          self.state.enqueue(current_state, 'autoloader queue event')

  def _queue_using_dependencies_data_download(self):
    current_success = self.state.as_state_df()
    current_success.createOrReplaceTempView('dep_prev_state')
    
    def _dl_dependencies_data():
      self._queue_using_dependencies_data = [r.asDict(True) for r in spark.sql("""
        SELECT 
          ara_pipeline_name
          ,earliest_consumer_timestamp
          ,curr_dep_state
          ,prev_dep_state
          ,CASE 
            WHEN prev_dep_state is NULL THEN curr_dep_state 
            ELSE array_distinct(array_union(array_except(curr_dep_state, prev_dep_state), array_except(prev_dep_state, curr_dep_state))) 
          END AS diff_dep_state      
        FROM (
          SELECT 
            c.ara_pipeline_name
            ,min(c.consumer_timestamp) AS earliest_consumer_timestamp
            ,collect_set(struct(c.*)) AS curr_dep_state
            ,first(p.state.prev_dep_state) AS prev_dep_state
          FROM metadata.scheduler_dataset_consumption_vs_production AS c
          LEFT JOIN dep_prev_state AS p
          ON p.ara_pipeline_name = c.ara_pipeline_name
          WHERE
            c.producer_is_ahead == 1
            AND c.ara_pipeline_name IN (
              SELECT DISTINCT ara_pipeline_name
              FROM metadata.pipeline_scheduler_triggers_flattened
              WHERE dependency = 1
            )
            AND c.source_dep_trigger = 1
          GROUP BY c.ara_pipeline_name
          ORDER BY earliest_consumer_timestamp
        )
      """).filter("size(diff_dep_state) > 0").collect()]

    def _dl_not_run():
      self._not_to_run_pipelines = set([
        x.ara_pipeline_name for x in spark.sql('''
          SELECT ara_pipeline_name
          FROM metadata.scheduler_dependency_existance_check
          WHERE exist_table_count <> table_count
          AND source_format = 'delta'
          AND source_engine = 'spark'
          CLUSTER BY ara_pipeline_name
        ''').collect()
        ])

    dask.compute(dask.delayed(_dl_dependencies_data)(), dask.delayed(_dl_not_run)())

  def _queue_using_dependencies(self):
    if not self._queue_using_dependencies_data:
      return

    self.logger.debug("queue using dependencies...")
    for r in self._queue_using_dependencies_data:
      current_state = self.state.get(r['ara_pipeline_name'])

      if not current_state or not current_state['enabled']:
        continue

      if self._are_upstream_dependencies_active(r['ara_pipeline_name']):
        dependencies = {d: self._is_pipeline_active(d) for d in self._upstream_dependency_wait_depth_map.get(r["ara_pipeline_name"])}
        self._not_queued_pipelines[r['ara_pipeline_name']] = (r['ara_pipeline_name'], 
                                                     self._batch_time, 
                                                     '_queue_using_dependencies',
                                                     '_are_upstream_dependencies_active', 
                                                     f'dependencies: [{dependencies}]',
                                                     'not queued'
                                                     )
        continue
      
      if current_state['dbr_run_state'] in [None, 'SUCCESS']:
        
        if current_state['dbr_run_state'] == 'SUCCESS':
          if self.cfg['dependency_trigger_minimum_interval'] > 0:
            location = self.cfg['monitor']['location'] + '/scheduler_dependency_trigger_monitor'
            self._create_dependency_trigger_interval_monitor_table(location)
            last_success_timestamp = self._get_datapipeline_last_success_timestamp(r['ara_pipeline_name'], location)
            if last_success_timestamp:
              if not isinstance(last_success_timestamp, datetime):
                # Convert the timestamp string to a datetime object (assuming it's in the format 'YYYY-MM-DD HH:MM:SS')
                last_success_timestamp = datetime.strptime(last_success_timestamp, '%Y-%m-%d %H:%M:%S')
              time_gap = (datetime.now() - last_success_timestamp).total_seconds() 
              if time_gap < self.cfg['dependency_trigger_minimum_interval']:
                self._not_queued_pipelines[r['ara_pipeline_name']] = (r['ara_pipeline_name'], 
                                                      self._batch_time, 
                                                      '_queue_using_dependencies',
                                                      'dependency_trigger_minimum_interval', 
                                                      f'last_success_timestamp: {last_success_timestamp}, dependency_trigger_minimum_interval: {self.cfg["dependency_trigger_minimum_interval"]}, current_interval: {time_gap}',
                                                      'not queued'
                                                      )
                continue
              else:
                self._not_queued_pipelines[r['ara_pipeline_name']] = (r['ara_pipeline_name'], 
                                                      self._batch_time, 
                                                      '_queue_using_dependencies',
                                                      'dependency_trigger_minimum_interval', 
                                                      f'last_success_timestamp: {last_success_timestamp}, dependency_trigger_minimum_interval: {self.cfg["dependency_trigger_minimum_interval"]}, current_interval: {time_gap}',
                                                      'queued'
                                                      )
                self._update_datapipeline_last_success_timestamp(r['ara_pipeline_name'], self._batch_time, location)
        
        current_state['state']['curr_dep_state'] = r['curr_dep_state']
        current_state['state']['prev_dep_state'] = r['prev_dep_state']
        current_state['state']['diff_dep_state'] = r['diff_dep_state']

        self.state.enqueue(current_state, 'dependency trigger v2') 

  def _create_dependency_trigger_interval_monitor_table(self, location):
    """
    Create a monitor table for dependency trigger minimum interval. If not existing, create it.
   
    """
    if DeltaTable.isDeltaTable(spark, location):
      return
    schema = StructType([
        StructField('ara_pipeline_name', StringType()),
        StructField('last_success_timestamp', TimestampType()),
        StructField('last_operation', StringType()),
        StructField('last_operation_timestamp', TimestampType())
      ])
    df = spark.createDataFrame([], schema)
    df.write.format('delta').mode('overwrite').save(location)
  
  def _get_datapipeline_last_success_timestamp(self, ara_pipeline_name, location):
    """
    Get the last success timestamp of a datapipeline.
   
    """
    df = spark.read.format('delta').load(location)
    df = df.filter(f"ara_pipeline_name == '{ara_pipeline_name}'")
    if df.limit(1).count() == 0:
      #insert a new record with current timestamp
      spark.sql(f"insert into delta.`{location}` values('{ara_pipeline_name}', '{self._batch_time}', 'insert', '{datetime.now()}')")
      return 
    return df.collect()[0]['last_success_timestamp']
  
  def _update_datapipeline_last_success_timestamp(self, ara_pipeline_name, last_success_timestamp, location):
    """
    Update the last success timestamp of a datapipeline.
   
    """
    try:
      spark.sql(f"update delta.`{location}` set last_success_timestamp = '{last_success_timestamp}', last_operation = 'update', last_operation_timestamp = '{datetime.now()}' where ara_pipeline_name = '{ara_pipeline_name}'")
    except Exception as e:
      print(f"Error updating Delta table at {location}: {str(e)}")
    
  def _monitor_run_tasks(self):
    self.logger.debug("monitor run tasks...")
    if not self.active_run_tasks:
      return

    finished, self.active_run_tasks = RunTask.wait_for_all(self.active_run_tasks, timeout=0, raise_on_error=False)

    for r in finished:
      self.logger.debug(f">>> {r}")

      self._remove_pipeline_or_cluster_reuse_task(r)
      if r.ara_pipeline_name:
        self._run_pipelines[r.ara_pipeline_name] = (r.ara_pipeline_name, self._batch_time, r.job_def, 'task run complete')
      # else:
      #   self._run_clusters[r.run_name] = (r.run_name, self._batch_time, 'task run complete')
      
      if r.ara_pipeline_name:
        s = self.state.get(r.ara_pipeline_name)
        c = self._scheduler_components[r.ara_pipeline_name]

        s = SchedulerState._update_state_dict_from_run_task(s, r)

        if r.is_success:
          s['error_count'] = 0
          s['error_next_run_ts'] = None
          s['next_run_ts'] = c.get_next_trigger_timestamp()
          s['state']['prev_dep_state'] = s['state']['curr_dep_state']

        if r.is_error:
          s['error_count'] = (s['error_count'] or 0) + 1
          s['error_next_run_ts'] = self._get_error_next_run(s['error_count'])
          s['next_run_ts'] = None
          
        self.state.update(s, reason='run task completed')

  def _is_pipeline_active(self, ara_pipeline_name):
    state = self.state.get(ara_pipeline_name)

    return state is not None and state['dbr_run_state'] in [ 'RUNNING', 'QUEUED' ]

  def _are_upstream_dependencies_active(self, ara_pipeline_name):
    deps = self._upstream_dependency_wait_depth_map.get(ara_pipeline_name)

    return deps is not None and any(self._is_pipeline_active(d) for d in deps)

  def _get_error_next_run(self, error_count):
    if error_count < 1:
      raise ValueError('error count must be positive')

    idx = error_count - 1
    if idx >= len(self.error_penalties):
      idx = len(self.error_penalties) - 1

    penalty_seconds = self.error_penalties[idx]

    return datetime.now() + timedelta(seconds=penalty_seconds)

  def _start_new_tasks(self):
    """
    This function is responsible for starting new run tasks. 
    - It starts by building a list of queued tasks, sorted by pipeline depth and grouped by similar downstream pipelines. 
    - It then builds a fitter (limit justification) based on currently running pipelines and idle reuse clusters.
    - Then it iterates through the queued tasks, checks if the pipeline is enabled, if it's streaming, if the cron triggers are in range, and if it fits the endpoint limits. 
    - If these conditions are met, it runs the task as a run task asynchronously and updates the state dict with the results of the run task.
    """
    if self._not_to_run_pipelines is None:
      return

    self.logger.debug("start new run tasks...")
    
    #self._queued_not_run_pipelines_monitor_opt_runs += 1

    #build queued tasks, using priorities based on depth of pipeline, grouped by similar direct downstrem pipelines
    queued = [ 
      s
      for name, s in self.state.items()
      if s['dbr_run_state'] == 'QUEUED'
    ]

    queued = sorted(queued, key=lambda d: self._pipelines_scheduling_priorities.get(d['ara_pipeline_name'], 99999))
    
    if self._queued_not_run_pipelines:
      self._queued_not_run_pipelines = list(OrderedDict.fromkeys(self._queued_not_run_pipelines).keys())
      queued_not_run_dict = {s['ara_pipeline_name']: s for s in queued if s['ara_pipeline_name'] in self._queued_not_run_pipelines}
      queued_not_run = [queued_not_run_dict[name] for name in queued_not_run_dict]
      queued = queued_not_run + [s for s in queued if s not in queued_not_run]
      
    #build fitter, based on currently running pipelines
    fitter = GroupFitter(self.endpoints_limits)
    for r in self.active_run_tasks:
      if r.ara_pipeline_name:
        endpoint_cost = self.pipelines_endpoint_fit_cost.get(r.ara_pipeline_name)
        #streaming pipelines do not have endpoint cost associated with them
        if not endpoint_cost:
          continue

        fitter.add(r.ara_pipeline_name, endpoint_cost, force=True)

    #build idle reuse clusters
    idle_reuse_clusters = self._get_idle_reuse_clusters_map()

    self.logger.debug(f"Available idle reuse clusters: {idle_reuse_clusters}")
    
    self._delayed_pipelines_runs = {}
    for s in queued:
      ara_pipeline_name = s['ara_pipeline_name']
      pipeline_ctx = self._pipeline_context[ara_pipeline_name]

      if not s['enabled']:
        continue

      #batch pipelines's can be blocked by other pipelines not yet being run
      if (not pipeline_ctx.streaming_enabled) and (ara_pipeline_name in self._not_to_run_pipelines):
        continue

      sched_component = self._scheduler_components[ara_pipeline_name]
     
      endpoint_cost = self.pipelines_endpoint_fit_cost.get(ara_pipeline_name)

      #this is not necessary for streaming with microbatch doesn't be managed by scheduler
      #and streaming with once trigger can be managed as batch process
      #if pipeline_ctx.streaming_enabled:
        ### we do not check endpoint fits, streaming has no limits :)
        ### check if crop triggers are in range, if not, dont's start
        #None => there are no cron triggers
        #False => none of triggers in range
        #True => at least one in range
      #  if sched_component.is_timestamp_in_cron_trigger_range(datetime.now()) is False:
      #    continue
      
      #does not fit the endpoint limits, skip it
      if endpoint_cost and not fitter.add(ara_pipeline_name, endpoint_cost):
        self._queued_not_run_pipelines.append(ara_pipeline_name) if ara_pipeline_name not in self._not_to_run_pipelines else None
        self._queued_not_run_pipelines_monitor[ara_pipeline_name] = (ara_pipeline_name, 
                                                     self._batch_time, 
                                                     'endpoint limits', 
                                                     self._pipelines_scheduling_priorities.get(ara_pipeline_name, 99999),
                                                     len(queued),
                                                     queued.index([p for p in queued if p.get('ara_pipeline_name') == ara_pipeline_name][0]),
                                                     f'endpoint cost: {endpoint_cost}',
                                                     'delayed'
                                                     )
        continue

      cluster_spec = None
      reuse_cluster_id = None
      
      if self._cluster_reuse_handler.is_enabled():
        fp = sched_component.cluster_spec_fingerprint
        if fp:
          available_clusters = idle_reuse_clusters.get(fp)
          
          while available_clusters:
            reuse_cluster_id = available_clusters.pop()
            if self.djs.get_cluster_by_id(reuse_cluster_id).get('state') == 'RUNNING':
              break
          else:
            # No available running clusters found
            reuse_cluster_id = None
          
          if not reuse_cluster_id:
            cluster_rt = self._cluster_reuse_handler.start_job_cluster(fp)
            if not cluster_rt:
              self._queued_not_run_pipelines.append(ara_pipeline_name) if ara_pipeline_name not in self._not_to_run_pipelines else None
              self._queued_not_run_pipelines_monitor[ara_pipeline_name] = (ara_pipeline_name, 
                                                           self._batch_time, 
                                                           'Failed to create reuse cluster', 
                                                           self._pipelines_scheduling_priorities.get(ara_pipeline_name, 99999),
                                                           len(queued),
                                                           queued.index([p for p in queued if p.get('ara_pipeline_name') == ara_pipeline_name][0]),
                                                           f'',
                                                           'delayed'
                                                           )
              continue
            self.active_run_tasks.add(cluster_rt)
            cluster_rt.wait_for_cluster_id()
            reuse_cluster_id = cluster_rt.cluster_id
            if self.djs.get_cluster_by_id(reuse_cluster_id).get('state').upper() not in ['RUNNING', 'PENDING', 'RESTARTING']:
              self._queued_not_run_pipelines.append(ara_pipeline_name) if ara_pipeline_name not in self._not_to_run_pipelines else None
              self._queued_not_run_pipelines_monitor[ara_pipeline_name] = (ara_pipeline_name, 
                                                           self._batch_time, 
                                                           'Failed to assign reuse cluster',
                                                           self._pipelines_scheduling_priorities.get(ara_pipeline_name, 99999),
                                                           len(queued),
                                                           queued.index([p for p in queued if p.get('ara_pipeline_name') == ara_pipeline_name][0]),
                                                           f"cluster status: {self.djs.get_cluster_by_id(reuse_cluster_id).get('state')}",
                                                           'delayed'
                                                           )
              continue
            self._add_pipeline_or_cluster_reuse_task(cluster_rt)
            
          cluster_spec = { 'existing_cluster_id': reuse_cluster_id }
        else:
          cluster_key = 'existing_cluster' if 'existing_cluster' in sched_component.cluster_spec else 'existing_cluster_id'
          cluster_value = sched_component.cluster_spec.get(cluster_key)
          if cluster_key == 'existing_cluster_id':
            id_to_name_dict = {v: k for k, v in self._running_tasks_on_ui_cluster_id.items()}
            cluster_value = id_to_name_dict.get(cluster_value)            
          if cluster_value in self._running_tasks_on_ui_cluster:
            if self._running_tasks_on_ui_cluster[cluster_value] >= self.cfg['ui_cluster_max_context']:            
              self._queued_not_run_pipelines.append(ara_pipeline_name) if ara_pipeline_name not in self._not_to_run_pipelines else None
              self._queued_not_run_pipelines_monitor[ara_pipeline_name] = (ara_pipeline_name, 
                                                            self._batch_time, 
                                                            f'Existing ui cluster: {cluster_value} reached max capacity',
                                                            self._pipelines_scheduling_priorities.get(ara_pipeline_name, 99999),
                                                            len(queued),
                                                            queued.index([p for p in queued if p.get('ara_pipeline_name') == ara_pipeline_name][0]),
                                                            f'',
                                                            'delayed'
                                                            )
              continue
            self._running_tasks_on_ui_cluster[cluster_value] += 1
   
             
      self.logger.debug(f"Executing {sched_component.run_mode} for {ara_pipeline_name} ({reuse_cluster_id=})")

      r = sched_component.run_as_run_task_aync(cluster_spec=cluster_spec)
      r.wait_for_cluster_id()

      self.logger.debug(f">>> {r}")
      self.active_run_tasks.add(r)
      self._add_pipeline_or_cluster_reuse_task(r)

      s = SchedulerState._update_state_dict_from_run_task(s, r)
      s = self.state.update(s, reason='run queued task')
      
      if ara_pipeline_name in self._queued_not_run_pipelines:
        self._queued_not_run_pipelines.remove(ara_pipeline_name)
        self._delayed_pipelines_runs[ara_pipeline_name] = (ara_pipeline_name, 
                                       self._batch_time, 
                                       '', 
                                       self._pipelines_scheduling_priorities.get(ara_pipeline_name, 99999), 
                                       len(queued), 
                                       queued.index([p for p in queued if p.get('ara_pipeline_name') == ara_pipeline_name][0]), 
                                       f"cluster_spec: {cluster_spec if cluster_spec else sched_component.cluster_spec}, cluster_type: {'job' if cluster_spec else 'ui'}", 
                                       'executed')
      self._queued_not_run_pipelines_monitor.pop(ara_pipeline_name, None)
      self._run_pipelines[ara_pipeline_name] = (ara_pipeline_name, self._batch_time, cluster_spec if cluster_spec else sched_component.cluster_spec, 'queued task run')

     
  @classmethod
  def _tables_to_internal_optimize(cls, paths_to_optimize=None):
    paths_to_optimize = paths_to_optimize or ['/mnt/monitoring', '/mnt/trackers']

    paths = []

    for root_path in paths_to_optimize:
      paths.extend([
        p 
        for p in DeltaLakeManagement.get_delta_tables_from_path(root_path)
        if not p.startswith('dbfs:/mnt/monitoring/metadata')
      ])

    return paths

  def _optimize_internal_tables(self):
    if (not self.active_run_tasks) and (datetime.now() >= self._next_technical_tables_optimize):     
      tables_to_optimize = self._tables_to_internal_optimize()
      self.logger.debug("Optimizing internal tables...")

      DeltaLakeManagement.optimize_delta_tables(tables_to_optimize)
      DeltaLakeManagement.vacuum_delta_tables(tables_to_optimize)

      self._next_technical_tables_optimize = datetime.now() + timedelta(minutes=60)

  @classmethod
  def get_scheduler_configuration(cls) -> dict:
    sched_cfg = Configuration('scheduler/Scheduler.yaml', default_config_dict={}).as_dict()

    sched_cfg['default_endpoint_type_limits'] = sched_cfg.get('default_endpoint_type_limits', {})
    sched_cfg['error_penalties'] = sched_cfg.get('error_penalties', [ 0, 60, 5*60, 15*60, 30*60, 3600, 2*3600, 4*3600 ])
    sched_cfg['graceful_job_termination_timeout'] = sched_cfg.get('graceful_job_termination_timeout', 900)
    sched_cfg['loop_time'] = int(sched_cfg.get('loop_time', 15))
    sched_cfg['upstream_dependency_wait_depth'] = int(sched_cfg.get('upstream_dependency_wait_depth', 3))
    sched_cfg['monitor'] = sched_cfg.get('monitor', {})
    sched_cfg['monitor']['enabled'] = sched_cfg['monitor'].get('enabled', False)
    sched_cfg['monitor']['location'] = sched_cfg['monitor'].get('location', '/mnt/monitoring/scheduler')
    sched_cfg['monitor']['retention'] = int(sched_cfg['monitor'].get('retention', 14))
    sched_cfg['ui_cluster_max_context'] = int(sched_cfg.get('ui_cluster_max_context', 150))
    sched_cfg['dependency_trigger_minimum_interval'] = int(sched_cfg.get('dependency_trigger_minimum_interval', 0))
    
    for _, d in Configuration.get_endpoints().items():
      t = d.get('type')
      if (not t) or (t in Configuration.internal_endpoint_types) or (t in sched_cfg['default_endpoint_type_limits']):
        continue
        
      sched_cfg['default_endpoint_type_limits'][t] = { 'source': 3, 'destination': 3 }

    return sched_cfg

  @classmethod
  def get_endpoint_limits(cls, endpoint):
    d = Configuration.get_endpoint(endpoint)
    if not d:
      raise KeyError(endpoint)

    t = d.get('type')
    if (not t) or (t in Configuration.internal_endpoint_types):
      return None

    sched_cfg = cls.get_scheduler_configuration()

    d['scheduler_limits'] = d.get('scheduler_limits', sched_cfg['default_endpoint_type_limits'][t])

    return d['scheduler_limits']

  @classmethod
  def get_endpoints_limits(cls):
    return { 
      name: cls.get_endpoint_limits(name)
      for name in Configuration.get_endpoints()
    }

  @classmethod
  def get_pipelines_endpoint_fit_cost(cls):
    pipelines = spark.sql("""
      SELECT DISTINCT 
        c.ara_pipeline_name
        ,collect_set(s.endpoint) as source
        ,collect_set(d.endpoint) as destination
      FROM metadata.pipeline_config as c
      LEFT JOIN metadata.pipeline_sources_flattened as s
      ON s.ara_pipeline_name = c.ara_pipeline_name
      LEFT JOIN metadata.pipeline_destinations_flattened as d
      ON d.ara_pipeline_name = c.ara_pipeline_name
      WHERE c.settings.streaming.enabled = 0
      GROUP BY c.ara_pipeline_name
    """).collect()

    return { 
      r.ara_pipeline_name: { 'source': r.source, 'destination': r.destination }
      for r in pipelines
    }

  def call_heartbeat_setup(self):
    for f in self.heartbeat_setup_functions:
      f(self)

  def call_heartbeat_callback(self):
    for f in self.heartbeat_callbacks:
      f(self)
