'''
This operator does the below in order:
 - Get pipeline id from parent task XCOM.
 - Poll ADF with pipeline id, and get child pipeline id.
 - Get output (max_load_timestamp) from last notebook activity in the child ADF pipeline.
 - Update Airflow variable with the max_load_timestamp.
'''


from typing import Optional, Dict
from airflow.models import Variable

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from utils.datafactory.adf_hook import AzureDataFactoryHook


def _handle_data_factory_operator_execution(operator, hook, log, filter_parameters, context) -> None:
    """
    Main function that handles ADF polling & output extraction from response.

    :author: Jim Todd

    :param operator: AzureDataFactory Operator being handled
    :param context: Airflow context
    """

    log.info('Polling to get response details - run_id: %s', operator.adf_run_id)

    kwargs = {"run_id": operator.adf_run_id, "filter_parameters": filter_parameters}
    if operator.resource_group_name:
        kwargs['resource_group_name'] = operator.resource_group_name
    if operator.factory_name:
        kwargs['factory_name'] = operator.factory_name

    # Get activity run of the parent pipeline.
    log.info('Parent ADF pipeline details:')
    activity_run = hook.get_conn().activity_runs.query_by_pipeline_run(**kwargs)
    log.info(vars(activity_run.value[0]))

    # Get activity run of the child pipeline.
    log.info('Child ADF pipeline details:')
    log.info(vars(activity_run.value[0]).get('output').get('pipelineRunId'))
    kwargs['run_id'] = vars(activity_run.value[0]).get('output').get('pipelineRunId')
    activity_run = hook.get_conn().activity_runs.query_by_pipeline_run(**kwargs)
    log.info(vars(activity_run.value[-1]))
    log.info(f"Notebook URL from child ADF activity: {vars(activity_run.value[-1]).get('output').get('runPageUrl')}")

    # Update airflow variable - max_load_ts as per table.
    Variable.set(
        key=operator.update_airflow_variable,
        value=str(vars(activity_run.value[-1]).get(
            'output'
        ).get(
            'runOutput'
        ).get(
            'gdp_max_load_timestamp'
        )
        ))
    log.info(
        f'Airflow Variable set successful: {operator.update_airflow_variable}: '
        f"{str(vars(activity_run.value[-1]).get('output').get('runOutput').get('gdp_max_load_timestamp'))}")


class AzureDataFactorySeekNotebookActivityOutput(BaseOperator):
    ui_color="#2B6756"

    template_fields = ['params']  # needed for jinja (airflow macros) to work

    @apply_defaults
    def __init__(
        self,
        *,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        data_factory_conn_id='azure_data_factory_default',
        update_airflow_variable=None,
        fetch_xcom_task=None,
        target_table=None,
        params: Optional[dict] = None,
        polling_period_seconds: int = 30,
        filter_parameters: Dict = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name
        self.data_factory_conn_id = data_factory_conn_id
        self.update_airflow_variable = update_airflow_variable
        self.fetch_xcom_task = fetch_xcom_task
        self.params = params
        self.filter_parameters = filter_parameters
        self.polling_period_seconds = polling_period_seconds
        self.adf_run_id = None
        self.target_table = target_table

    def _get_hook(self) -> AzureDataFactoryHook:
        return AzureDataFactoryHook(
            self.data_factory_conn_id
        )

    def execute(self, context):
        hook = self._get_hook()
        kwargs = dict()
        if self.resource_group_name:
            kwargs['resource_group_name'] = self.resource_group_name
        if self.factory_name:
            kwargs['factory_name'] = self.factory_name
        if self.params is not None:
            kwargs['parameters'] = self.params

        self.adf_run_id = context['ti'].xcom_pull(
            key=f"last_success_adf_pipeline_{self.target_table}",
            task_ids=self.fetch_xcom_task
        )
        self.log.info(f'XCOM pulled - last_success_adf_pipeline_{self.target_table}:', self.adf_run_id)

        _handle_data_factory_operator_execution(self, hook, self.log, self.filter_parameters, context)
