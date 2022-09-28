use mlflow::ExecutionConfigGUID;
use mlflow::execute::pipeline_relay_execute_guid;

pub fn run_pipeline_relay(config: &ExecutionConfigGUID, pipeline_index: usize) {
    pipeline_relay_execute_guid(&config, pipeline_index, 0);
}