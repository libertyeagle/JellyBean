use mlflow::ExecutionConfig;
use mlflow::pipeline_relay_execute;

pub fn run_pipeline_relay(config: ExecutionConfig, pipeline_index: usize, relay_node_index: usize) {
    pipeline_relay_execute(&config, pipeline_index, relay_node_index);
}