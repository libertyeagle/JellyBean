use std::collections::VecDeque;
use std::iter::FromIterator;

use mlflow::{Map, Inspect, MapLocal};
use mlflow::PipelineGraphBuilder;

use mlflow::ExecutionConfigGUID;
use mlflow::execute::pipeline_worker_execute_guid;

pub fn run_pipeline_worker(config: &ExecutionConfigGUID, pipeline_index: usize) {
    let builder = |builder: &mut PipelineGraphBuilder<usize>| {
        let input_data_vec = VecDeque::from_iter([1 ,2 ,3, 4, 5]);
        let op0 = builder.new_input_from_source(input_data_vec, |data, _| {
            (*data as usize) + 1
        }, "Input");
        let op1 = op0.map(|x| x * 2, "Map_1");
        let op2 = op0.map(|x| String::from(format!("hello with {}", x)), "Map_2");
        let _op3 = op0.map(|x| x + 1, "Map_3");
        let op4 = op1.map_local(|x| x + 5, "Map_4");
        let _op5 = op4.inspect(|x| {
            println!("recv i32 {} at op5", x);
        }, "Insepct_1");
        let _op6 = op2.inspect(|x| {
            println!("recv string: {}", x);
        }, "Inspect_2");
    };

    pipeline_worker_execute_guid(builder, &config, pipeline_index, 0);
}