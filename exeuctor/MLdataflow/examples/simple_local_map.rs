use std::collections::VecDeque;
use std::iter::FromIterator;

use mlflow::MapLocal;
use mlflow::GraphBuilder;
use mlflow::local_execute_thread;

fn main() {
    let builder = |builder: &mut GraphBuilder<usize>| {
        let input_data_vec = VecDeque::from_iter([1 ,2 ,3, 4, 5]);
        let handle = builder.new_input_from_source(input_data_vec, |data, _| {
            (*data as usize) + 1
        }, "Input").map_local(|x| (x * 2, "hello".to_string()), "Map_0");
        handle.map_local(|x| {
            println!("hello with {}", x.0);
            x.0 * 2
        }, "Map_1");
    };
    local_execute_thread(builder);
}