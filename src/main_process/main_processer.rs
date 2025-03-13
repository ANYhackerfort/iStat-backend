use std::collections::HashMap;
use crate::NodePayload;
use std::any::Any;
use axum::Json;
use serde::Serialize;

use crate::nodes::inner_join::Inner_Join;
use crate::nodes::clean_na::Clean_By_Column;
use crate::nodes::output_csv::Output_CSV;

pub struct NodeManager<'a> {
    pub numbers: Vec<u32>,
    pub node_dict: &'a HashMap<u32, NodePayload>, // Reference to a local HashMap
    pub node_map: HashMap<String, Box<dyn Any>>,
    pub file_dict: &'a mut HashMap<String, String>, 
}

#[derive(Serialize)]
pub struct ProcessedNode {
    pub node_id: u32,
    pub data: serde_json::Value, 
}

impl<'a> NodeManager<'a> {
    pub fn new(numbers: Vec<u32>, node_dict: &'a HashMap<u32, NodePayload>, file_dict: &'a mut HashMap<String, String>) -> Self {
        let mut node_map = HashMap::new();

        // Populate `node_map` with types from `node_dict`
        for (_, node_payload) in node_dict.iter() {
            let node_type = &node_payload.r#type;
            if !node_map.contains_key(node_type) {
                if node_type == "inner-join-csv" {
                    node_map.insert(node_type.clone(), Box::new(Inner_Join) as Box<dyn Any>);
                } else if node_type == "data-cleaning-block-remove-null" {
                    node_map.insert(node_type.clone(), Box::new(Clean_By_Column) as Box<dyn Any>);
                } else if node_type == "output-to-csv" {
                    node_map.insert(node_type.clone(), Box::new(Output_CSV) as Box<dyn Any>);
                } else {
                    continue;
                }
            }                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
        }
            NodeManager { numbers, node_dict, node_map, file_dict }
    }

    pub fn get_from_node_map<T: 'static>(&self, key: &str) -> Option<&T> {
        self.node_map.get(key)?.downcast_ref::<T>()
    }

    pub fn process_nodes_in_order(&mut self) -> Json<Vec<ProcessedNode>> {
        let mut results: Vec<ProcessedNode> = Vec::new();
        for number in &self.numbers {
            if let Some(node_payload) = self.node_dict.get(number) {
                let node_type = &node_payload.r#type;
                let node_id: u32 = node_payload.node_id;
                if let Some(node) = self.node_map.get(node_type) { // Populate node modules here, if successful from any of these functions, then it shouldn't return any string. 
                    let neighbors_dependent: Vec<u32> = node_payload.neighbors_dependent.clone();
                    let node_data = &node_payload.data;
                    if let Some(node) = node.downcast_ref::<Inner_Join>() {
                        let csv_path1: &String = self.file_dict.get(&neighbors_dependent[0].to_string()).unwrap();
                        let csv_path2 = self.file_dict.get(&neighbors_dependent[1].to_string()).unwrap();
                        let return_path = node.process_node(&csv_path1, &csv_path2, &node_data, &node_id).unwrap();
                        self.file_dict.insert(node_id.to_string(), return_path);
                    } else if let Some(node) = node.downcast_ref::<Clean_By_Column>() {
                        let csv_path1: &String = self.file_dict.get(&neighbors_dependent[0].to_string()).unwrap();
                        let return_path = node.process_node(&csv_path1, &node_data, &node_id).unwrap();
                        self.file_dict.insert(node_id.to_string(), return_path);
                    } else if let Some(node) = node.downcast_ref::<Output_CSV>() {
                        let csv_path1: &String = self.file_dict.get(&neighbors_dependent[0].to_string()).unwrap();
                        if let Ok(return_data) = node.process_node(&csv_path1) {
                            results.push(ProcessedNode {
                                node_id,
                                data: return_data, 
                            });
                        }
                    }   
                }
            }
        }
        Json(results)
    }

    pub fn print_state(&self) {
        println!("Numbers: {:?}", self.numbers);

        println!("Node Dictionary:");
        for (key, value) in self.node_dict.iter() {
            println!(
                "Key: {}, Node: {{ ID: {}, Type: {}, Neighbors Dependent: {:?}, Neighbors Pointing: {:?}, Data: {} }}",
                key, value.node_id, value.r#type, value.neighbors_dependent, value.neighbors_pointing, value.data
            );
        }
    }
}
