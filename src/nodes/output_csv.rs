use std::fs::File;
use std::io::{BufReader};
use serde_json::json;
use csv::ReaderBuilder;
use std::error::Error;

pub struct Output_CSV;

impl Output_CSV {
    // Open CSV file and package its contents as JSON
    pub fn process_node(&self, file_path: &str) -> Result<serde_json::Value, Box<dyn Error>> {
        let file = File::open(file_path)?;
        let mut reader = ReaderBuilder::new().from_reader(BufReader::new(file));

        let headers = reader.headers()?.clone(); // Get headers
        let mut csv_data: Vec<serde_json::Value> = Vec::new();

        for result in reader.records() {
            let record = result?;
            let row: serde_json::Value = headers.iter().zip(record.iter()).collect();
            csv_data.push(row);
        }

        Ok(json!(csv_data)) // Return JSON representation of CSV
    }
}