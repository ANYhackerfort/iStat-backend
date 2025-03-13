use csv::{ReaderBuilder, WriterBuilder, StringRecord};
use std::error::Error;
use std::fs;
pub struct Clean_By_Column;

impl Clean_By_Column {
    pub fn process_node(&self, file1: &str, data: &str, node_id: &u32) -> Result<String, Box<dyn Error>> {
        let mut reader1 = ReaderBuilder::new().from_path(file1)?;
    
        // Get the headers and find the index of the `data` column
        let headers = reader1.headers()?.clone();
        let data_index = headers
            .iter()
            .position(|h| h == data)
            .ok_or_else(|| format!("File '{}' does not contain a column named '{}'", file1, data))?;
    
        // Create an output file path
        let output_file = format!("./storage_bin/cleaned_{}.csv", node_id);
        
        // Create a CSV writer
        let mut writer = WriterBuilder::new().from_path(&output_file)?;
        
        // Write headers
        writer.write_record(&headers)?;
    
        // Filter and write non-null rows
        for result in reader1.records() {
            let record = result?;
            if let Some(value) = record.get(data_index) {
                if value != "NA" && !value.trim().is_empty() {
                    writer.write_record(&record)?;
                }
            }
        }
    
        println!("Cleaned CSV written to {}", output_file);
        Ok(output_file)
    }
}