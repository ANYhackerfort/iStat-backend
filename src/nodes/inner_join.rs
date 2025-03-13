use csv::{ReaderBuilder, WriterBuilder};
use std::collections::HashSet;
use std::error::Error;

pub struct Inner_Join;

impl Inner_Join {
    pub fn process_node(
        &self,
        file1: &str,
        file2: &str,
        data: &str,
        node_id: &u32,
    ) -> Result<String, Box<dyn Error>> {
        // Create CSV readers for both files
        println!("file1: {}, file2: {}", file1, file2);
        let mut reader1 = ReaderBuilder::new().from_path(file1)?;
        let mut reader2 = ReaderBuilder::new().from_path(file2)?;
    
        // Get headers for both files and find the index of the `data` column
        let headers1 = reader1.headers()?.clone();
        let data_index1 = headers1
            .iter()
            .position(|h| h == data)
            .ok_or_else(|| format!("File '{}' does not contain a column named '{}'", file1, data))?;
    
        let headers2 = reader2.headers()?.clone();
        let data_index2 = headers2
            .iter()
            .position(|h| h == data)
            .ok_or_else(|| format!("File '{}' does not contain a column named '{}'", file2, data))?;
    
        // Collect all `data` values into sets for intersection
        let mut data_set1: HashSet<String> = HashSet::new();
        for result in reader1.records() {
            let record = result?;
            if let Some(value) = record.get(data_index1) {
                data_set1.insert(value.to_string());
            }
        }
    
        let mut data_set2: HashSet<String> = HashSet::new();
        for result in reader2.records() {
            let record = result?;
            if let Some(value) = record.get(data_index2) {
                data_set2.insert(value.to_string());
            }
        }
    
        // Find common values in the `data` column
        let common_data_set: HashSet<_> = data_set1.intersection(&data_set2).cloned().collect();
    
        // Combine headers from both files without duplicates
        let mut combined_headers_vec: Vec<&str> = headers1.iter().collect();
        for header in headers2.iter() {
            if !combined_headers_vec.contains(&header) {
                combined_headers_vec.push(header);
            }
        }
    
        // Create an output file path
        let output_file = format!("./storage_bin/{}.csv", node_id);
    
        // Initialize the CSV writer
        let mut writer = WriterBuilder::new().from_path(&output_file)?;
        writer.write_record(&combined_headers_vec)?;
    
        // Process each common `data` value and populate the writer
        for common_id in &common_data_set {
            let mut combined_row: Vec<String> = vec!["".to_string(); combined_headers_vec.len()];
    
            // Reset and iterate through reader1 to find the matching row
            let mut reader1 = ReaderBuilder::new().from_path(file1)?; // Recreate reader1
            reader1.headers()?; // Skip headers
            for result in reader1.records() {
                let record = result?;
                if record.get(data_index1) == Some(common_id.as_str()) {
                    // Populate the row with values from reader1
                    for (i, value) in record.iter().enumerate() {
                        let header_value = &headers1[i];
                        if let Some(index) = combined_headers_vec
                            .iter()
                            .position(|h| h.to_string() == *header_value) {
                                combined_row[index] = value.to_string(); // Copy the value
                        }
                    }
                    break;
                }
            }
    
            // Reset and iterate through reader2 to find the matching row
            let mut reader2 = ReaderBuilder::new().from_path(file2)?; // Recreate reader1
            reader2.headers()?; // Skip headers
            for result in reader2.records() {
                let record = result?;
                if record.get(data_index2) == Some(common_id.as_str()) {
                    // Populate the row with values from reader2
                    for (i, value) in record.iter().enumerate() {
                        if let Some(index) = combined_headers_vec
                            .iter()
                            .position(|h| h.to_string() == headers2[i]) {
                                combined_row[index] = value.to_string();
                        }
                    }
                    break;
                }
            }
    
            // Write the combined row to the output CSV
            writer.write_record(&combined_row)?;
        }
    
        println!("Combined CSV written to {}", output_file);
        Ok(output_file)
    }
}