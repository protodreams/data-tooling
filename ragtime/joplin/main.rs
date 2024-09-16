

use std::sync::Arc;

use arrow_array::types::Float32Type;



#[tokio::main]
async fn main()-> Result<()> {
    let uri = "data/sample-lancedb";
    let db = connect(uri).execute().await()?;
}
