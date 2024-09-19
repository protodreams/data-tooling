

use std::sync::Arc;

use arrow_array::types::Float32Type;

use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use futures::TryStreamExt;

use lancedb::arrow::IntoArrow;
use lancedb::connection::Connection;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::{connect, Result, Table as LanceDbTable};



#[tokio::main]
async fn main()-> Result<()> {
    if std::path::Path::new("data").exists() {
        std::fs::remove_dir_all("data").unwrap();
    }

    let uri = "data/sample-lancedb";
    let db = connect(uri).execute().await?;

    println!("{:?}", db.table_names().execute().await?);

    let tbl = create_table(&db).await?;
    create_index(&tbl).await?;
    let batches = search(&tbl).await?;
    println!("{:?}", batches);

    match create_empty_table(&db).await {
        Ok(_) => Ok(()),
        Err(e) =>  {
            eprintln!("Error create table: {}", e);
            Err(e)
        }
    }?;

    match tbl.delete("id > 24").await {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Error delete table: {}", e);
            Err(e)
        }
    }?;

    match db.drop_table("my_table").await {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Error drop table {}", e);
            Err(e)
        }   
    }?;

    Ok(())
}

#[allow(dead_code)]
async fn open_with_existing_tbl() -> Result<()> {
    let uri = "data/sample-lancedb";
    let db = connect(uri).execute().await?;
    #[allow(unused_variables)]

    let table = match db.open_table("my_table").execute().await {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("Error open table {}", e);
            Err(e)
        }
    };
    
    Ok(())
}  

fn create_record_batch_iterator(schema: Arc<Schema>) -> Result<Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>, ArrowError>  {
        let record_batch = RecordBatch::try_new( 
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..TOTAL as i32)),
                    Arc::new( 
                        FixedSizeListArray::from_iter_primitive::<Float32Type>(
                            (0..TOTAL).map(|_| vec![1.0; DIM]),
                        )),
                ],
            )?;
        
        let batches = RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema);
        Ok(Box::new(batches))
}



fn create_some_records() -> Result<impl IntoArrow> {
    const TOTAL: usize = 1000;
    const DIM: usize = 128;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector", 
            DataType::FixedSizeList(  
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ), 
            true,
        ),
    ]));

    let batches = create_record_batch_iterator(schema)?;
    Ok(batches)
}

async fn create_table(db: &Connection) -> Result<LanceDbTable> {
    let initial_data = create_some_records()?;
    let tbl = db
              .create_table("my_table", initial_data)
              .execute()
              .await?;

    let new_data = create_some_records()?;
    tbl.add(new_data).execute().await?;

    Ok(tbl)
}

async fn create_empty_table(db: &Connection) -> Result<LanceDbTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("item", DataType:: Utf8, true),
    ]));
    db.create_empty_table("empty_table", schema).execute().await?
}

async fn create_index(table: &LanceDbTable) -> Result<()> {
    table.create_index(&["vector"], Index::Auto).execute.await?;
}

async fn search(table: &LanceDbTable) -> Result<Vec<RecordBatch>> {
    table
        .query()
        .limit()
        .nearest_to(&[1.0; 128])?
        .await?
        .try_collect::<Vec<_>>()
        .await?
}




    

