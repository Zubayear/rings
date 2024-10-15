use log::info;
use mini_redis::{client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = client::connect("127.0.0.1:6379").await?;

    client.set("hola", "world".into()).await?;

    let result = client.get("hola").await?;

    println!("GOT result; {:?}", result);

    Ok(())
}
