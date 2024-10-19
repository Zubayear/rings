use std::env;
use std::net::Ipv4Addr;
use std::sync::Arc;

use diameter::avp::address::Value::IPv4;
use diameter::avp::flags::M;
use diameter::avp::Address;
use diameter::avp::Enumerated;
use diameter::avp::Identity;
use diameter::avp::UTF8String;
use diameter::avp::Unsigned32;
use diameter::dictionary::{self, Dictionary};
use diameter::flags;
use diameter::transport::DiameterClient;
use diameter::transport::DiameterClientConfig;
use diameter::{ApplicationId, CommandCode, DiameterMessage};
use log::{debug, info, warn};

use is_empty::IsEmpty;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use serde::{Deserialize, Serialize};

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        debug!("Pre balance {:?}", rebalance);
    }

    fn post_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        debug!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        debug!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume_and_print(brokers: &str, group_id: &str, topics: &[&str]) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "latest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(topics)
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };

                let my_message = match serde_json::from_str::<MyMessage>(payload) {
                    Ok(t) => t,
                    Err(e) => {
                        warn!("error {}", e);
                        MyMessage { name: None }
                    }
                };

                info!("message: {:?}", my_message);

                if !my_message.is_empty() {
                    if let Err(commit_err) = consumer.commit_message(&m, CommitMode::Async) {
                        warn!("Error committing message: {}", commit_err);
                    } else {
                        info!("message committed");
                    }
                }
            }
        };
    }
}

#[derive(Debug, Serialize, Deserialize, IsEmpty)]
struct MyMessage {
    name: Option<String>,
}

#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    debug!("Application started");

    let brokers = "localhost:9092";
    let group_id = "group.id";
    let topics = &["the_doors"];
    consume_and_print(brokers, group_id, topics).await;
    info!("yo bro 'Talk Tuah' is on");

    let dict = Dictionary::new(&[&dictionary::DEFAULT_DICT_XML]);
    let dict = Arc::new(dict);

    let client_config = DiameterClientConfig {
        use_tls: false,
        verify_cert: false,
    };

    let mut client = DiameterClient::new("localhost:3868", client_config);
    match client.connect().await {
        Ok(mut handler) => {
            let dict_ref = Arc::clone(&dict);
            tokio::spawn(async move {
                DiameterClient::handle(&mut handler, dict_ref).await;
            });

            send_cer(&mut client, Arc::clone(&dict)).await;

            send_ccr(&mut client, Arc::clone(&dict)).await;
        }
        Err(e) => {
            warn!("Could not connect to diameter server: {}", e);
        }
    }
}

async fn send_cer(client: &mut DiameterClient, dict: Arc<Dictionary>) {
    let seq_num = client.get_next_seq_num();
    let mut cer = DiameterMessage::new(
        CommandCode::CapabilitiesExchange,
        ApplicationId::Common,
        flags::REQUEST,
        seq_num,
        seq_num,
        dict,
    );
    cer.add_avp(264, None, M, Identity::new("host.example.com").into());
    cer.add_avp(296, None, M, Identity::new("realm.example.com").into());
    cer.add_avp(
        257,
        None,
        M,
        Address::new(IPv4(Ipv4Addr::new(127, 0, 0, 1))).into(),
    );
    cer.add_avp(266, None, M, Unsigned32::new(35838).into());
    cer.add_avp(269, None, M, UTF8String::new("diameter-rs").into());

    let resp = client.send_message(cer).await.unwrap();
    let cea = resp.await.unwrap();
    info!("Received rseponse: {}", cea);
}

async fn send_ccr(client: &mut DiameterClient, dict: Arc<Dictionary>) {
    let seq_num = client.get_next_seq_num();
    let mut ccr = DiameterMessage::new(
        CommandCode::CreditControl,
        ApplicationId::CreditControl,
        flags::REQUEST,
        seq_num,
        seq_num,
        dict,
    );
    ccr.add_avp(264, None, M, Identity::new("host.example.com").into());
    ccr.add_avp(296, None, M, Identity::new("realm.example.com").into());
    ccr.add_avp(263, None, M, UTF8String::new("ses;12345888").into());
    ccr.add_avp(416, None, M, Enumerated::new(1).into());
    ccr.add_avp(415, None, M, Unsigned32::new(1000).into());
    ccr.add_avp(
        1228,
        Some(10415),
        M,
        Address::new(IPv4(Ipv4Addr::new(127, 0, 0, 1))).into(),
    );

    let resp = client.send_message(ccr).await.unwrap();
    let cca = resp.await.unwrap();
    info!("Received rseponse: {}", cca);
}
