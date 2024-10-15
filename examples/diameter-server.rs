use diameter::avp::flags::M;
use diameter::avp::Enumerated;
use diameter::avp::Identity;
use diameter::avp::UTF8String;
use diameter::avp::Unsigned32;
use diameter::dictionary::{self, Dictionary};
use diameter::flags;
use diameter::transport::DiameterServer;
use diameter::transport::DiameterServerConfig;
use diameter::DiameterMessage;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Diameter Dictionary
    let dict = Dictionary::new(&[&dictionary::DEFAULT_DICT_XML]);
    let dict = Arc::new(dict);

    // Set up a Diameter server listening on a specific port
    let mut server = DiameterServer::new("0.0.0.0:3868", DiameterServerConfig { native_tls: None })
        .await
        .unwrap();

    // Asynchronously handle incoming requests to the server
    let dict_ref = Arc::clone(&dict);
    server
        .listen(
            move |req| {
                let dict_ref2 = Arc::clone(&dict);
                async move {
                    println!("Received request: {}", req);

                    // Create a response message based on the received request
                    let mut res = DiameterMessage::new(
                        req.get_command_code(),
                        req.get_application_id(),
                        req.get_flags() ^ flags::REQUEST,
                        req.get_hop_by_hop_id(),
                        req.get_end_to_end_id(),
                        dict_ref2,
                    );

                    // Add various Attribute-Value Pairs (AVPs) to the response
                    res.add_avp(264, None, M, Identity::new("host.example.com").into());
                    res.add_avp(296, None, M, Identity::new("realm.example.com").into());
                    res.add_avp(263, None, M, UTF8String::new("ses;123458890").into());
                    res.add_avp(416, None, M, Enumerated::new(1).into());
                    res.add_avp(415, None, M, Unsigned32::new(1000).into());
                    res.add_avp(268, None, M, Unsigned32::new(2001).into());

                    // Return the response
                    Ok(res)
                }
            },
            dict_ref,
        )
        .await
        .unwrap();
}
