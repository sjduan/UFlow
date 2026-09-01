use std::env;
use std::fs::OpenOptions;
use uf_core::bridge::BridgeFrame;
use uf_ub_sim_agent::MockCompanion;

fn main() {
    let mut args = env::args().skip(1);
    let device = match (args.next().as_deref(), args.next()) {
        (Some("--device"), Some(path)) => path,
        _ => {
            eprintln!("usage: uf-ub-sim-agent --device <virtio-port-device>");
            std::process::exit(2);
        }
    };
    let mut stream = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&device)
        .unwrap_or_else(|error| panic!("open bridge device {}: {}", device, error));
    let mut companion = MockCompanion::default();
    loop {
        let request = match BridgeFrame::read_from(&mut stream) {
            Ok(frame) => frame,
            Err(error) => {
                eprintln!("bridge read failed: {:?}", error);
                break;
            }
        };
        let response = companion
            .handle(request)
            .unwrap_or_else(|error| panic!("bridge request failed: {:?}", error));
        response
            .write_to(&mut stream)
            .unwrap_or_else(|error| panic!("bridge write failed: {:?}", error));
    }
}
