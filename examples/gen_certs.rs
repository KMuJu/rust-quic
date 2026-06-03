use rcgen::generate_simple_self_signed;
use std::fs;

fn main() {
    let cert = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    fs::write("examples/keys/cert.pem", cert.cert.pem()).unwrap();
    fs::write("examples/keys/key.pem", cert.key_pair.serialize_pem()).unwrap();
    println!("Generated examples/cert.pem and examples/key.pem");
}
