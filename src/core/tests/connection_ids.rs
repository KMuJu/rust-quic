use crate::core::{
    stream::Side,
    tests::{create_connections, do_handshake, new_cid, send_all_packets},
};

#[test]
fn test_getting_new_cid() {
    let (mut client, mut server) = create_connections();
    do_handshake(&mut client, &mut server);

    let new_cid = new_cid(Side::Client);

    client.send_new_cid(new_cid);

    send_all_packets(&mut client, &mut server, 2000, false);

    client.retire_current_cid();

    send_all_packets(&mut client, &mut server, 2000, false);

    let sid = server.new_bidir();
    server.write(&[0, 1, 2, 3], sid).unwrap();
    send_all_packets(&mut server, &mut client, 2000, false);
}
