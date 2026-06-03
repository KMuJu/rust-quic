use std::time::Duration;

use crate::core::{
    space::PacketSpace,
    tests::{MTU, create_connections, do_handshake, send_all_packets},
};

#[test]
fn test_maxackdelay_timer() {
    let (mut client, mut server) = create_connections();
    do_handshake(&mut client, &mut server);

    let id = client.new_bidir();
    client.write(&[1, 2, 3, 4, 5], id).unwrap();
    send_all_packets(&mut client, &mut server, MTU, false);

    let next = server.next_deadline();
    println!();
    assert!(next.is_some(), "Next deadline should something");
    assert_eq!(
        next,
        server.next_ack_deadline(),
        "Next deadline should be the ack deadline"
    );

    // Send acks
    send_all_packets(&mut server, &mut client, MTU, false);
    assert_eq!(server.next_deadline(), None, "Next deadline should be None");
}

#[test]
fn test_loss_timer() {
    let (mut client, mut server) = create_connections();
    do_handshake(&mut client, &mut server);

    let id = client.new_bidir();
    client.write(&[1, 2, 3, 4, 5], id).unwrap();
    send_all_packets(&mut client, &mut server, MTU, true); // Packet is lost

    let next = client.next_deadline();
    assert!(next.is_some(), "Loss timer should be set");

    let (pto, p_space) = client.get_pto_time_and_space().unwrap();
    assert_eq!(
        next.unwrap(),
        pto,
        "Next deadline should be pto since no ack has been received"
    );
    assert_eq!(p_space, PacketSpace::Data);

    // Send two packets
    client.write(&[1, 2, 3, 4, 5], id).unwrap();
    send_all_packets(&mut client, &mut server, MTU, false);
    client.write(&[1, 2, 3, 4, 5], id).unwrap();
    send_all_packets(&mut client, &mut server, MTU, false);

    // Send acks
    send_all_packets(&mut server, &mut client, MTU, false);

    let next = client.next_deadline();
    assert!(next.is_some(), "Loss timer should be set");

    let (loss, p_space) = client.get_loss_time_and_space().unwrap();
    assert_eq!(
        next.unwrap(),
        loss,
        "Next deadline should be loss since the first packet is unacked"
    );
    assert_eq!(p_space, PacketSpace::Data);

    client.handle_timeout(loss + Duration::from_secs(10)); // Trigger loss
    send_all_packets(&mut client, &mut server, MTU, false); // Retransmission
    send_all_packets(&mut server, &mut client, MTU, false); // Sending ack

    let next = client.next_deadline();
    assert_eq!(
        next, None,
        "Loss timer should be reset after retransmission"
    );
}
