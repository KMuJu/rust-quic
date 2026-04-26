# Quic implementation

This project takes some inspiration from quinn-rs/quinn

## Mocking handshake

I decided to make a fake version of the handshake.

It goes like this:

```text
Client                   Server
|----Initial(ClientHello)--->|
|<---Handshake(ServerHello)--|
|----Handshake(Finished)---->|
|<---Short(HandshakeDone)----|
```

Here Initial, Handshake, Short are the different packet headers
