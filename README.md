# Quic implementation

This project is an attempt at implementing the QUIC protocol following these RFCs:
- [RFC9000](https://datatracker.ietf.org/doc/html/rfc9000)
- [RFC9001](https://datatracker.ietf.org/doc/html/rfc9001)
- [RFC9002](https://datatracker.ietf.org/doc/html/rfc9002)

> [!IMPORTANT]
> It is in no way a finished implementation and has lots of missing features of the protocol, but the main flow of the protocol is correct.

The structure takes some inspiration from [quinn-rs/quinn](https://github.com/quinn-rs/quinn/), but tries to remove abstractions in order for it to be easier to understand

## AI usage

AI was mainly used to help me understand the RFCs, debugging and verifying that what I was doing was sound.
No claude code or something similar was used, mainly because this was for learning purposes and not just something I wanted to have as a finished product.

## Handhsake

Early on, the handshake was mocked up to be able to implement other parts of the protocol before handling encryption and such.
This is handled by [rustls](https://github.com/rustls/rustls/), and I pass the data from crypto frames into the tls connection.

## Encryption

Was recently added using the quic module in rustls.
How the keys are handled might still be wrong, but the encryption works.

## Architecture

```
                             ┌──────────────────┐
                             │ ConnectionDriver │
                             └──────────────────┘                              ┌──────────────────────────────────┐
                                      │                                        │                                  │
 ┌──────────┐   Arc<Mutex<>>          │ Arc<Mutex<>>                           │              Core                │
 │ Incoming │──────────┐              ▼                                        │                                  │
 └──────────┘          │   ┌──────────────────────┐                            │    ┌───────────────────────┐     │
                       └──►│                      │        Functions           │    │                       │     │
 ┌──────────────┐          │                      │◄────────────────────────────────│                       │     │
 │ StreamHandle │─────────►│     ConnectionIO     │                            │    │     ConnectionCore    │     │
 └──────────────┘          │                      │────────────────────────────────►│                       │     │
                       ┌──►│                      │                            │    │                       │     │
 ┌────────────┐        │   └──────────────────────┘                            │    └───────────────────────┘     │
 │ Connection │────────┘           ▲         │                                 │                                  │
 └────────────┘  Arc<Mutex<>>      │         │                                 │                                  │
                                   │         │                                 │                                  │
                                   │         │ Channel                         │                                  │
                                   │         │                                 │                                  │
                                   │         │                                 │                                  │
                                   │         ▼                                 │                                  │
                               ┌────────────────┐                              │     ┌───────────────────────┐    │
                               │                │           Functions          │     │                       │    │
 ┌──────────┐  Arc<Mutex<>>    │                │◄───────────────────────────────────│                       │    │
 │ Endpoint │─────────────────►│   EndpointIO   │                              │     │      EndpointCore     │    │
 └──────────┘                  │                │───────────────────────────────────►│                       │    │
                               │                │                              │     │                       │    │
                               └────────────────┘                              │     └───────────────────────┘    │
                                        ▲                                      │                                  │
                                        │ Arc<Mutex<>>                         │                                  │
                                        │                                      └──────────────────────────────────┘
                               ┌────────────────┐
                               │ EndpointDriver │
                               └────────────────┘
```

## Notes about QUIC

Can be found [here](QUIC-NOTES.md)

## Challenges

- Learning how lower level network protocols work, how they have to handle incoming data while also buffering it for the users.
- Keeping most of the code sync, while still having an async api
  - Ended up having to refactor this part a couple times, and it is still imperfect
- Debug async code
  - Had to use lots of logging and have AI tell me what was happening in the logs
  - Should probably use something like the tokio console and tracing, but haven't gotten around to setting that up
- Implementing loss detection and recovery
  - Ended up following the pseudocode as much as possible, but that meant restructuring how the code worked
- Managing the handshake and encryption of packets
  - Again, needed some refactoring because my mock was quite ugly
