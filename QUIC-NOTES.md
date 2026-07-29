# Notes from RFC9000

## Streams

Multiplexes streams
Four types of streams

Client-initiated bidirectional (both ways) (Client <-> Server)
Server-initiated bidirectional (both ways) (Server <-> Client)
Client-initiated unidirectional (one way) (Client -> Server)
Server-initiated unidirectional (one way) (Server -> Client)

Uniquely identified with 62 bit stream id
The least significant bit of the stream id is the initiator
0 for client (even number), 1 for server (odd number)
The second least significant bit is bi or unidirectional
0 for bi, 1 for uni

Successive streams of each type are created with numerically increasing stream IDs

### Sending and receiving

Uses stream id and offset fields in stream frames to place data in order
Must be able to deliver stream data to an application as ordered byte stream
Need to buffer data that is out of order, up to advertised flow control limit
> [!IMPORTANT]
> Need to find a way to handle this

Can receive data for a stream with the same offset multiple times, this can be discarded
It may be a protocol violation if the data changes

Is an ordered byte-stream abstraction with no other structure visible to the protocol

Must not send data without ensuring it is within the flow control limits set by its peer

### Priority

Relies on receiving stream priority from the application
Should provide ways for the application to indicate the priority of streams
Uses the priority to allicate resourses to the streams

### Operations

Sending
- Write data, understanding when stream flow control credit has successfully been reserved to send the written data
- End the stream
- Reset the stream

Receiving
- Read data
- Abort reading of the stream and request closure, possibly resulting in a STOP_SENDING frame

Can request to be informed of state changes on streams

### Stream states

A stream has two possible statemachines, one for receiving and one for sending

#### Sending

```
     +
       o
       | Create Stream (Sending)
       | Peer Creates Bidirectional Stream
       v
   +-------+
   | Ready | Send RESET_STREAM
   |       |-----------------------.
   +-------+                       |
       |                           |
       | Send STREAM /             |
       |      STREAM_DATA_BLOCKED  |
       v                           |
   +-------+                       |
   | Send  | Send RESET_STREAM     |
   |       |---------------------->|
   +-------+                       |
       |                           |
       | Send STREAM + FIN         |
       v                           v
   +-------+                   +-------+
   | Data  | Send RESET_STREAM | Reset |
   | Sent  |------------------>| Sent  |
   +-------+                   +-------+
       |                           |
       | Recv All ACKs             | Recv ACK
       v                           v
   +-------+                   +-------+
   | Data  |                   | Reset |
   | Recvd |                   | Recvd |
   +-------+                   +-------+
     +
```

Ready state is a stream that is able to accept data from the application
Send state
- Transmits and retransmits stream data in STREAM frames
- Respects ow control limits set by its peer
- Accepts and processes MAX_STREAM_DATA frames
- Generates STREAM_DATA_BLOCKED frames if it is blocked from sending by stream flow control limits

Data sent
- Is indicated by the application that all data has been sent
- Only retransmits if needed
- No need to check flow control limits

Reset sent
- From any state, the application can signal that it wishes to abandon transmission
- Or it receives a STOP_SENDING
- Sends a reset frame

#### Receiving

```
       o
       | Recv STREAM / STREAM_DATA_BLOCKED / RESET_STREAM
       | Create Bidirectional Stream (Sending)
       | Recv MAX_STREAM_DATA / STOP_SENDING (Bidirectional)
       | Create Higher-Numbered Stream
       v
   +-------+
   | Recv  | Recv RESET_STREAM
   |       |-----------------------.
   +-------+                       |
       |                           |
       | Recv STREAM + FIN         |
       v                           |
   +-------+                       |
   | Size  | Recv RESET_STREAM     |
   | Known |---------------------->|
   +-------+                       |
       |                           |
       | Recv All Data             |
       v                           v
   +-------+ Recv RESET_STREAM +-------+
   | Data  |--- (optional) --->| Reset |
   | Recvd |  Recv All Data    | Recvd |
   +-------+<-- (optional) ----+-------+
       |                           |
       | App Read All Data         | App Read Reset
       v                           v
   +-------+                   +-------+
   | Data  |                   | Reset |
   | Read  |                   | Read  |
   +-------+                   +-------+
```

Initiated when the first STREAM, STREAM_DATA_BLOCKED or RESET_STREAM frame is received
For bidirectional initiated by a peer, MAX_STREAM_DATA or STOP_SENDING frame also initiates
For a bidirectional stream, the statemachine enters first state when the initiated part enters the ready state (??)

Open a bidirectional stream when a MAX_STREAM_DATA or STOP_SENDING frame is received from the peer
Receiving a MAX_STREAM_DATA frame for an unopened stream indicates that the remote peer has opened the stream and is providing flow control credit
STOP_SENDING frame for unopened stream indicates that the remote no longer wishes to receive data on this stream
Both might arrive before a STREAM or STREAM_DATA_BLOCKED frame

Before a stream is created, all streams of the same type with lower numbered ids must be created

Recv
- Receives STREAM and STREAM_DATA_BLOCKED frames
- Incoming data is buffered and reassembled into the correct order for delivery to the application
- As data is consumed by the application, send MAX_STREAM_DATA to allow peer to send more data
- When a STREAM frame with a FIN bit is received, the final size of the stream is known -> enter Size Known state
- RESET_STREAM -> Reset Recvd state

Size Known
- No need to send MAX_STREAM_DATA frames
- Only waits for retransmitions
- RESET_STREAM -> Reset Recvd state

Data Recvd
- Any STREAM or STREAM_DATA_BLOCKED frame can be discarded
- Persists until all data is read to the application

It is possible that all stream data has already been received when a RESET_STREAM is received
It is also possible that all the data is received after receiving a RESET_STREAM
> [!IMPORTANT]
> Need to find a way to handle this

RESET_STREAM means that en endpoint cannot guarantee deliver of stream data
May interrupt delivery of stream data, discard any data that was not consumed, and signal the receipt of the RESET_STREAM
RESET_STREAM signal might be suppressed or withheld if stream data is completely received and is buffered
If it is suppressed, then stay in the Data Recvd state

#### Permitted Frame Types

Only three frame types affect the states
STEAM, STREAM_DATA_BLOCKED, RESET_STREAM
Sender must not send these when in terminal state
Sender must not send STREAM or STREAM_DATA_BLOCKED after RESET_STREAM
Receiver might receive any of these packets in any state

Receiver sends MAX_STREAM_DATA and STOP_SENDING frames

Receiver only sends MAX_STREAM_DATA in Recv state
Receiver may send a STOP_SENDING frame in any state unless it has received a RESET_STREAM frame

#### Bidirectional Stream States

Sends and receives

RFC9000 3.4

#### Solicited State Transitionos

Applications can abort reading the stream and specify an application error code
If in Recv or Size Known state, should send STOP_SENDING

STREAM frames received after sending STOP_SENDING frame are still counted towards connection and stream flow control
They can be discarded though

A STOP_SENDING frame requests a RESET_STREAM frame in return
Must send RESET_STREAM when receiving a STOP_SENDING stream in Ready or Send state
If in Data Sent state, then may wait until it is known whether packets are acked or lost
Should send RESET_STREAM instead of retransmiting the data

Should copy the error code from the STOP_SENDING frame to RESET_STREAM frame it sends
Endpoint sending STOP_SENDING frame may ignore the incomming error

STOP_SENDING should only be sent for a non-reset stream

Endpoint is expected to send another STOP_SENDING if a packet containing a previous STOP_SENDING is lost
Is unnecessary if all data or RESET_STREAM has been received for the stream

### Flow Control

Can send control updates along with other frames, such as ACK frames

#### Data Flow Control

Receiver advertises the limit of total bytes it is prepared to receive on a given stream or for the entire connection
- Stream flow control
- Connection flow control

Must not send data in excess of either limit

Initial limits for all streams are set through transport parameters during handshake
Can increase limits with MAX_STREAM_DATA frame with the corresponding stream ID
MAX_STREAM_DATA indicates the maimum absolute byte offset of a stream
A receiver can calculate this based on the sum of bytes consumed on all streams

Once a receiver advertises a limit, advertising a smaller limit has no effect
A sender must ignore any MAX_STREAM_DATA or MAX_DATA frames that do not increase flow control limits

A receiver must close the connection with FLOW_CONTROL_ERROR if the sender violates the advertised connection or stream data limits

If a sender has sent data up to the limit, it is blocked, should send a STREAM_DATA_BLOCKED or DATA_BLOCKED frame
If blocked for longer than the idle timeout, receiver might close the connection

#### Increasing Flow Control Limits

Implementation decide how much to advertise in MAX_STREAM_DATA and MAX_DATA frames
These are considerations

To avoid blocking a sender, a receiver may send a MAX_STREAM_DATA or MAX_DATA frame multiple times within a round trip

Undesirable to send MAX_STREAM_DATA and MAX_DATA frames with small changes
Less frequent updates means more recources commitments at the receiver

#### Handling Stream Cancellation

Endpoints need to eventually agree on the amount of flow control credit that has been consumed on every stream

On receopt of a RESET_STREAM frame, an endpoint will tear down state for the matching stream and ignore further data arriving on that stream

RESET_STREAM terminates one direction of a stream abruptly
For a bidirectional stream, RESET_STREAM has no effect on data flow in the opposite direction
Both endpoints must maintain flow control in the unterminated direction until that direction enters a terminal state

#### Stream Final Size

Final size is the amount of flow control credit that is consumed by a stream
Assuming that every contiguous byte on the stream was sent once, the final size is the number of bytes sent
One higher than the offset of the byte with the largest offset sent on the stream, or zero if no bytes were sent

A sender always sends the final size of a stream to the receiver reliably, even when the stream is terminated
Final size is the sum of the Offset and Length fields of a STREAM frame with a FIN flag
These fields might be implicit
Final Size field of a RESET_STREAM frame carries this value

The receiver must use the final size of the stream to account for all bytes sent on the stream in its connection-level flow controller

Once final size is known, it cannot change
If a RESET_STREAM or STREAM frame indicates a change in the final size, should respond with an FINAL_SIZE_ERROR
Even after a stream is closed

#### Controlling Concurrency

An endpoint limits the cumulative number of incoming streams a peer can open
Only streams with id less than (max_streams * 4 + first_stream_id_of_type) can be opened

Initial limits are set in transport parameters
After that it is advertised using MAX_STREAMS frames

If a max_streams transport parameter or a MAX_STREAMS frame is received with a value greater than 2^60
-> close connection with an TRANSPORT_PARAMETER_ERROR if in transport parameter or FRAME_ENCODING_ERROR if it was received in a frame

Must not exceed the limit set by their peer
If receives frame with id exceeding the limit
-> Must treat as connection error of type STREAM_LIMIT_ERROR

MAX_STREAMS frames that do not increase the stream limit must be ignored

If unable to open stream, should send STREAM_BLOCKED frame
Useful for debugging

## Connections

Uses connection id to manage network changes
Will then use path validation to prevent spoofing
Something something about a challenge

## Encryption

# Notes About Implementation

https://www.andy-pearce.com/blog/posts/2023/Mar/http3-in-practice-quic/

Endpoint
- Like a socket, wraps around udp socket

Connection
- Shared state between endpoints
- Between two endpoints
- To do with handshakes and stuff

Stream
- Actual data being sent and received

Packet
- Part of an UDP datagram

Frame
- Parts of a packet
