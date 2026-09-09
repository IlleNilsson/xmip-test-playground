//! The broker round trips: activemq, rabbitmq, nats-jetstream, redpanda
//! and postgresql, each behind the same [`RoundTrip`] the pingpong scenario
//! drives.
//!
//! Every one of these puts a server in the middle in production — a STOMP
//! broker, an AMQP broker, a `JetStream` server, a Kafka broker, a database.
//! The build box has none, so each adapter binds the transport's own
//! one-client session as the far end — a server's worth of protocol for one
//! connection, which is what the transport ships for a producer that talks
//! straight to Xmip — and a client connects, sends the payload once, and
//! what the session took is what came back.

use transport::Transport;
use transport::error::protocol_error;
use transport_activemq::ActiveMqTransport;
use transport_nats_jetstream::JetStreamTransport;
use transport_postgresql::PostgresTransport;
use transport_rabbitmq::RabbitMqTransport;
use transport_redpanda::RedpandaTransport;

use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT, listen_exchange};

/// `ActiveMQ`: bind a STOMP session, connect a client, SEND the payload to
/// one queue with a receipt, and take it as the session's one SEND.
pub struct ActiveMqRoundTrip;

impl RoundTrip for ActiveMqRoundTrip {
    fn transport(&self) -> &'static str {
        "activemq"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end =
            ActiveMqTransport::new("127.0.0.1:0", "/queue/probe").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut session = far_end.accept_one(listener)?;
                let arrived = session
                    .next_send()?
                    .ok_or_else(|| protocol_error("the client disconnected without sending"))?;
                // The client DISCONNECTs with a receipt and waits for it;
                // serve it, and see the client go.
                session.next_send()?;
                Ok(arrived)
            },
            |address| {
                ActiveMqTransport::new(address, "/queue/probe")
                    .timing_out_after(timeout)
                    .send("/queue/probe", payload)
            },
        )
    }
}

/// `JetStream`: bind a session serving one stream over one subject, connect a
/// client, publish the payload on that subject and wait for the stream's
/// acknowledgement by sequence, and take it as the session's one publish.
pub struct JetStreamRoundTrip;

impl RoundTrip for JetStreamRoundTrip {
    fn transport(&self) -> &'static str {
        "nats-jetstream"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end =
            JetStreamTransport::new("127.0.0.1:0", "probe", "probe").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut session = far_end.accept_one(listener)?;
                // The acknowledgement goes out before the publish is reported,
                // so the client has its sequence by the time this returns.
                session
                    .next_publish()?
                    .ok_or_else(|| protocol_error("the client closed without publishing"))
            },
            |address| {
                JetStreamTransport::new(address, "probe", "probe")
                    .timing_out_after(timeout)
                    .send("probe", payload)
            },
        )
    }
}

/// Redpanda: on the wire it is Kafka, so this is the kafka round with the
/// redpanda transport at both ends — bind a session, connect a client,
/// produce the payload as one record, and take it as the session's one
/// produce. The Admin API is not consulted; a send never does.
pub struct RedpandaRoundTrip;

impl RoundTrip for RedpandaRoundTrip {
    fn transport(&self) -> &'static str {
        "redpanda"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = RedpandaTransport::new("127.0.0.1:0", "probe").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut session = far_end.accept_one(listener)?;
                session
                    .next_produce()?
                    .ok_or_else(|| protocol_error("the client closed without producing"))
            },
            |address| {
                RedpandaTransport::new(address, "probe")
                    .timing_out_after(timeout)
                    .send("probe", payload)
            },
        )
    }
}

/// `PostgreSQL`: bind a session, log a client in by trust, INSERT the payload
/// as one column of one row — text as text, anything else in the bytea hex
/// form — and take it as the session's one insert, the bytes again.
pub struct PostgresqlRoundTrip;

impl RoundTrip for PostgresqlRoundTrip {
    fn transport(&self) -> &'static str {
        "postgresql"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end =
            PostgresTransport::new("127.0.0.1:0", "probe", "probe").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut session = far_end.accept_one(listener)?;
                let arrived = session
                    .next_insert()?
                    .ok_or_else(|| protocol_error("the client closed without inserting"))?;
                // Read the Terminate that follows, so the goodbye is taken
                // rather than written into a closed socket.
                session.next_insert()?;
                Ok(arrived)
            },
            |address| {
                PostgresTransport::new(address, "probe", "probe")
                    .timing_out_after(timeout)
                    .send("probe/payload", payload)
            },
        )
    }
}

/// `RabbitMQ`: bind an AMQP 0-9-1 session, connect a client as guest,
/// declare the queue and publish the payload to it through the default
/// exchange, and take it as the session's one publish.
pub struct RabbitMqRoundTrip;

impl RoundTrip for RabbitMqRoundTrip {
    fn transport(&self) -> &'static str {
        "rabbitmq"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = RabbitMqTransport::new("127.0.0.1:0", "probe").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut session = far_end.accept_one(listener)?;
                let arrived = session
                    .next_publish()?
                    .ok_or_else(|| protocol_error("the client closed without publishing"))?;
                // The client closes the channel and the connection and waits
                // for each -ok; serve them, and see the client go.
                session.next_publish()?;
                Ok(arrived)
            },
            |address| {
                RabbitMqTransport::new(address, "probe")
                    .timing_out_after(timeout)
                    .send("probe", payload)
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn returned(rt: &dyn RoundTrip, payload: &[u8]) -> Vec<u8> {
        match rt.exchange(payload) {
            Exchange::Returned(bytes) => bytes,
            Exchange::OneSided(why) | Exchange::Failed(why) => {
                panic!("{} did not return: {why}", rt.transport())
            }
        }
    }

    #[test]
    fn activemq_carries_a_stream_through_a_session() {
        let long = vec![0x2a; 3000];
        assert_eq!(returned(&ActiveMqRoundTrip, b"sent"), b"sent");
        assert_eq!(returned(&ActiveMqRoundTrip, &long), long);
        assert_eq!(returned(&ActiveMqRoundTrip, b""), b"");
    }

    #[test]
    fn nats_jetstream_carries_a_stream_through_a_session() {
        let long = vec![0x2a; 3000];
        assert_eq!(
            returned(&JetStreamRoundTrip, b"pub\r\nlished"),
            b"pub\r\nlished"
        );
        assert_eq!(returned(&JetStreamRoundTrip, &long), long);
        assert_eq!(returned(&JetStreamRoundTrip, b""), b"");
    }

    #[test]
    fn redpanda_carries_a_stream_through_a_session() {
        let long = vec![0x2a; 3000];
        assert_eq!(returned(&RedpandaRoundTrip, b"record"), b"record");
        assert_eq!(returned(&RedpandaRoundTrip, &long), long);
        assert_eq!(returned(&RedpandaRoundTrip, b""), b"");
    }

    #[test]
    fn rabbitmq_carries_a_stream_through_a_session() {
        let long = vec![0x2a; 3000];
        assert_eq!(returned(&RabbitMqRoundTrip, b"published"), b"published");
        assert_eq!(returned(&RabbitMqRoundTrip, &long), long);
        assert_eq!(returned(&RabbitMqRoundTrip, b""), b"");
    }

    #[test]
    fn postgresql_carries_text_and_bytes_through_a_session() {
        let long = vec![0x2a; 3000];
        assert_eq!(returned(&PostgresqlRoundTrip, b"it's here"), b"it's here");
        assert_eq!(returned(&PostgresqlRoundTrip, &long), long);
        assert_eq!(returned(&PostgresqlRoundTrip, b""), b"");
        assert_eq!(returned(&PostgresqlRoundTrip, &[0xff, 0xfe]), [0xff, 0xfe]);
        assert_eq!(returned(&PostgresqlRoundTrip, b"a\0b"), b"a\0b");
    }
}
