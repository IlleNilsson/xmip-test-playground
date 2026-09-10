//! The messaging round trips: mqtt, nats, amqp and kafka, each behind the same
//! [`RoundTrip`] the pingpong scenario drives.
//!
//! Both put a broker in the middle in production. The build box has none, so
//! each adapter binds the transport's own one-client session as the far end
//! — a broker's worth of protocol for one connection, which is what the
//! transport ships for a device that publishes straight to Xmip — and a
//! client connects, publishes the payload once, and what the session took is
//! what came back.

use transport::Transport;
use transport::error::protocol_error;
use transport_amqp::AmqpTransport;
use transport_kafka::KafkaTransport;
use transport_mqtt::MqttTransport;
use transport_nats::NatsTransport;

use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT, listen_exchange};

/// MQTT: bind a session, connect a client, publish the payload at `QoS` 1 on
/// one topic, and take it as the session's one PUBLISH.
pub struct MqttRoundTrip;

impl RoundTrip for MqttRoundTrip {
    fn transport(&self) -> &'static str {
        "mqtt"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = MqttTransport::new("127.0.0.1:0", "probe").timing_out_after(TIMEOUT);
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
                    .next_publish()?
                    .ok_or_else(|| protocol_error("the client disconnected without publishing"))
            },
            |address| {
                MqttTransport::new(address, "probe")
                    .timing_out_after(timeout)
                    .send(&format!("mqtt://{address}/probe"), payload)
            },
        )
    }
}

/// NATS: bind a session, connect a client, publish the payload on one
/// subject and flush, and take it as the session's one PUB.
pub struct NatsRoundTrip;

impl RoundTrip for NatsRoundTrip {
    fn transport(&self) -> &'static str {
        "nats"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = NatsTransport::new("127.0.0.1:0", "probe").timing_out_after(TIMEOUT);
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
                // The client flushes with PING after PUB and waits for the
                // PONG; serve it, and see the client close.
                session.next_publish()?;
                Ok(arrived)
            },
            |address| {
                NatsTransport::new(address, "probe")
                    .timing_out_after(timeout)
                    .send(&format!("nats://{address}/probe"), payload)
            },
        )
    }
}

/// AMQP: bind a session, connect a client, publish the payload once to an
/// exchange under a routing key, and take it as the session's one publish.
pub struct AmqpRoundTrip;

impl RoundTrip for AmqpRoundTrip {
    fn transport(&self) -> &'static str {
        "amqp"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end =
            AmqpTransport::new("127.0.0.1:0", "probe", "probe", "probe").timing_out_after(TIMEOUT);
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
                // Serve the close that follows, so the goodbye is answered.
                session.next_publish()?;
                Ok(arrived)
            },
            |address| {
                AmqpTransport::new(address, "probe", "probe", "probe")
                    .timing_out_after(timeout)
                    .send("probe", payload)
            },
        )
    }
}

/// Kafka: bind a session, connect a client, produce the payload as one
/// record, and take it as the session's one produce.
pub struct KafkaRoundTrip;

impl RoundTrip for KafkaRoundTrip {
    fn transport(&self) -> &'static str {
        "kafka"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = KafkaTransport::new("127.0.0.1:0", "probe").timing_out_after(TIMEOUT);
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
                KafkaTransport::new(address, "probe")
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
    fn mqtt_and_nats_carry_a_stream_through_a_session() {
        let long = vec![0x2a; 100_000];
        assert_eq!(returned(&MqttRoundTrip, b"publish"), b"publish");
        assert_eq!(returned(&MqttRoundTrip, &long), long);
        assert_eq!(returned(&MqttRoundTrip, b""), b"");
        assert_eq!(returned(&NatsRoundTrip, b"pub\r\nlished"), b"pub\r\nlished");
        assert_eq!(returned(&NatsRoundTrip, &long), long);
        assert_eq!(returned(&NatsRoundTrip, b""), b"");
        assert_eq!(returned(&AmqpRoundTrip, b"published"), b"published");
        assert_eq!(returned(&AmqpRoundTrip, &long), long);
        assert_eq!(returned(&AmqpRoundTrip, b""), b"");
        assert_eq!(returned(&KafkaRoundTrip, b"record"), b"record");
        assert_eq!(returned(&KafkaRoundTrip, &long), long);
        assert_eq!(returned(&KafkaRoundTrip, b""), b"");
    }

    #[test]
    fn mqtt_carries_the_edges() {
        crate::support::carries_the_edges(&MqttRoundTrip);
    }

    #[test]
    fn nats_carries_the_edges() {
        crate::support::carries_the_edges(&NatsRoundTrip);
    }

    #[test]
    fn amqp_carries_the_edges() {
        crate::support::carries_the_edges(&AmqpRoundTrip);
    }

    #[test]
    fn kafka_carries_the_edges() {
        crate::support::carries_the_edges(&KafkaRoundTrip);
    }
}
