//! The industrial round trips: modbus, bacnet, serial, can-bus, IEC 104 and
//! DNP3, each behind the same [`RoundTrip`] the pingpong scenario drives.
//!
//! These four carry small frames — a Modbus PDU is 253 bytes, a CAN frame
//! eight — so a Stream travels as a sequence: transactions in turn on one
//! Modbus connection, BVLC datagrams in order with an empty one to close,
//! CAN frames under one identifier until the bus is quiet. That is what the
//! protocols above them do (ISO-TP over CAN, a register block over Modbus),
//! and the adapter does it here so the scenario stays one thing over all of
//! them. Serial and CAN have no device on the build box: serial round-trips
//! through its framing over an in-memory line, CAN over the loopback bus.

use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;

use transport::Transport;
use transport_bacnet::BacnetTransport;
use transport_can_bus::{Bus, CanTransport, Loopback};
use transport_dnp3::Dnp3Transport;
use transport_iec_60870_5_104::Iec104Transport;
use transport_modbus::ModbusTransport;
use transport_serial::{Framing, SerialTransport};

use crate::roundtrip::{Exchange, RoundTrip, TIMEOUT, listen_exchange};

/// Modbus: bind a server, connect a client, send the payload as transactions
/// of at most one PDU each, the server echoing every request, and read the
/// stream back in order.
pub struct ModbusRoundTrip;

impl RoundTrip for ModbusRoundTrip {
    fn transport(&self) -> &'static str {
        "modbus"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = ModbusTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut connection = far_end.accept_one(listener)?;
                let mut origin = String::from("modbus://");
                let mut bytes = Vec::new();
                while let Some((arrived, header)) = connection.next_request()? {
                    connection.respond(header, &arrived.bytes)?;
                    origin = arrived.origin_uri;
                    bytes.extend_from_slice(&arrived.bytes);
                }
                Ok(transport::Arrived::new(origin, bytes))
            },
            |address| {
                let mut client = ModbusTransport::new("127.0.0.1:0")
                    .timing_out_after(timeout)
                    .connect(address)?;
                for pdu in payload.chunks(transport_modbus::MAX_PDU) {
                    if client.request(pdu)? != pdu {
                        return Err(transport::error::protocol_error("the echo differed"));
                    }
                }
                Ok(())
            },
        )
    }
}

/// BACnet/IP: bind a socket, send the payload as unicast NPDUs in order and an
/// empty one to close, and receive until the empty one.
pub struct BacnetRoundTrip;

impl RoundTrip for BacnetRoundTrip {
    fn transport(&self) -> &'static str {
        "bacnet"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = BacnetTransport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (socket, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let payload = payload.to_vec();
        let sender = std::thread::spawn(move || {
            let sender = BacnetTransport::new("127.0.0.1:0");
            for npdu in payload.chunks(transport_bacnet::MAX_DATAGRAM - 4) {
                sender.send(&address, npdu)?;
            }
            sender.send(&address, &[])
        });
        let mut bytes = Vec::new();
        let caught = loop {
            match far_end.receive_one(&socket) {
                Ok(arrived) if arrived.bytes.is_empty() => break Ok(()),
                Ok(arrived) => bytes.extend_from_slice(&arrived.bytes),
                Err(error) => break Err(error),
            }
        };
        match (caught, sender.join()) {
            (Ok(()), Ok(Ok(()))) => Exchange::Returned(bytes),
            (Err(error), _) => Exchange::Failed(format!("receive failed: {error}")),
            (_, Ok(Err(error))) => Exchange::Failed(format!("send failed: {error}")),
            (_, Err(_)) => Exchange::Failed("the sending thread panicked".to_string()),
        }
    }
}

/// Serial: frame the payload as the line would carry it, and read one frame
/// back through the same framing. The device is the one thing a build box
/// does not have; the framing is what a Location configures.
pub struct SerialRoundTrip;

impl RoundTrip for SerialRoundTrip {
    fn transport(&self) -> &'static str {
        "serial"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        // End-of-transmission closes a frame; a payload that carries one is
        // framed by length instead, as a line with fixed records would be.
        let framing = if payload.contains(&0x04) {
            Framing::Fixed(payload.len())
        } else {
            Framing::Delimited(vec![0x04])
        };
        let line = SerialTransport::new("loopback", 9600).framed(framing);
        let on_the_wire = match line.framed_bytes(payload) {
            Ok(bytes) => bytes,
            Err(error) => return Exchange::Failed(format!("framing failed: {error}")),
        };
        match line.read_one(&mut BufReader::new(on_the_wire.as_slice())) {
            Ok(arrived) => Exchange::Returned(arrived.bytes),
            Err(error) => Exchange::Failed(format!("reading the line failed: {error}")),
        }
    }
}

/// CAN: put the payload on the loopback bus as frames of at most eight bytes
/// under one identifier, and read the bus until it is quiet.
pub struct CanRoundTrip;

impl RoundTrip for CanRoundTrip {
    fn transport(&self) -> &'static str {
        "can-bus"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let bus: Arc<dyn Bus> = Arc::new(Loopback::new());
        let transport = CanTransport::new(bus, 0x181).timing_out_after(Duration::from_millis(10));
        for frame in payload.chunks(8) {
            if let Err(error) = transport.send("can://loopback/0x181", frame) {
                return Exchange::Failed(format!("send failed: {error}"));
            }
        }
        let mut bytes = Vec::new();
        loop {
            match transport.receive_one() {
                Ok(Some(arrived)) => bytes.extend_from_slice(&arrived.bytes),
                Ok(None) => return Exchange::Returned(bytes),
                Err(error) => return Exchange::Failed(format!("receive failed: {error}")),
            }
        }
    }
}

/// IEC 104: bind a controlled station, connect a controlling one, send the
/// payload as ASDUs of at most 249 bytes in sequence, each acknowledged,
/// and read the stream back in order.
pub struct Iec104RoundTrip;

impl RoundTrip for Iec104RoundTrip {
    fn transport(&self) -> &'static str {
        "iec-60870-5-104"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = Iec104Transport::new("127.0.0.1:0").timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut station = far_end.accept_one(listener)?;
                let mut origin = String::from("iec104://");
                let mut bytes = Vec::new();
                while let Some(arrived) = station.next_asdu()? {
                    origin = arrived.origin_uri;
                    bytes.extend_from_slice(&arrived.bytes);
                }
                Ok(transport::Arrived::new(origin, bytes))
            },
            |address| {
                let mut controller = Iec104Transport::new("127.0.0.1:0")
                    .timing_out_after(timeout)
                    .connect(address)?;
                for asdu in payload.chunks(transport_iec_60870_5_104::MAX_ASDU) {
                    controller.send_asdu(asdu)?;
                }
                controller.stop()
            },
        )
    }
}

/// DNP3: bind an outstation, connect a master, send the payload as one
/// fragment however many segments it takes, and read it back whole.
pub struct Dnp3RoundTrip;

impl RoundTrip for Dnp3RoundTrip {
    fn transport(&self) -> &'static str {
        "dnp3"
    }

    fn exchange(&self, payload: &[u8]) -> Exchange {
        let far_end = Dnp3Transport::new("127.0.0.1:0", 1024, 1).timing_out_after(TIMEOUT);
        let (listener, address) = match far_end.bind() {
            Ok(bound) => bound,
            Err(error) => return Exchange::Failed(format!("bind failed: {error}")),
        };
        let timeout = TIMEOUT;
        listen_exchange(
            listener,
            &address,
            move |listener| {
                let mut outstation = far_end.accept_one(listener)?;
                outstation.next_fragment()?.ok_or_else(|| {
                    transport::error::protocol_error("the master closed without a fragment")
                })
            },
            |address| {
                Dnp3Transport::new("127.0.0.1:0", 1, 1024)
                    .timing_out_after(timeout)
                    .send(address, payload)
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
    fn modbus_carries_a_stream_as_transactions() {
        let long = vec![0x2a; 1000];
        assert_eq!(returned(&ModbusRoundTrip, b"read holding"), b"read holding");
        assert_eq!(returned(&ModbusRoundTrip, &long), long);
        assert_eq!(returned(&ModbusRoundTrip, b""), b"");
    }

    #[test]
    fn bacnet_carries_a_stream_as_datagrams() {
        let long = vec![0x2a; 5000];
        assert_eq!(returned(&BacnetRoundTrip, b"who-is"), b"who-is");
        assert_eq!(returned(&BacnetRoundTrip, &long), long);
    }

    #[test]
    fn iec_104_and_dnp3_carry_a_stream_to_a_station() {
        let long = vec![0x2a; 3000];
        assert_eq!(returned(&Iec104RoundTrip, b"asdu"), b"asdu");
        assert_eq!(returned(&Iec104RoundTrip, &long), long);
        assert_eq!(returned(&Iec104RoundTrip, b""), b"");
        assert_eq!(returned(&Dnp3RoundTrip, b"fragment"), b"fragment");
        assert_eq!(returned(&Dnp3RoundTrip, &long), long);
        assert_eq!(returned(&Dnp3RoundTrip, b""), b"");
    }

    #[test]
    fn serial_and_can_round_trip_in_process() {
        assert_eq!(
            returned(&SerialRoundTrip, b"line\r\nbreak"),
            b"line\r\nbreak"
        );
        assert_eq!(
            returned(&SerialRoundTrip, b"eot\x04inside"),
            b"eot\x04inside"
        );
        assert_eq!(
            returned(&CanRoundTrip, b"seventeen bytes!!"),
            b"seventeen bytes!!"
        );
        assert_eq!(returned(&CanRoundTrip, b""), b"");
    }
}
