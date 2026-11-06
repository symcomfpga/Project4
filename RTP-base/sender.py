import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

# Constants based on RTP specifications
HEADER_SIZE = 16
MAX_PACKET_SIZE = 1472
MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE  # 1456 bytes
TIMEOUT = 0.5  # 500 ms retransmission timer

TYPE_START = 0
TYPE_END = 1
TYPE_DATA = 2
TYPE_ACK = 3


def make_packet(pkt_type, seq_num, payload=b""):
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    header = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(payload), checksum=0)
    pkt_bytes = bytes(header / payload)
    header.checksum = compute_checksum(pkt_bytes)
    return bytes(header / payload)


def sender(receiver_ip, receiver_port, window_size):
    """Open socket and reliably send message from sys.stdin using RTP."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0.01)  # non-blocking / fine-grained timeout for timer checking

    recv_addr = (receiver_ip, receiver_port)

    # Read all data from stdin (supports binary or text)
    if hasattr(sys.stdin, "buffer"):
        data = sys.stdin.buffer.read()
    else:
        data = sys.stdin.read().encode("utf-8")

    # Step 1: Handshake with START packet (seq_num = 0)
    start_pkt = make_packet(TYPE_START, 0)
    while True:
        s.sendto(start_pkt, recv_addr)
        try:
            s.settimeout(0.5)
            pkt, _ = s.recvfrom(2048)
            if len(pkt) >= HEADER_SIZE:
                header = PacketHeader(pkt[:HEADER_SIZE])
                pkt_checksum = header.checksum
                header.checksum = 0
                payload = pkt[HEADER_SIZE : HEADER_SIZE + header.length]
                if compute_checksum(header / payload) == pkt_checksum:
                    if header.type == TYPE_ACK and header.seq_num == 1:
                        break
        except socket.timeout:
            pass

    # Step 2: Chunk the data into DATA packets (seq_num starts from 1)
    packets = []
    seq = 1
    for i in range(0, len(data), MAX_PAYLOAD_SIZE):
        chunk = data[i : i + MAX_PAYLOAD_SIZE]
        packets.append((seq, make_packet(TYPE_DATA, seq, chunk)))
        seq += 1

    total_packets = len(packets)

    # Step 3: Sliding window transmission (Go-Back-N / Cumulative ACK)
    base = 0  # index in packets corresponding to oldest unacknowledged packet
    next_to_send = 0  # index in packets to be sent next
    timer_start = None

    s.settimeout(0.01)

    while base < total_packets:
        # Send packets up to window_size
        while next_to_send < base + window_size and next_to_send < total_packets:
            s.sendto(packets[next_to_send][1], recv_addr)
            if timer_start is None:
                timer_start = time.time()
            next_to_send += 1

        # Drain all currently available ACKs in socket buffer
        while True:
            try:
                pkt, _ = s.recvfrom(2048)
                if len(pkt) >= HEADER_SIZE:
                    header = PacketHeader(pkt[:HEADER_SIZE])
                    pkt_checksum = header.checksum
                    header.checksum = 0
                    payload = pkt[HEADER_SIZE : HEADER_SIZE + header.length]
                    if compute_checksum(header / payload) == pkt_checksum:
                        if header.type == TYPE_ACK:
                            ack_seq = header.seq_num
                            new_base = ack_seq - 1
                            if new_base > base:
                                base = new_base
                                if base < next_to_send:
                                    timer_start = time.time()
                                else:
                                    timer_start = None
            except (socket.timeout, BlockingIOError):
                break

        # Check timeout
        if timer_start is not None and (time.time() - timer_start) >= TIMEOUT:
            # Retransmit all unacknowledged packets in current window
            for idx in range(base, min(base + window_size, total_packets)):
                s.sendto(packets[idx][1], recv_addr)
            timer_start = time.time()

    # Step 4: Teardown with END packet
    # seq_num for END: according to spec, seq is total_packets + 1
    end_seq = total_packets + 1
    end_pkt = make_packet(TYPE_END, end_seq)

    s.sendto(end_pkt, recv_addr)
    end_timer = time.time()

    while time.time() - end_timer < TIMEOUT:
        try:
            s.settimeout(max(0.0, TIMEOUT - (time.time() - end_timer)))
            pkt, _ = s.recvfrom(2048)
            if len(pkt) >= HEADER_SIZE:
                header = PacketHeader(pkt[:HEADER_SIZE])
                pkt_checksum = header.checksum
                header.checksum = 0
                payload = pkt[HEADER_SIZE : HEADER_SIZE + header.length]
                if compute_checksum(header / payload) == pkt_checksum:
                    if header.type == TYPE_ACK:
                        break
        except socket.timeout:
            break

    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
