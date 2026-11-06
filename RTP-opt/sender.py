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
    s.settimeout(0.01)

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

    # Step 3: Sliding window transmission with Selective Repeat / Individual ACKs
    # Maintain ACK status for packets
    acked = [False] * total_packets
    base = 0  # index in packets corresponding to oldest unacknowledged packet
    next_to_send = 0  # index in packets to be sent for the first time
    timer_start = None

    s.settimeout(0.01)

    while base < total_packets:
        # Send new packets up to window_size
        while next_to_send < base + window_size and next_to_send < total_packets:
            s.sendto(packets[next_to_send][1], recv_addr)
            if timer_start is None:
                timer_start = time.time()
            next_to_send += 1

        # Drain all currently available ACKs in socket buffer
        window_advanced = False
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
                            ack_idx = ack_seq - 1
                            if 0 <= ack_idx < total_packets:
                                acked[ack_idx] = True
                                # Slide base forward if base packet was acked
                                old_base = base
                                while base < total_packets and acked[base]:
                                    base += 1
                                if base > old_base:
                                    window_advanced = True
            except (socket.timeout, BlockingIOError):
                break

        # If window advanced, reset or clear timer
        if window_advanced:
            if base < next_to_send:
                timer_start = time.time()
            else:
                timer_start = None

        # Check timeout: 500 ms retransmission timer for current window
        if timer_start is not None and (time.time() - timer_start) >= TIMEOUT:
            # Retransmit ONLY packets in current window that have NOT been acknowledged
            window_end = min(base + window_size, next_to_send)
            for idx in range(base, window_end):
                if not acked[idx]:
                    s.sendto(packets[idx][1], recv_addr)
            timer_start = time.time()

    # Step 4: Teardown with END packet
    end_seq = total_packets + 1
    end_pkt = make_packet(TYPE_END, end_seq)

    start_end_time = time.time()
    s.sendto(end_pkt, recv_addr)
    last_send = time.time()

    while time.time() - start_end_time < TIMEOUT:
        try:
            s.settimeout(0.1)
            pkt, _ = s.recvfrom(2048)
            if len(pkt) >= HEADER_SIZE:
                header = PacketHeader(pkt[:HEADER_SIZE])
                pkt_checksum = header.checksum
                header.checksum = 0
                payload = pkt[HEADER_SIZE : HEADER_SIZE + header.length]
                if compute_checksum(header / payload) == pkt_checksum:
                    if header.type == TYPE_ACK and header.seq_num == end_seq:
                        break
        except (socket.timeout, BlockingIOError):
            if time.time() - last_send >= 0.15:
                s.sendto(end_pkt, recv_addr)
                last_send = time.time()

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
