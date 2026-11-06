import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

HEADER_SIZE = 16

TYPE_START = 0
TYPE_END = 1
TYPE_DATA = 2
TYPE_ACK = 3


def make_ack(seq_num):
    header = PacketHeader(type=TYPE_ACK, seq_num=seq_num, length=0, checksum=0)
    pkt_bytes = bytes(header)
    header.checksum = compute_checksum(pkt_bytes)
    return bytes(header)


def receiver(receiver_ip, receiver_port, window_size):
    """Listen on socket and print received message to sys.stdout."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    sender_addr = None
    expected_seq = 0  # starts expecting START (seq_num = 0)
    buffer = {}  # seq_num -> payload bytes
    in_connection = False

    while True:
        pkt, address = s.recvfrom(2048)
        if len(pkt) < HEADER_SIZE:
            continue

        pkt_header = PacketHeader(pkt[:HEADER_SIZE])
        msg = pkt[HEADER_SIZE : HEADER_SIZE + pkt_header.length]

        # Verify checksum; drop packet if checksum does not match
        pkt_checksum = pkt_header.checksum
        pkt_header.checksum = 0
        computed_checksum = compute_checksum(pkt_header / msg)
        if pkt_checksum != computed_checksum:
            continue

        pkt_type = pkt_header.type
        seq_num = pkt_header.seq_num

        # If not connected yet, wait for START packet
        if not in_connection:
            if pkt_type == TYPE_START and seq_num == 0:
                in_connection = True
                sender_addr = address
                expected_seq = 1
                # Send ACK for START packet (ack seq_num = 1)
                ack_pkt = make_ack(1)
                s.sendto(ack_pkt, sender_addr)
            continue

        # In connection: ignore packets from other senders
        if address != sender_addr:
            continue

        # Ignore redundant START packets while in the middle of a connection
        if pkt_type == TYPE_START:
            # Resend ACK for START in case sender's START ACK was lost/delayed
            ack_pkt = make_ack(1)
            s.sendto(ack_pkt, sender_addr)
            continue

        if pkt_type == TYPE_DATA:
            # Drop all packets with seq_num >= expected_seq + window_size
            if seq_num >= expected_seq + window_size:
                continue

            # Send individual ACK with seq_num set to whatever it was in the data packet
            ack_pkt = make_ack(seq_num)
            s.sendto(ack_pkt, sender_addr)

            if seq_num >= expected_seq:
                buffer[seq_num] = msg
                # Advance expected_seq and deliver consecutive buffered packets
                while expected_seq in buffer:
                    payload = buffer.pop(expected_seq)
                    if hasattr(sys.stdout, "buffer"):
                        sys.stdout.buffer.write(payload)
                        sys.stdout.buffer.flush()
                    else:
                        sys.stdout.write(payload.decode("utf-8", errors="replace"))
                        sys.stdout.flush()
                    expected_seq += 1

        elif pkt_type == TYPE_END:
            # Check if all packets prior to END have been delivered
            if seq_num <= expected_seq:
                ack_pkt = make_ack(seq_num)
                s.sendto(ack_pkt, sender_addr)
                # Exit once it sends the ACK packet of the END message
                break
            else:
                # If END arrives out-of-order, send ACK for the current expected_seq
                ack_pkt = make_ack(expected_seq)
                s.sendto(ack_pkt, sender_addr)

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

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
