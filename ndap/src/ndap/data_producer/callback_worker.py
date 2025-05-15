import os
import time
import glob
import requests
import base64
import logging
from scapy.utils import PcapReader
from scapy.all import raw

BATCH_SIZE = 1000
PCAP_DIR = "dataset_files"

def get_sorted_pcap_files():
    """Returns a sorted list of PCAP files in the dataset directory."""
    return sorted(
        glob.glob(os.path.join(PCAP_DIR, "*.pcap")),
        key=lambda x: int(os.path.splitext(os.path.basename(x))[0])
    )

def get_packet_batch(pcap_path, start_index, batch_size):
    """Reads a batch of packets from the PCAP file, starting at a given index."""
    batch = []
    with PcapReader(pcap_path) as reader:
        for i, pkt in enumerate(reader):
            if i < start_index:
                continue
            
            if i >= start_index + batch_size:
                break

            try:
                timestamp = getattr(pkt, 'time', time.time())

                batch.append({
                    "timestamp": str(timestamp),
                    "pcap_bytes": base64.b64encode(raw(pkt)).decode('utf-8')
                })

            except Exception as e:
                logging.info(f"Error processing packet {i}: {e}")
    return batch

def subscription_callback_worker(subscriptions, _, subscription_ready_event):
    subscription_ready_event.wait()

    pcap_files = get_sorted_pcap_files()
    if not pcap_files:
        logging.info("No PCAP files found in directory.")
        return

    while True:
        for sub_id, sub in list(subscriptions.items()):
            uri = sub['notificationURI']
            file_idx = sub.get('file_index', 0)
            packet_idx = sub.get('last_index', 0)

            if file_idx >= len(pcap_files):
                logging.info(f"Subscription {sub_id} finished all files.")
                continue

            current_pcap = pcap_files[file_idx]
            packet_batch = get_packet_batch(current_pcap, packet_idx, BATCH_SIZE)

            if not packet_batch:
                # Move to next file
                subscriptions[sub_id]['file_index'] = file_idx + 1
                subscriptions[sub_id]['last_index'] = 0

                logging.info(f"Moving subscription {sub_id} to next file.")
                continue

            payload = {
                "event": sub["event"],
                "pcap": packet_batch
            }

            try:
                response = requests.post(uri, json=payload)

                if response.status_code == 200:
                    subscriptions[sub_id]['last_index'] = packet_idx + BATCH_SIZE

                else:
                    logging.info(f"Error {response.status_code} from {uri}")

            except Exception as e:
                logging.info(f"Failed to notify {uri}: {e}")

        time.sleep(5)