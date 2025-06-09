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

def get_packet_batch_from_reader(reader, batch_size):
    batch = []
    count = 0

    while count < batch_size:
        try:
            pkt = reader.read_packet()
            if pkt is None:
                break  # EOF
            timestamp = getattr(pkt, 'time', time.time())
            batch.append({
                "timestamp": str(timestamp),
                "pcap_bytes": base64.b64encode(raw(pkt)).decode('utf-8')
            })
            count += 1
        except Exception as e:
            logging.info(f"Error reading packet: {e}")
            break

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

            if file_idx >= len(pcap_files):
                logging.info(f"Subscription {sub_id} finished all files.")
                continue

            current_pcap = pcap_files[file_idx]

            # Open PcapReader if not already open
            if 'pcap_reader' not in sub:
                try:
                    sub['pcap_reader'] = PcapReader(current_pcap)
                    logging.info(f"Opened PcapReader for subscription {sub_id}, file {current_pcap}")
                except Exception as e:
                    logging.info(f"Failed to open PCAP file for {sub_id}: {e}")
                    continue

            reader = sub['pcap_reader']
            packet_batch = get_packet_batch_from_reader(reader, BATCH_SIZE)

            if not packet_batch:
                # End of file
                reader.close()
                sub.pop('pcap_reader', None)
                sub['file_index'] = file_idx + 1
                sub['last_index'] = 0
                logging.info(f"Finished file {current_pcap}, moving subscription {sub_id} to next file.")
                continue

            payload = {
                "event": sub["event"],
                "pcap": packet_batch
            }

            try:
                response = requests.post(uri, json=payload)
                if response.status_code == 200:
                    sub['last_index'] = sub.get('last_index', 0) + BATCH_SIZE
                else:
                    logging.info(f"Error {response.status_code} from {uri}")
            except Exception as e:
                logging.info(f"Failed to notify {uri}: {e}")

        time.sleep(5)