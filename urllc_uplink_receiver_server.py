import socket
import time
import math
import json
from threading import Thread
from multiprocessing import Process, Manager, Value, Queue, Array
import select
import queue
import csv
import os
from socket import timeout as socket_timeout
import asyncio


def get_json_file(json_path):
    while True:
        try:
            config_file = open(json_path)
            node_parameters = json.load(config_file)
            config_file.close()
            return node_parameters
        except json.decoder.JSONDecodeError:
            print("failed to open (", json_path, "). Will try again.")
            # time.sleep(1)


def get_bs_col_ip():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["node_type"] == "bs":
            col_ip = node_dict["col_ip"]
            return col_ip


def get_client_lte_info(client_col_address):
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["col_ip"] == client_col_address:
            client_lte_ip = node_dict["lte_ip"]
            imsi = node_dict["ue_imsi"]
            return client_lte_ip, node_id, imsi


def get_my_info():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["this_is_my_node"]:
            return node_dict


def get_client_lte_info_by_node_id(target_node_id):
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if int(node_id) == int(target_node_id):
            return node_dict


def get_all_ue_dicts():
    ue_list = []
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["node_type"] == "ue":
            ue_list.append(node_dict)
    return ue_list


def unpack_header(message):
    # global traffic_measurements
    # print("message size:", len(message))
    # message_size includes the 30-bytes header.
    timestamp = message[:20]
    timestamp = float(timestamp)
    # current_time = time.time()

    sequence_number = message[20:30]
    # print("new message_size", message_size)
    sequence_number = int(sequence_number)
    # print("sequence_number", sequence_number)

    # traffic_measurements.put([len(message), delay, sequence_number])
    return timestamp, sequence_number
    # return message_size - header_size


def self_close_udp_socket(client_col_ip):

    client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_col_ip)
    client_receiving_port = 9900 + int(client_imsi[13:])
    closing_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    closing_message = bytes('', 'utf')

    while True:
        sent = closing_socket.sendto(closing_message, (lte_ip, client_receiving_port))
        client_dict = process_dict[client_node_id]
        if client_dict["status"] == "down":
            break
        # time.sleep(1)
    closing_socket.close()


def receive_up_to(col_socket, buffer):

    while True:
        try:
            received_message = b''
            while len(received_message) < buffer:
                remaining_bytes = col_socket.recv(buffer - len(received_message))
                if len(remaining_bytes) == 0:
                    print("Master node closed the support connection")
                    col_socket.close()
                    return -1
                received_message = received_message + remaining_bytes
            return received_message
        except KeyboardInterrupt:
            print("\nCtrl+C on receive_up_to")
            col_socket.close()
            return -1
"""
This function can terminate by three different events:
- by detecting client_dict["status"] == "down" which is set by the measurement function.
- by timing out on recvfrom then detecting client_dict["status"] == "down".
- by the dedicated self_close_udp_socket function (which is enough to cover the previous events).
"""
# Start receiving packets from UE over LTE.
def start_uplink_receiver(client_col_ip):

    client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_col_ip)
    uplink_receiver_port = 9900 + int(client_imsi[13:])
    uplink_receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    uplink_receiver_socket.bind((lte_ip, uplink_receiver_port))
    uplink_receiver_socket.settimeout(5)  # Currently not needed.

    # node_dict = {}
    # node_dict["client_node_id"] = client_node_id
    # node_dict["client_col_ip"] = client_col_ip
    # node_dict["client_col_ip"] = client_col_ip
    # clients_dict[client_node_id] = node_dict

    my_slice_queue = slice_1_queue

    counter = 0
    # sequence = 0
    total_received = 0
    current_time = time.time()

    while True:
        try:
            # Currently not needed because now we have self_close_udp_socket.
            # client_dict = process_dict[client_node_id]
            # if client_dict["status"] == "down":
            #     print("Signal from measurement process to close receiver process for node", client_node_id)
            #     break

            data, addr = uplink_receiver_socket.recvfrom(MTU)
            receive_time = time.time()

            if len(data) == 0:
                print("close message received - injection process closed from the client side")
                # To inform the self-closer that the socket is closed successfully.
                client_dict = process_dict[client_node_id]
                client_dict["status"] = "down"
                process_dict[client_node_id] = client_dict
                break

            timestamp, sequence_number = unpack_header(data)
            packet_size = len(data)
            packet_report = [client_node_id, receive_time, timestamp, sequence_number, packet_size]
            my_slice_queue.put(packet_report)

            # total_received = total_received + len(data)
            # counter = counter + 1

            # if (receive_time - current_time) > 1:
            #     current_traffic = total_received * 8 / 1000000
            #     current_traffic = '{0:.3f}'.format(current_traffic)
            #     print(current_traffic, "Mbits received within", '{0:.7f}'.format(time.time() - current_time),
            #           "seconds from node ", client_node_id, "- current sequence number:", sequence_number)
            #     total_received = 0
            #     current_time = time.time()

        except socket_timeout:
            # client_dict = process_dict[client_node_id]
            # if client_dict["status"] == "down":
            #     print("timeout - closing receiver process for node", client_node_id)
            #     break
            print("Process hung for 5 seconds.")
            # restart_switch.value = True  # Inform the main process to restart me.
            # traffic_socket.close()
            # return
        except KeyboardInterrupt:
            print("\nCtrl+C on receiver process for node", client_node_id)
            break

    print("total received", counter, "messages")
    print("closing down uplink receiver process for node", client_node_id)


# Using TCP on Colosseum's internal network.
# def start_measurements_connection(client_socket, client_address):
#
#     client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_address[0])
#
#     while True:
#         try:
#             message = client_socket.recv(1000)
#             if len(message) == 0:
#                 print("client", client_node_id, "closed the measurement socket")
#                 self_close_udp_socket(client_address[0])
#                 break
#         except KeyboardInterrupt:
#             print("\nCtrl+C on measurement process for node", client_node_id)
#             break
#
#     client_socket.shutdown(2)
#     client_socket.close()

    # I don't think it is necessary anymore because now we have self_close_udp_socket.
    # client_dict = process_dict[client_node_id]
    # client_dict["status"] = "down"
    # process_dict[client_node_id] = client_dict

    # print("closing down measurement process for node", client_node_id)

    # sender_thread = Thread(target=backbone_sender, args=(client_socket, client_address,))
    # sender_thread.start()

    # Current thread is the receiver thread.

    # while True:
    #     try:
    #         client_dict = process_dict[client_node_id]
    #         if client_dict["status"] == "down":
    #             print("timeout - closing receiver process for node", client_node_id)
    #             break
    #         time.sleep(1)
    #     except KeyboardInterrupt:
    #         client_dict = process_dict[client_node_id]
    #         client_dict["status"] = "down"
    #         process_dict[client_node_id] = client_dict
    #         print("\nCtrl+C on receiver process for node", client_node_id)
    #         break

    # sent = client_socket.send(bytes("close", 'utf'))


def start_receiving_connections():

    # A TCP server socket set into the internal Colosseum network.
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    while True:
        try:
            server_socket.bind((col_ip, col_port))
            server_socket.listen(10)
            print("Starting a TCP server on", col_ip, "at port", col_port)
            break
        except OSError:
            print("Couldn't bind on IP", col_ip, "and port", col_port)

    while True:
        try:
            client_socket, client_address = server_socket.accept()
            print("UE", client_address[0], "connected from port", client_address[1], "on the Colosseum internal network")

            client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_address[0])
            client_dict = {}
            client_dict["status"] = "up"
            client_dict["internal_socket"] = client_socket
            process_dict[client_node_id] = client_dict

            client_uplink_injection_process = Process(target=start_uplink_receiver, args=(client_address[0],))
            client_uplink_injection_process.start()

            # client_measurement_process = Process(target=start_measurements_connection, args=(client_socket, client_address,))
            # client_measurement_process.start()

        # This happens when closing a socket on accept().
        except ConnectionAbortedError:
            print("Server socket closed")
            server_socket.close()
            break

        except KeyboardInterrupt:
            print("\nCtrl+C: closing down entire server")
            # for node_id, process_node_dict in process_dict.items():
            #     print("node_dict:", process_node_dict)
            #     process_node_dict["status"] = "down"
            #     process_node_dict["internal_socket"].close()
                # server_socket.shutdown(2)
            server_socket.close()
            break

    print("closing down main server process")


def start_slice_manager(my_slice_id):

    slice_configuration = get_json_file("radio_api/slice_configuration.txt")
    for slice_id, slice_dict in slice_configuration.items():
        if int(slice_id) == my_slice_id:
            housed_nodes = slice_dict["housed_nodes"]
            max_bandwidth = slice_dict["max_bandwidth"]  # in bytes
            break

    # for key, val in slice_dict.items():
    #     print(key, val)
    # print("first node:", housed_nodes[0], "- type:", type(housed_nodes[0]))
    print("max_bandwidth:", max_bandwidth * 8 / 1000000, "Mbps")

    global datasets_queues
    datasets_queues = {}

    number_of_nodes = len(housed_nodes)
    slice_max_buffer_size = max_bandwidth
    node_max_buffer_size = slice_max_buffer_size / number_of_nodes  # Divide the slice buffer equally among slice nodes.
    node_current_buffer_size = 0  # Every node starts with an empty buffer.

    counter = 0
    thread_list = []
    for node_id in housed_nodes:
        # node_queue = queue.Queue()
        node_id = str(node_id)
        node_dict = {}
        node_dict["my_queue"] = queue.Queue()  # To deliver the packets to the dataset creation thread.
        node_dict["my_buffer"] = node_current_buffer_size  # If the buffer is filled, the next packet will be marked as 'dropped'.
        node_dict["my_id"] = node_id
        datasets_queues[node_id] = node_dict
        client_dataset_thread = Thread(target=create_dataset, args=(node_id, node_dict["my_queue"],))
        client_dataset_thread.start()
        counter += 1
        thread_list.append(client_dataset_thread)
        # print("node_id type", type(node_id))

    print("created", counter, "dataset threads at slice manager", slice_id)

    counter = 0
    skip_counter = 0
    # sequence = 0
    # total_received = 0
    total_pkts = 0
    current_time = time.time()
    last_second = time.time()
    # current_slice_buffer = 0  # In bytes.

    while True:
        try:

            # packet_report = slice_1_queue.get(block=False)
            packet_report = slice_1_queue.get()

            client_node_id = packet_report[0]
            receive_time = packet_report[1]
            timestamp = packet_report[2]
            sequence_number = packet_report[3]
            packet_size = packet_report[4]
            delay = '{0:.7f}'.format((receive_time - timestamp) * 1000)  # In millisecond.
            # delay = round(delay, 5)
            # print("sequence_number:", sequence_number, "- delay:", delay)
            # print("client_node_id type", type(client_node_id))

            counter = counter + 1

            passed_time = time.time() - current_time
            current_time = time.time()

            # First step: process/remove bytes from the buffer at the max_bandwidth rate given the passed_time.
            bytes_processed = passed_time * max_bandwidth

            # The round-robin process needs a sorted list of buffer sizes to work correctly.
            node_buffers_list = []
            current_slice_buffer = 0
            for node_id, node_dict in datasets_queues.items():
                node_id = node_dict["my_id"]
                node_current_buffer_size = node_dict["my_buffer"]
                node_buffers_list.append([node_id, node_current_buffer_size])
                # current_slice_buffer += node_current_buffer_size
            node_buffers_list.sort(key=lambda x: x[1])  # Sort the nodes based on their buffer sizes.

            # Allocate resources (process/remove bytes from node buffers) for each node in RR fashion.
            num_of_nodes = len(node_buffers_list)
            for node in node_buffers_list:
                node_share = math.floor(bytes_processed / num_of_nodes)  # node_share in bytes unit (no fraction of bytes).
                if node[1] < node_share:
                    allocated_resources = node[1]  # The node has less bytes in its buffer than its allocated share.
                else:
                    allocated_resources = node_share
                num_of_nodes -= 1
                bytes_processed -= allocated_resources

                datasets_queues[node[0]]["my_buffer"] -= allocated_resources
                current_slice_buffer += datasets_queues[node[0]]["my_buffer"]

                # Buffer should never be less than 0 when using RR.
                if datasets_queues[node[0]]["my_buffer"] < 0:
                    print(node[0], "buffer is", datasets_queues[node[0]]["my_buffer"])
                    break
                    # datasets_queues[node[0]]["my_buffer"] = 0

            # current_slice_buffer = current_slice_buffer - bytes_processed
            # datasets_queues[client_node_id]["my_buffer"] -= bytes_processed / len(housed_nodes)

            # if current_slice_buffer < 0:
            #     current_slice_buffer = 0
            # if datasets_queues[client_node_id]["my_buffer"] < 0:
            #     datasets_queues[client_node_id]["my_buffer"] = 0

            # if (current_slice_buffer + packet_size) > slice_buffer_size:
            if (datasets_queues[client_node_id]["my_buffer"] + packet_size) > node_max_buffer_size:
                dropped = True
            else:
                # current_slice_buffer = current_slice_buffer + packet_size
                datasets_queues[client_node_id]["my_buffer"] += packet_size
                current_slice_buffer += packet_size
                dropped = False
            node_current_buffer_size = datasets_queues[client_node_id]["my_buffer"]

            # Adding additional information to the packet report.
            packet_report.append(delay)
            packet_report.append(dropped)
            packet_report.append(node_current_buffer_size)
            packet_report.append(node_max_buffer_size)
            packet_report.append(current_slice_buffer)
            packet_report.append(slice_max_buffer_size)
            packet_report.append(max_bandwidth)
            packet_report.append(my_slice_id)
            packet_report.append(number_of_nodes)

            datasets_queues[client_node_id]["my_queue"].put(packet_report)

            total_pkts += 1

            # if (current_time - last_second) > 1:
            #     print("- current_slice_buffer in Mbit:",
            #           '{0:.7f}'.format(current_slice_buffer * 8 / 1000000), "skip_counter:", skip_counter,
            #           "- bytes_processed", bytes_processed)
            #     skip_counter = 0
            #     last_second = current_time

        except queue.Empty:
            skip_counter += 1
        except KeyboardInterrupt:
            break

    for node_id in housed_nodes:
        datasets_queues[str(node_id)]["my_queue"].put("close")

    remaining_packets = slice_1_queue.qsize()
    print("closing slice manager", slice_id, "- total_packets:", total_pkts, "- remaining_packets:", remaining_packets)

    for client_dataset_thread in thread_list:
        client_dataset_thread.join()


def create_dataset(node_id, my_queue):

    current_node_dict = get_client_lte_info_by_node_id(node_id)
    node_imsi = current_node_dict["ue_imsi"]
    node_lte_ip = current_node_dict["lte_ip"]

    ue_list = get_all_ue_dicts()
    first_node_dict = ue_list[0]
    second_node_dict = ue_list[1]

    first_node_imsi = first_node_dict["ue_imsi"]
    first_node_lte_ip = first_node_dict["lte_ip"]
    ack_port_1 = 7700 + int(first_node_imsi[13:])

    second_node_imsi = second_node_dict["ue_imsi"]
    second_node_lte_ip = second_node_dict["lte_ip"]
    ack_port_2 = 7700 + int(second_node_imsi[13:])

    ack_channel_1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ack_channel_2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # For preventing sending the same ACK multiple times.
    # acked_packets = []
    # for i in range(0, 200):
    #     acked_packets.append("x")

    # feedback_port = 7700 + int(node_imsi[13:])
    # feedback_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    ppr_dataset_name = "ue_" + node_id + ".csv"
    ppr_dataset_path = "/root/ue_datasets/"
    ppr_dataset_file = open(ppr_dataset_path + ppr_dataset_name, "w")
    ppr_dataset_writer = csv.writer(ppr_dataset_file)

    ppr_header = []

    ppr_header.append("packet_size")  # In bytes.
    ppr_header.append("timestamp")  # The timestamp of the message creation at the sender.
    ppr_header.append("sequence_number")
    ppr_header.append("inter_arrival_ms")  # The inter-arrival time between this packet and the last received packet.
    ppr_header.append("lost_packets")  # The packets lost in transmission from this packet to the last received packet.
    ppr_header.append("receive_time")  # The time the packet is received at the antenna.
    ppr_header.append("transmission_delay_ms")  # receive_time - timestamp
    ppr_header.append("dropped")  # "1" if the node buffer is full, and "0" otherwise.
    ppr_header.append("buffering_delay_ms")  # receive_time - (the time the packet exited the buffer) ("-1" for dropped packets.
    ppr_header.append("node_current_buffer_size")  # How many bytes in the node's buffer at the moment this packet was received.
    ppr_header.append("node_max_buffer_size")  # Currently does not change once initialized.
    ppr_header.append("slice_current_buffer_size")  # How many bytes in the whole slice buffer at the moment this packet was received.
    ppr_header.append("slice_max_buffer_size")  # Currently does not change once initialized.
    ppr_header.append("node_id")  # Currently does not change once initialized.
    ppr_header.append("slice_id")  # Currently does not change once initialized.
    ppr_header.append("slice_max_bandwidth")  # In Mbps. Currently does not change once initialized.
    ppr_header.append("number_of_users")  # number_of_users in this slice. Currently does not change once initialized.

    ppr_dataset_writer.writerow(ppr_header)

    current_time = time.time()
    total_received = 0
    total_dropped = 0
    total_lost = 0
    total_delay = 0
    total_pkt = 0
    pkt_per_second = 0
    skip_counter = 0
    time_left = 1
    ack_sequence = 1

    first_packet = my_queue.get()

    if first_packet == "close":
        return

    client_node_id = first_packet[0]
    previous_packet_arrival = first_packet[1]
    previous_sequence = first_packet[3]

    while True:
        try:
            # packet_report = my_queue.get(block=False)
            packet_report = my_queue.get()

            if packet_report == "close":
                break

            pkt_per_second += 1
            total_pkt += 1

            client_node_id = packet_report[0]
            receive_time = packet_report[1]
            timestamp = packet_report[2]
            sequence_number = packet_report[3]
            packet_size = packet_report[4]
            transmission_delay = packet_report[5]
            dropped = packet_report[6]
            node_current_buffer_size = packet_report[7]
            node_max_buffer_size = packet_report[8]
            current_slice_buffer_size = packet_report[9]
            slice_max_buffer_size = packet_report[10]
            slice_max_bandwidth = packet_report[11]
            slice_id = packet_report[12]
            number_of_nodes = packet_report[13]

            exit_time = time.time()
            # buffering_delay = exit_time - receive_time
            buffering_delay = current_slice_buffer_size / slice_max_bandwidth  # Theoretical buffering time.
            lost_packets = sequence_number - previous_sequence - 1
            inter_arrival = receive_time - previous_packet_arrival

            previous_packet_arrival = receive_time
            previous_sequence = sequence_number
            total_lost += lost_packets
            total_delay += float(transmission_delay)

            buffering_delay = '{0:.7f}'.format(buffering_delay * 1000)
            inter_arrival = '{0:.7f}'.format(inter_arrival * 1000)

            assert node_id == client_node_id

            if dropped:
                total_dropped += 1
            else:
                total_received = total_received + packet_size

            if (exit_time - current_time) > 1:
                current_traffic = total_received * 8 / 1000000
                current_traffic = '{0:.3f}'.format(current_traffic)
                print(current_traffic, "Mbits received within", '{0:.7f}'.format(time.time() - current_time),
                      "seconds from node", client_node_id, "- total_dropped", total_dropped, "- pkt_per_second =",
                      pkt_per_second, "- skip_counter:", skip_counter, "- total_lost:", total_lost, "- total_delay:", total_delay)
                total_received = 0
                total_dropped = 0
                total_lost = 0
                total_delay = 0
                pkt_per_second = 0
                skip_counter = 0
                current_time = time.time()

            packet_dataset_entry = []

            packet_dataset_entry.append(packet_size)
            packet_dataset_entry.append(timestamp)
            packet_dataset_entry.append(sequence_number)
            packet_dataset_entry.append(inter_arrival)
            packet_dataset_entry.append(lost_packets)
            packet_dataset_entry.append(receive_time)
            packet_dataset_entry.append(transmission_delay)
            packet_dataset_entry.append(dropped)
            packet_dataset_entry.append(buffering_delay)
            packet_dataset_entry.append(node_current_buffer_size)
            packet_dataset_entry.append(node_max_buffer_size)
            packet_dataset_entry.append(current_slice_buffer_size)
            packet_dataset_entry.append(slice_max_buffer_size)
            packet_dataset_entry.append(client_node_id)
            packet_dataset_entry.append(slice_id)
            packet_dataset_entry.append(slice_max_bandwidth)
            packet_dataset_entry.append(number_of_nodes)

            ppr_dataset_writer.writerow(packet_dataset_entry)

            # To duplicate the ACK via other BSs.
            # ack_queue.put([packet_dataset_entry, node_lte_ip, feedback_port])

            pkt_sn = packet_dataset_entry[2]
            acked = False
            min_sn = 9999999999
            min_sn_idx = 0
            # tmp_lst = []
            for idx, acked_sn in enumerate(acked_packets):
                # tmp_lst.append(acked_sn)
                if pkt_sn == acked_sn:
                    acked = True
                    break
                if min_sn > acked_sn:
                    min_sn = acked_sn
                    min_sn_idx = idx

            if acked:
                # print("node_id:", node_id, "- pkt_sn", pkt_sn, "already ACKed")
                pass
            else:
                packet_dataset_entry.append(ack_sequence)
                ack_sequence += 1
                acked_packets[min_sn_idx] = pkt_sn
                packet_feedback = json.dumps(packet_dataset_entry).encode('utf8')

                # Sending/Duplicating ACK back to client via LTE.
                # sent = feedback_socket.sendto(packet_feedback, (node_lte_ip, feedback_port))
                sent = ack_channel_1.sendto(packet_feedback, (first_node_lte_ip, ack_port_1))
                sent = ack_channel_2.sendto(packet_feedback, (second_node_lte_ip, ack_port_2))

                # Some prints
                # nid = packet_dataset_entry[13]
                # pkt_seq = packet_dataset_entry[2]
                # ack_seq = packet_dataset_entry[-1]
                # lost_pkts = packet_dataset_entry[4]
                # print("node id:", nid, "- pkt_seq:", pkt_seq, "- ack_seq:", ack_seq, "- lost_pkts:", lost_pkts)





        except queue.Empty:
            skip_counter += 1
            # if (time.time() - current_time) > 1:
            #     current_traffic = total_received * 8 / 1000000
            #     current_traffic = '{0:.3f}'.format(current_traffic)
            #     print(current_traffic, "Mbits received within", '{0:.7f}'.format(time.time() - current_time),
            #           "seconds from node", client_node_id, "- total_dropped", total_dropped, "- pkt_per_second =",
            #           pkt_per_second, "- skip_counter:", skip_counter, "- total_lost:", total_lost, "- total_delay:", total_delay)
            #     total_received = 0
            #     total_dropped = 0
            #     total_lost = 0
            #     total_delay = 0
            #     pkt_per_second = 0
            #     skip_counter = 0
            #     current_time = time.time()
        except KeyboardInterrupt:
            break

    remaining_packets1 = my_queue.qsize()
    # remaining_packets2 = len(my_queue)
    # print("remaining unlogged packets:", remaining_packets1)
    last_seq = packet_dataset_entry[2]
    print("closing dataset thread for node", node_id, "- total logged packets:", total_pkt,
          "- remaining unlogged packets:", remaining_packets1, "- last_seq:", last_seq)
    ppr_dataset_file.close()


def start_ack_duplication_process():

    node_dict = get_my_info()
    my_lte_ip = node_dict["lte_ip"]
    my_col_ip = node_dict["col_ip"]
    my_node_id = node_dict["node_id"]

    urllc_mapping_configuration = get_json_file("radio_api/urllc_mapping_configuration.txt")
    for node_id, mapping_dict in urllc_mapping_configuration.items():
        if int(node_id) == my_node_id:
            break

    assert mapping_dict["node_type"] == "bs"

    try:
        if mapping_dict["my_role"] == "master":

            inter_bs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            inter_bs_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            inter_bs_port = my_col_ip[11:]

            inter_bs_socket.bind((my_col_ip, inter_bs_port))
            inter_bs_socket.listen(10)
            bs_socket, bs_col_address = inter_bs_socket.accept()

        if mapping_dict["my_role"] == "support":

            master_bs_id = mapping_dict["master_node_id"]
            master_bs_info = get_client_lte_info_by_node_id(master_bs_id)
            master_bs_col_ip = master_bs_info["col_ip"]

            inter_bs_port = master_bs_col_ip[11:]
            bs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            bs_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            bs_socket.connect((master_bs_col_ip, inter_bs_port))

        bs_ack_dup_thread = Thread(target=dup_ack_to_bs, args=(bs_socket,))
        bs_ack_dup_thread.start()

        ue_ack_dup_thread = Thread(target=relay_ack_from_bs_to_ue, args=(bs_socket,))
        ue_ack_dup_thread.start()

        bs_ack_dup_thread.join()
        ue_ack_dup_thread.join()

    except ConnectionRefusedError:
        print("ConnectionRefusedError")
    except OSError:
        print("OSError")
    except KeyboardInterrupt:
        print("\nCtrl+C on start_ack_reply_process")

    # bs_socket.close()
    # dup_ack_socket.close()


# To duplicate an ACK created by this BS to other BSs.
def dup_ack_to_bs(bs_socket):

    while True:
        try:
            dup_info = ack_queue.get()
            ack_dup_request = create_ack_relay_message(dup_info)
            bs_socket.send(ack_dup_request)
        except KeyboardInterrupt:
            break


# Receiving ACKs from other BSs then send on UE downlink.
def relay_ack_from_bs_to_ue(bs_socket):

    dup_ack_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    while True:
        try:
            ack_size = receive_up_to(bs_socket, 5)
            if ack_size == -1:
                break
            ack_request = receive_up_to(bs_socket, int(ack_size))
            ack_request = json.loads(ack_request.decode("utf8"))
            ack_packet = ack_request[0]
            node_lte_ip = ack_request[1]
            feedback_port = ack_request[2]

            ack_packet = json.dumps(ack_packet).encode('utf8')

            sent = dup_ack_socket.sendto(ack_packet, (node_lte_ip, feedback_port))
        except KeyboardInterrupt:
            break

    bs_socket.close()
    dup_ack_socket.close()


def create_ack_relay_message(dup_info):
    dup_info = json.dumps(dup_info)
    size = len(dup_info)
    size = str(size)
    padding = "0" * (5 - len(size))
    size = padding + size
    ack_dup_request = bytes(dup_info + size, 'utf')
    return ack_dup_request

if __name__ == "__main__":

    os.system("rm -rf ue_datasets")
    os.system("mkdir ue_datasets")

    lte_ip = "172.16.0.1"

    col_ip = get_bs_col_ip()
    col_port = 5555

    MTU = 1472  # 1500 - 20 (IP) - 8 (UDP)
    header_size = 30
    MTU_data = 'x' * (MTU - header_size)

    process_dict = Manager().dict()
    clients_dict = Manager().dict()

    acked_packets = Array('i', 200)

    slice_1_queue = Queue()
    # slice_2_queue = Queue()
    # slice_3_queue = Queue()

    # ack_queue = Queue()

    slice_1_process = Process(target=start_slice_manager, args=(1,))
    # slice_2_process = Process(target=start_slice_manager, args=(2,))
    # slice_3_process = Process(target=start_slice_manager, args=(3,))

    slice_1_process.start()
    # slice_2_process.start()
    # slice_3_process.start()

    # ack_process = Process(target=start_ack_duplication_process)
    # ack_process.start()

    start_receiving_connections()

    slice_1_process.join()
    # slice_2_process.join()
    # slice_3_process.join()
    # ack_process.join()

    print("terminating server program")
