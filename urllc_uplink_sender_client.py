
import socket
from socket import timeout as socket_timeout
import time
import json
from multiprocessing import Process, Queue, Value
import queue
import math
import select
from threading import Thread


def get_injection_file_node(node_id):
    injection_file = open("nodes_config_files/" + str(node_id) + "_traffic_file.txt")
    while True:
        try:
            traffic_parameters = json.load(injection_file)
            break
        except json.decoder.JSONDecodeError:
            print("failed to open traffic file of node", node_id, "we will try again.")
    injection_file.close()
    return traffic_parameters


def get_json_file(json_path):
    while True:
        try:
            config_file = open(json_path)
            node_parameters = json.load(config_file)
            config_file.close()
            return node_parameters
        except json.decoder.JSONDecodeError:
            print("failed to open (", json_path, "). Will try again.")


def get_bs_col_ip():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["node_type"] == "bs":
            col_ip = node_dict["col_ip"]
            return col_ip


def get_my_info():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["this_is_my_node"]:
            return node_dict


def get_node_info(target_node_id):
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if int(node_id) == target_node_id:
            return node_dict


def get_injection_parameters(node_id):
    traffic_parameters = get_injection_file_node(node_id)
    packet_size = traffic_parameters["packet_size"]
    desired_bandwidth = traffic_parameters["desired_bandwidth"]
    return packet_size, desired_bandwidth


def create_feedback(feedback):

    feedback_size = len(feedback)
    feedback_size = str(feedback_size)
    padding = "0" * (5 - len(feedback_size))  # feedback should be exactly 5 bytes.
    feedback_size = padding + feedback_size
    feedback = feedback_size.encode('utf8') + feedback
    return feedback


def create_packet(data, sequence_number):

    padding = "0" * (10 - len(sequence_number))  # sequence_number should be exactly 10 bytes.
    sequence_number = padding + sequence_number

    timestamp = time.time()
    timestamp = str(timestamp)
    padding = "0" * (20 - len(timestamp))  # timestamp should be exactly 20 bytes.
    timestamp = timestamp + padding

    message = bytes(timestamp + sequence_number + data, 'utf')
    return message


def create_pd_request(data, sequence_number):
    packet_size = str(len(data))
    packet_padding = "0" * (5 - len(packet_size))  # packet_size should be exactly 5 bytes.
    packet_size = packet_padding + packet_size

    sequence_padding = "0" * (10 - len(sequence_number))  # sequence_number should be exactly 10 bytes.
    sequence_number = sequence_padding + sequence_number

    pd_request = bytes(packet_size + sequence_number, 'utf')
    return pd_request


def send_to_uplink_pd_mc(data, sequence_number):

    pd_request = create_pd_request(data, str(sequence_number))
    support_socket.send(pd_request)

    packet = create_packet(data, str(sequence_number))
    sent = uplink_injection_socket.sendto(packet, (server_lte_ip, uplink_injection_port))
    return sent


def start_master_uplink_traffic_injection():

    message_size, desired_bandwidth = get_injection_parameters(my_node_id)

    num_of_packets = message_size / MTU  # How many packets we need to send a single message.
    num_of_MTU_packets = math.floor(num_of_packets)  # The last packet is less than MTU size.

    sequence_number = 1
    total_packets = 0
    total_sent = 0  # For print purposes only.
    sleep_time = message_size / desired_bandwidth  # inter-arrival time of messages (not packets).

    print('Message size: ', message_size / 1000, "KB")
    print('Desired bandwidth: ', desired_bandwidth * 8 / 1000000, "Mbps")
    print("Whole message inter-arrivals: ", sleep_time, "second")
    print("uplink_injection_port:", uplink_injection_port)

    # current_time = time.time()
    one_second = time.time()

    while True:

        try:
            current_time = time.time()

            # The server closed the measurement socket which sets kill_switch to True.
            if kill_switch.value:
                print("server is closed (uplink injection)")
                break

            ############### Start Injection #################
            for i in range(0, num_of_MTU_packets):
                sent = send_to_uplink_pd_mc(MTU_data, sequence_number)
                total_sent = total_sent + sent
                sequence_number = sequence_number + 1
                total_packets += 1
                if sequence_number == 9999999999:
                    sequence_number = 1

            # Both actually works.
            # remaining_bytes = message_size - total_sent
            remaining_bytes = message_size - (MTU * num_of_MTU_packets)

            if remaining_bytes != 0:  # We still have less-than-MTU bytes to send.
                if remaining_bytes <= header_size:  # Minimum packet size is header size.
                    data = ''
                else:
                    data = MTU_data[:remaining_bytes - header_size]

                sent = send_to_uplink_pd_mc(data, sequence_number)
                total_sent = total_sent + sent
                sequence_number = sequence_number + 1
                total_packets += 1
                if sequence_number == 9999999999:
                    sequence_number = 1
            ################ End Injection #################

            if (current_time - one_second) > 1:
                print("-- sent:", total_sent * 8 / 1000000, "Mbit within", time.time() - one_second)
                one_second = time.time()
                total_sent = 0

                new_message_size, new_desired_bandwidth = get_injection_parameters(my_node_id)
                if message_size == new_message_size and desired_bandwidth == new_desired_bandwidth:
                    pass  # No change in traffic profile.
                else:
                    # print("new injection parameters for node", client_node_id, "- message_size", new_message_size, "desired_bandwidth", new_desired_bandwidth)
                    message_size = new_message_size
                    desired_bandwidth = new_desired_bandwidth
                    sleep_time = message_size / desired_bandwidth
                    num_of_packets = message_size / MTU
                    num_of_MTU_packets = math.floor(num_of_packets)

                    # Inter-arrival of packets cannot exceed 1 second.
                    if sleep_time > 1:
                        sleep_time = 1

            # print("----------------------------")
            wasted_time = time.time() - current_time
            time.sleep(sleep_time - wasted_time)

        # This happens when trying to sendto() a non-empty buffer.
        # This will never happen if the socket is not set to non-blocking.
        except BlockingIOError:
            select.select([], [uplink_injection_socket], [])
            print("buffer is full (the remaining of the message will be lost)")
            time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("\nctrl+C detected on injection process for node", my_node_id)
            break

    uplink_injection_socket.close()
    support_socket.shutdown(2)
    support_socket.close()
    print("total_packets:", total_packets)
    print("existing injection process for client", my_node_id)


def start_support_uplink_traffic_injection():
    current_time = time.time()
    total_sent = 0
    total_packets = 0
    while True:
        try:
            pd_request = b''
            while len(pd_request) < 15:
                remaining_bytes = support_socket.recv(15 - len(pd_request))
                if len(remaining_bytes) == 0:
                    print("Master node closed the support connection")
                    print("total_packets:", total_packets)
                    support_socket.close()
                    uplink_injection_socket.close()
                    return
                pd_request = pd_request + remaining_bytes

            packet_size = pd_request[:5]
            sequence_number = pd_request[5:]
            packet_data = 'x' * int(packet_size)

            packet = create_packet(packet_data, sequence_number.decode("utf-8"))
            sent = uplink_injection_socket.sendto(packet, (server_lte_ip, uplink_injection_port))
            total_sent += sent
            total_packets += 1

            if (time.time() - current_time) > 1:
                print("support node sent", total_sent * 8 / 1000000, "Mbit within", time.time() - current_time)
                current_time = time.time()
                total_sent = 0

        except KeyboardInterrupt:
            support_socket.close()
            uplink_injection_socket.close()
            return


# If MC is enabled, we need a support_socket via the Colosseum internal network
# to coordinate between the master node and the support node.
# The master node tells the support node what packet to send over the uplink channel,
# while the support node report back the received ACK from the base station back to the master node.
# Note that the master node also need to receive its own ACKs from its base station.
def get_uplink_mc_socket(mapping_dict):

    if mapping_dict["my_role"] == "master":
        master_port = 8800 + int(my_imsi[13:])
        while True:
            try:
                server_support_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_support_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_support_socket.bind((my_col_ip, master_port))
                server_support_socket.listen(10)
                print("Starting a support TCP server on", my_col_ip, "at port", master_port)
                support_socket, support_node_col_address = server_support_socket.accept()
                print("Support node has connected from col_address", support_node_col_address)
                return support_socket
            except OSError:
                print("Couldn't bind on IP", my_col_ip, "on port", master_port)
                time.sleep(1)
            except KeyboardInterrupt:
                print("\nCtrl+C on support socket server.")
                return -1

    if mapping_dict["my_role"] == "support":
        master_node_id = mapping_dict["master_node_id"]
        master_node_dict = get_node_info(master_node_id)
        master_node_col_ip = master_node_dict["col_ip"]
        master_node_col_imsi = master_node_dict["ue_imsi"]
        master_port = 8800 + int(master_node_col_imsi[13:])
        while True:
            try:
                support_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                support_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                support_socket.connect((master_node_col_ip, master_port))
                print("connected to master node.")
                return support_socket
            except ConnectionRefusedError:
                # if restart_switch.value or kill_switch.value:
                #     print("Master node already closed. Exiting process.")
                #     return -1
                print("We will try again shortly (setup_uplink_mc)")
                time.sleep(1)
            except OSError:
                print("LTE IP is not ready yet (setup_uplink_mc)")
                time.sleep(1)
            except KeyboardInterrupt:
                print("\nCtrl+C on support socket.")
                return -1

    return -1


# Receiving ACKs (as a support node) with packet report from base station,
# then send the report to the master node via the support_socket (Colosseum internal network).
def start_support_feedback_channel():
    while True:
        try:
            feedback, addr = feedback_socket.recvfrom(1000)
            # feedback_size = len(feedback)
            # feedback = json.loads(feedback)
            # seq_num = feedback[2]
            # current_node_id = feedback[13]
            # print("seq_num:", seq_num, "- node_id:", current_node_id)
            feedback = create_feedback(feedback)
            sent = support_socket.send(feedback)

        except socket_timeout:
            print("support feedback channel has timed out.")
            if kill_switch.value:
                break
        except BrokenPipeError:
            break
        except KeyboardInterrupt:
            break
    print("exiting start_support_feedback_channel")


def receive_up_to(buffer):

    while True:
        try:
            received_message = b''
            while len(received_message) < buffer:
                remaining_bytes = support_socket.recv(buffer - len(received_message))
                if len(remaining_bytes) == 0:
                    print("Master node closed the support connection")
                    support_socket.close()
                    uplink_injection_socket.close()
                    return -1
                received_message = received_message + remaining_bytes
            return received_message
        except KeyboardInterrupt:
            print("\nCtrl+C on receive_up_to")


def start_master_feedback_channel():

    support_feedback_queue = queue.Queue()
    bs_feedback_queue = queue.Queue()

    support_feedback_thread = Thread(target=start_master_support_feedback_channel, args=(support_feedback_queue,))
    support_feedback_thread.start()

    bs_feedback_thread = Thread(target=start_master_bs_feedback_channel, args=(bs_feedback_queue,))
    bs_feedback_thread.start()

    while True:
        try:

            while True:
                try:
                    support_report = support_feedback_queue.get_nowait()
                except queue.Empty:
                    break

            while True:
                try:
                    bs_report = bs_feedback_queue.get_nowait()
                except queue.Empty:
                    break

        except KeyboardInterrupt:
            break


# master node receiving feedback (ACK) from support node.
def start_master_support_feedback_channel(support_feedback_queue):
    while True:
        try:
            feedback_size = receive_up_to(5)  # feedback_size is exactly 5 bytes.
            if feedback_size == -1:
                break
            feedback = receive_up_to(int(feedback_size))

            feedback = json.loads(feedback.decode("utf8"))
            # seq_num = feedback[2]
            # current_node_id = feedback[13]
            # print("seq_num:", seq_num, "- node_id:", current_node_id)

            # feedback, addr = feedback_socket.recvfrom(1000)
            # feedback = json.loads(feedback)
            # seq_num = feedback[2]
            # current_node_id = feedback[13]
            # print("seq_num:", seq_num, "- node_id:", current_node_id)

        except KeyboardInterrupt:
            support_socket.shutdown(2)
            break
    support_socket.close()


def start_master_bs_feedback_channel(bs_feedback_queue):

    while True:
        try:
            feedback, addr = feedback_socket.recvfrom(1000)
            # feedback_size = len(feedback)
            # feedback = json.loads(feedback)
            # seq_num = feedback[2]
            # current_node_id = feedback[13]
            # print("seq_num:", seq_num, "- node_id:", current_node_id)
            feedback = create_feedback(feedback)
            sent = support_socket.send(feedback)

        except socket_timeout:
            print("support feedback channel has timed out.")
            if kill_switch.value:
                break
        except BrokenPipeError:
            break
        except KeyboardInterrupt:
            break
    print("exiting start_support_feedback_channel")


def start_measurement():

    while True:
        try:
            measurements_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            measurements_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            measurements_socket.connect((server_col_ip, server_col_port))
            break
        except ConnectionRefusedError:
            if restart_switch.value or kill_switch.value:
                print("Measurement server already closed. Exiting process.")
                return
            print("We will try again shortly (measurements_socket)")
            time.sleep(1)
        except OSError:
            print("Server LTE IP is not ready yet (measurements_socket)")
            time.sleep(1)

    print("connected to main server for measurement on Colosseum IP", server_col_ip)

    if mapping_dict["my_role"] == "master":
        print("starting master injection process")
        traffic_uplink_injection_process = Process(target=start_master_uplink_traffic_injection)
        traffic_uplink_injection_process.start()
        feedback_process = Process(target=start_master_feedback_channel)
        feedback_process.start()

    if mapping_dict["my_role"] == "support":
        print("starting support injection process")
        traffic_uplink_injection_process = Process(target=start_support_uplink_traffic_injection)
        traffic_uplink_injection_process.start()
        feedback_process = Process(target=start_support_feedback_channel)
        feedback_process.start()

    # TODO: measurement channel is not complete yet.
    while True:
        try:
            message = measurements_socket.recv(1000)
            if len(message) == 0:
                print("server closed the measurement socket")
                break
        except KeyboardInterrupt:
            print("\nCtrl+C on measurement process")
            break
    measurements_socket.shutdown(2)
    measurements_socket.close()
    support_socket.close()
    kill_switch.value = True


if __name__ == "__main__":

    node_dict = get_my_info()
    my_imsi = node_dict["ue_imsi"]
    my_lte_ip = node_dict["lte_ip"]
    my_col_ip = node_dict["col_ip"]
    my_node_id = node_dict["node_id"]

    urllc_mapping_configuration = get_json_file("radio_api/urllc_mapping_configuration.txt")
    for node_id, mapping_dict in urllc_mapping_configuration.items():
        if int(node_id) == my_node_id:
            break

    # print("node_dict:", node_dict)
    print("mapping_dict:", mapping_dict)

    support_socket = get_uplink_mc_socket(mapping_dict)

    # Every UE will have a dedicated UDP process/port using UE's last 2 IMSI digits.
    uplink_injection_port = 9900 + int(my_imsi[13:])
    uplink_injection_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP injection socket.

    # for receiving ACKs from the base station.
    feedback_port = 7700 + int(my_imsi[13:])
    feedback_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    feedback_socket.bind((my_lte_ip, feedback_port))
    feedback_socket.settimeout(1)

    server_lte_ip = "172.16.0.1"

    server_col_ip = get_bs_col_ip()
    server_col_port = 5555

    restart_switch = Value('i')
    restart_switch.value = False

    kill_switch = Value('i')
    kill_switch.value = False

    # 1472 makes sure to fit our UDP packet to exactly a single MAC-layer frame which is 1500 bytes.
    MTU = 1472  # 1472 + IP 20-bytes + UDP 8-bytes = 1500 bytes.
    header_size = 30
    MTU_data = 'x' * (MTU - header_size)

    while True:

        try:
            traffic_measurements = Queue()
            restart_switch.value = False

            measurements_socket_process = Process(target=start_measurement)
            measurements_socket_process.start()
            measurements_socket_process.join()

            # Exiting the entire program when the server closes.
            if kill_switch.value:
                print("terminating entire client process")
                break

            # Traffic process got hung so we need to restart the process.
            print("refreshing client process")

        except KeyboardInterrupt:
            # A better option is to close the recv() socket in case of the server is connected but idle (although the
            # server is designed to never be idle).
            kill_switch.value = True
            # delay_measurements.put("exit")  # I don't think it is really needed.
            break

    print("terminating program...")



