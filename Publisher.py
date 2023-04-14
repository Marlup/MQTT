# key words: message (msg)

from enum import Enum

import socket
import json
import time

class PacketType(Enum):
    ConnUnknown = -1
    ConnAck = 0
    ConnRej = 1
    Publish = 2
    Republish = 3
    PubAck = 4 # Englobes PubRec (qos 2) and PubAck (1) into Ack
    PubComp = 5

class Broker():
    # Listen client
    # Proccess alive time
    # Suback
    # Unsuback
    # Read pkt
    # Verify pkt msg
    # Process pkt msg
    # Send pkt msg to Subscriber
    # Send acknowledge to Publisher 
    # Save Publish pkt
    # Send PubRec
    # Save PubRec
    # Discard all states (Publish pkt, PubRec pkt)
    # Save reference to pkt identifier of original Publish pkt (to avoid second processing)
    # (QoS1, QoS2) Queue messages
    # (QoS1) send pkt acknowledge to Publisher (PubAck => publish acknowledge)
    # (QoS2) send pkt reception confirmation to publisher (PubRec => publish reception)
    # (QoS2) send pkt completion to Publisher (PubCom => publish completion)

    pass
    def __init__(self):
        pass
        
class PublisherMeta():
    def __init__(self, 
                 _id: str=None, 
                 username:str ="user",
                 password:str =None,
                 topic_name:str =None, 
                 qos=0, 
                 retain_flag=False, 
                 republish_time=60,
                 keep_alive=0,
                 clean_session=False,
                 last_will_topic=None,
                 last_will_qos=0,
                 last_will_message="unexpected exit",
                 last_will_retain=False,
                 buffer_size=1024
                 ):
        if not isinstance(_id, str) and not clean_session:
            raise "Input error. Empty id requires clean session as False"
        self.id = _id
        if not isinstance(username, str):
            raise "Input error. Username was not provided or invalid dtype"
        self.username = username
        if not isinstance(password, str):
            raise "Input error. Password was not provided or invalid dtype"
        self.password = password
        if topic_name is None:
            self.topic_name = "root/topic"
        else:
            self.topic_name = topic_name
        self.qos = qos
        self.retain_flag = retain_flag
        self.republish_time = republish_time
        self.keep_alive = keep_alive
        self.clean_session = clean_session
        if last_will_topic is None:
            self.last_will_topic = self.topic_name
        else:
            self.last_will_topic = last_will_topic
        self.last_will_qos = last_will_qos
        self.last_will_message = last_will_message
        self.last_will_retain = last_will_retain
        self.buffer_size = buffer_size
        self.restart_attributes()
    def restart_attributes(self):
        self.dup_flag = False
        self.original_packet = None
        self.last_packet = None
        self.current_pkt_type = None
        self.type_pkt = ""
        self.on_republish = False
        self.socket = None
        self.ok_broker_conn = False
    @staticmethod
    def _setNewTopicName(self, newTopic):
        self.topicName = newTopic

class Publisher(PublisherMeta):
    # Connect
    # Disconnect
    # Format msg
    # Send Publish pkt to Broker(Publish).
    # Send LWT
    # Change Connection parameters
    # Save original Publish pkt
    # (if not PubRec pkt) Send saved original Publish 
    #   pkt to Broker with dupflag.
    # Discard original Publish pkt
    
    # Save PubRec pkt
    # Send PubRel pkt
    # Discard PubRec pkt
    # Save PubRel pkt
    # Discard PubCom pkt
    # Make pkt identifier available
    # QoS2 send pkt state release to Broker (PubRel => publish release)
    pass

    def __init__(self, **kwargs):
        super(Publisher).__init__(self, kwargs)

    def _format_connect(self):
        conn_data = {"clientId": self.id,
                     "type": "connect",
                     "cleanSession": self.clean_session,
                     "username": self.username,
                     "password": self.password,
                     "lastWillTopic": self.last_will_topic,
                     "lastWillQos": self.last_will_qos,
                     "lastWillMessage": self.last_will_message,
                     "lastWillRetain": self.last_will_retain,
                     "keepAlive": self.keep_alive
                     }
        return conn_data
    
    def connect_socket(self, ip="localhost", port=8888):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.broker_address = (ip, port)
        self.sock.connect(ip, port)
        data: str = self._format_connect()
        self.sock.sendall(data.encode())

    def disconnect_socket(self):
        self.sock.close()

    def _send_to_socket(self, data: str):
        self.sock.sendall(data.encode())

    def _build_publish_packet(self, data):
        return self._format_publish_packet(data)
    
    def _build_pubrel_packet(self):
        return self._format_pubrel_packet()
    
    def _read_sensor(self):
        return -1
    
    def _format_publish_packet(self, data):
        packet = {
            "pktid": self.id,
            "type": "publish",
            "payload": data,
            "topicname": self.topic_name,
            "qos": self.qos, 
            "retainflag": self.retain_flag,
            "dupflag": self.dup_flag
            }
        return packet
    
    def _format_pubrel_packet(self):
        packet = {
            "pktid": self.id,
            "type": "PubRel"
            }
        return packet

    def send_disconnect(self):
        pass

    def _listen_broker(self):
        response = self.sock.recv(self.buffer_size)
        pkt_type = response["type"]
        decoded_pkt_type = self._decode_packet_type(pkt_type)
        return decoded_pkt_type

    def send_packet(self):
        if self.ok_broker_conn:
            data = self._process_communication()
            data = json.dumps(data)
            self._send_to_socket(data)
        else:
            self._listen_broker()
            if self.type_pkt == PacketType.ConnAck:
                self.ok_broker_conn = True
        data = self._process_communication()
        data = json.dumps(data)
        self._send_to_socket(data)

    def _process_communication(self):
        if self.qos == 0:
            data = self._read_sensor()
            return self._build_publish_packet(data)
        else:
            current_pkt_type = self._listen_broker()
            self._process_type_pkt(current_pkt_type)
            return self._process_nonzero_qos(self)

    def _process_type_pkt(self, pkt_type):
        if self.qos == 1:
            if pkt_type == PacketType.PubAck:
                self.type_pkt = PacketType.Publish
                self.on_republish = False
        elif self.qos == 2:
            if pkt_type == PacketType.PubAck:
                self.dup_flag = True
                self.on_republish = False
            elif pkt_type == PacketType.PubComp:
                self.type_pkt = PacketType.Publish
                self.dup_flag = False

    def _decode_packet_type(self, pkt_type):
        if pkt_type == "ConnAck":
            return PacketType.ConnAck
        elif pkt_type == "ConnRej":
            return PacketType.ConnRej
        elif pkt_type == "Publish":
            return PacketType.Publish
        elif pkt_type == "Republish":
            return PacketType.Republish
        elif pkt_type == "PubAck":
            return PacketType.PubAck
        elif pkt_type == "PubComp":
            return PacketType.PubComp

    def _process_nonzero_qos(self):
        if (self.qos == 1 and (self.type_pkt == PacketType.Publish or 
                               self.type_pkt == PacketType.PubAck)
            or
            (self.qos == 2 and (self.type_pkt == PacketType.Publish or 
                                self.type_pkt == PacketType.PubComp)
                         )):
            data = self._read_sensor()
            self.original_msg = self._build_publish_packet(data)
            self.type_pkt = PacketType.Republish
            return self.original_msg
        elif self.qos == 2:
            if self.type_pkt == PacketType.PubAck:
                return self._build_pubrel_packet()
        if self.type_pkt == PacketType.Republish and self.on_republish:
            return self.original_msg

def main():
    if __name__ == "_main_":
        publisher = Publisher()
        publisher.connect_socket("localhost", 8888)
        t_start = time.perf_counter()
        retake_send = publisher.republish_time
        while True:
            if (time.perf_counter() - t_start) > retake_send:
                # send data to the broker
                publisher.send_packet()
                t_start = time.perf_counter()
            else:
                print("Waiting...")