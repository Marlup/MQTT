# - Different publishers can send messages to the same topic.

from enum import Enum

import socket
import json
import time

class PacketType(Enum):
    Connect = 0
    Disconnect = 1
    Subscribe = 2
    Unsubscribe = 3
    Publish = 4
    Republish = 5
    PubRel = 6

class Status(Enum):
    Ok = 0
    ErrorSameTopic = 1


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
    # Send PubRel
    # Save PubRel
    # Discard all states (Publish pkt, PubRel pkt)
    # Save reference to pkt identifier of original Publish pkt (to avoid second processing)
    # (QoS1, QoS2) Queue messages
    # (QoS1) send pkt acknowledge to Publisher (PubRec => publish acknowledge)
    # (QoS2) send pkt reception confirmation to publisher (PubRel => publish reception)
    # (QoS2) send pkt completion to Publisher (PubCom => publish completion)

    pass
    def __init__(self):
        pass

class BrokerMeta():
    def __init__(self, 
                 username: str="broker",
                 password: str="0123",
                 _id: str="user", 
                 buffer_size: int=1024,
                 min_keepalive: float=5,
                 max_keepalive: float=120
                 ):
        self.id = _id
        self.username = username
        self.password = password
        self.buffer_size = buffer_size
        self.min_keepalive = min_keepalive
        self.max_keepalive = max_keepalive
        self.restart_attributes()
    def restart_attributes(self):
        self.publishers_to_connections: dict = {}
        self.topics_to_subscribers = {} 
        self.topics_to_messages = {}
        self.clients_to_sockets = {}

        self.broker_address: tuple = None
        self.broker_socket = None
        self.incoming_packet = False
class Broker(BrokerMeta):
    pass

    def __init__(self, **kwargs):
        super(Broker).__init__(self, kwargs)
    
    # Socket management functions
    def deploy_socket(self, ip="localhost", port=8888):
        self.broker_address = (ip, port)
        # create a TCP/IP socket
        self.broker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # bind the socket to a specific IP address and port
        self.broker_socket.bind((ip, port))

        # listen for incoming connections
        self.broker_socket.listen(1)        
    
    def _accept_incoming(self):
        self.broker_socket.accept()
        self.broker_available = True
    
    def _close_socket(self):
        self.broker_socket.close()

    def process_pkt(self, pkt):
        if self.broker_available:
            self._handle_incoming_packet(self, pkt)
        else:
            pass


    # Incoming data handling functions
    def _handle_incoming_packet(self, pkt, saved_pkt) -> dict:
        pkt_type = self._decode_packet_type(pkt["type"])
        if pkt_type == PacketType.Publish:
            return self._push_packet(pkt)
        if pkt_type == PacketType.Republish:
            if pkt["id"] != saved_pkt["id"]:
                return self._build_pubrec_packet()
            else:
                return None
        if pkt_type == PacketType.PubRel:
            return self._build_pubcomp_packet()
        if pkt_type == PacketType.Connect:
            session_present, code = self._process_connrec(pkt)
            return self._build_connrec_packet(pkt["clientid"], session_present, code)
        if pkt_type == PacketType.Disconnect:
            return self._disconect_publisher()
        if pkt_type == PacketType.Subscribe:
            ok = self._process_suback(pkt)
            
            return self._build_suback_packet(ok)
        if pkt_type == PacketType.Unsubscribe:
            self.process_unsuback(pkt)
            return self._build_unsuback_packet()

    def _decode_packet_type(self, pkt_type):
        if pkt_type == "Publish":
            return PacketType.Publish
        elif pkt_type == "Republish":
            return PacketType.Republish
        elif pkt_type == "PubRel":
            return PacketType.PubRel
        elif pkt_type == "Connect":
            return PacketType.Connect
        elif pkt_type == "Disconnect":
            return PacketType.Disconnect
        elif pkt_type == "Subscribe":
            return PacketType.Subscribe
        elif pkt_type == "Unsubscribe":
            return PacketType.Unsubscribe

    # Sensor functions
    def _read_sensor(self):
        return -1
    # Publish packet functions 
    def _push_packet(self, pkt):
        return pkt
    # PubRec packet functions
    def _build_pubrec_packet(self, pkt_id):
        packet = {
            "type": "PubRec",
            "pktid": pkt_id
            }
        return packet
    # PubComp packet functions
    def _build_pubcomp_packet(self, pkt_id: str):
         packet = {
            "type": "PubComp",
            "pktid": pkt_id
            }
         return packet
    # ConnRec packet functions 
    def _process_connrec(self, pkt: dict) -> tuple(bool, bool):
        # Connection validations
        client_id = pkt["clientid"]
        clean_session = pkt["cleansession"]
        topic = pkt["topic"]
        ok_credentials, msg, code = self._connection_validations(pkt["username"], 
                                                                 pkt["password"],
                                                                 pkt["topic"],
                                                                 pkt["qos"],
                                                                 pkt["keepalive"],
                                                                 )
        
        #if not ok_credentials:
        #    raise f"Connection rejected. {msg}"
        #else:
        #    if clean_session:
        #        session_present = False
        #    elif self.publishers_to_connections.get(client_id, False):
        #        if self.topics_to_messages[topic]:
        #            session_present = True
        #        else:
        #            session_present = False
        #    else:
        #        session_present = False
        session_present = False
        if not ok_credentials:
            raise f"Connection rejected. {msg}"
        elif self.publishers_to_connections.get(client_id, False):
            if self.topics_to_messages[topic]:
                session_present = True
            elif clean_session:
                self.topics_to_messages[topic]["payload"] = ""
        if not session_present:
            self._store_connection(pkt)
            socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket.connect(pkt["ip"], pkt["port"])
            self.clients_to_sockets[client_id] = socket
        return session_present, code

    def _store_connection(self, pkt: dict):
        data = {
            "topic": pkt["topic"],
            "qos": pkt["qos"],
            "payload": "",
            "lastWillmessage": pkt["lastWillmessage"],
            "retain": pkt["retain"],
            "keepalive": pkt["keepalive"],
            "cleansession": pkt["cleansession"]
        }
        self.publishers_to_connections[pkt["clientId"]] = data

    def _connection_validations(self, 
                                client_id: str,
                                user: str, 
                                password: str, 
                                topic: str, 
                                qos: int, 
                                keep_alive: float) -> tuple(bool, str, int):
        accepted = False
        if user != self.username or password != self.password:
            return accepted, "Invalid username or password. Connection rejected", 4
        if not self.publishers_to_connections.get(client_id, False):
            return accepted, "Invalid clientId. Connection rejected", 2
        if qos > 2:
            return accepted, "QoS greater than 2. Connection rejected", 6
        if (keep_alive < self.min_keepalive or 
            keep_alive > self.max_keepalive):
            return accepted, f"Keep alive less than {self.min_keepalive} or greater than {self.max_keepalive}", 7
        accepted = True
        return accepted, "ok", 0
        
    def _build_connrec_packet(self, pkt_id, session_present, code):
        packet = {
            "type": "ConnRec",
            "sessionpresent": session_present,
            "returncode": code
            }
        return packet
        
    # DicConn packet functions 
    def _disconect_publisher(self, pkt_id):
        pass
    # SuBack packet functions 
    def _process_suback(self, pkt):
        codes = []
        for i, subscription in enumerate(pkt["subscriptions"]):
            subs_topic = subscription["topic"]
            subs_qos = subscription["qos"]
            # id example: subscriptorid_topicFormat
            subs_id = pkt["id"] + "_" + subs_topic
            status = self._valid_packet(subs_id, subs_qos)
            if status <= 2:
                self.subscriptions[subs_id] = {
                    "subscriberid": subs_id,
                    "topic": subs_topic,
                    "qos": subs_qos
                    }
            codes.append(status)
        return codes
    def _build_suback_packet(self, pkt_id, codes):
        packet = {
            "type": "SuBack",
            "pktid": pkt_id,
            "returncodes": codes
            }
        return packet
    # UnSuBack packet functions
    def process_unsuback(self, pkt):
        for unsub_id in pkt["unsubscriptionsid"]:
            self.subscriptions.pop(unsub_id)

    def _build_unsuback_packet(self, pkt_id):
        packet = {
            "type": "UnsuBack",
            "pktid": pkt_id
            }
        return packet
    
    # Packet validation process
    def _valid_packet(self, device_id, topic, qos):
        if (device_id in self.subscriptions or \
           qos > 2 or
           topic not in self.topics_published):
            return 128
        else:
            return qos
    
    # Send packets
    def _send_packets(self):
        for socket in self.s

    # Process incoming packets
    def _process_packet(self):
        if self.incoming_packet:
            data = self._process_communication()
            data = json.dumps(data)
            self._send_to_socket(data)
        else:
            if self.current_pkt_type == PacketType.ConnAck:
                self.ok_broker_conn = True
        data = self._process_communication()
        data = json.dumps(data)
        self._send_to_socket(data)

    def _process_communication(self, data, pkt_type, qos):
        if qos == 0:
            return self._push_packet(data)
        else:
            self._process_current_pkt_type(pkt_type)
            return self._process_nonzero_qos(self, data, pkt_type, qos)

    def _process_nonzero_qos(self, data, pkt_type, qos):
        if (qos == 1 and (pkt_type == PacketType.Publish or 
                          pkt_type == PacketType.Republish)
            or
           (qos == 2 and (pkt_type == PacketType.Publish or 
                          pkt_type == PacketType.Republish)
                         )):
            return self._push_packet(data)
        elif qos == 2:
            if pkt_type == PacketType.PubRel:
                return self._build_pubrec_packet()
        if pkt_type == PacketType.Republish and self.on_republish:
            return self.original_msg

def main():
    if __name__ == "_main_":
        broker = Broker()
        broker.connect_socket("localhost", 8888)
        t_start = time.perf_counter()
        retake_send = broker.republish_time
        while True:
            if (time.perf_counter() - t_start) > retake_send:
                # send data to the broker
                broker.send_packet()
                t_start = time.perf_counter()
            else:
                print("Waiting...")
