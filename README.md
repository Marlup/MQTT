# MQTT
MQTT example in Python

**Warning**. This project is under development.

The project contains three main files:
- Broker.py. Functionalities for the broker part in MQTT protocol. It manages incoming client connections, 
  message publishing, subscription and unsubscription.
- Publisher.py. Functionalities for the publisher part in MQTT protocol. It's a template for a device which reads data from a sensor
  and manages communication with Broker.
- Subscriptor.py. Functionalities for the subscriptor part in MQTT protocol. It manages communication to Broker in order to subscribe
  to topics and receive messages from them.


## Constraints and rules imposed on clients (Publisher and Subscriber) and server (Broker)

- Publisher client must send a connection packet with valid user and password to Broker in order to start
  publishing messages. If connection is rejected by the Broker, the publish will also be rejected.
- Subscriber clients do not need previous connection request in order to subscribe and receive messages.
- Subscriber can subscribe to *topics* which are not receiving messages, either because Subscriber forsees messages to come from
  a specific topic, the Publisher has stopped message flow for any reason or a topic containing an unexpected typo was provided.
- Both Publisher and Subscriber can set the Quality of Service level (QoS) of the messages for a certain topic. If an incoming message
  is defined with a QoS level (defined by Publisher) different than QoS level set by a subscription (defined by Subscriptor), the message 
  will be sent with QoS level expected by the subscription.
  
  **to be continued**...
