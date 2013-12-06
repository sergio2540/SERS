package gossip.message;

import java.io.Serializable;


@SuppressWarnings("serial")
public class Message implements Serializable {

	MessageType msgType;
	float value;
	float weight;
	
	public Message(MessageType msgType, float value, float weight) {
		this.msgType = msgType;
		this.value = value;
		this.weight = weight;
	}
	
	public Message(MessageType type) {
		this.msgType = type;
	}
	
	public MessageType getMsgType() {
		return msgType;
	}

	public float getValue() {
		return value;
	}

	public float getWeight() {
		return weight;
	}
	
	public String toString() {
		return "" + getMsgType() + "[ value: " + getValue() + ", weight: " + getWeight() + " ]";		
	}
	
}
