package gossip.message;

import java.io.Serializable;


@SuppressWarnings("serial")
public class Message implements Serializable {

	MessageType msgType;
	double value;
	double weight;
	
	public Message(MessageType msgType, double value, double weight) {
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

	public double getValue() {
		return value;
	}

	public double getWeight() {
		return weight;
	}
	
	public String toString() {
		return "" + getMsgType() + "[ value: " + getValue() + ", weight: " + getWeight() + " ]";		
	}
	
}
