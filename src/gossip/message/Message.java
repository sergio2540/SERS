package gossip.message;

import java.io.Serializable;


@SuppressWarnings("serial")
public class Message implements Serializable {
	
	MessageType msgType;
	double id;
	double value;
	double weight;
	
	public Message(MessageType msgType, double value, double weight,double id) {
		this.msgType = msgType;
		this.value = value;
		this.weight = weight;
		this.id = id;
	}
	
	public Message(MessageType type) {
		this.msgType = type;
	}
	
	public MessageType getMsgType() {
		return msgType;
	}
	
	public void setId(double id) {
		this.id = id;
	}
	
	public double getId() {
		return id;
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
