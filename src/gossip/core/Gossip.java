package gossip.core;

import gossip.message.Message;
import gossip.message.MessageType;

import java.io.IOException;

public class Gossip {

	private float activeNodes;
	private float activeNodesWeight;
	
	private float activeUsers;
	private float activeUsersWeight;
	
	private float averageFiles;
	private float averageFilesWeight; 
	
	private float averageMb;
	private float averageMbWeight;
	
	public Gossip(float activeNodes, float activeNodesWeight) {
		this.activeNodes = activeNodes;
		this.activeNodesWeight = activeNodesWeight;
	}
	
	public Gossip(){}
	
	public void setValues(float activeNodes, float activeNodesWeight,float activeUsers, float activeUsersWeight,float averageFiles, float averageFilesWeight,float averageMb,float averageMbWeight ) {
		this.activeNodes = activeNodes;
		this.activeNodesWeight = activeNodesWeight;
		this.activeUsers = activeUsers;
		this.activeUsersWeight = activeUsersWeight;
		this.averageFiles = averageFiles;
		this.averageMb = averageMb;
		this.averageMbWeight = averageMbWeight;
	}
	
	public float getActiveNodes() {
		return activeNodes;
	}

	private void setActiveNodes(float activeNodes) {
		this.activeNodes = activeNodes;
	}

	public float getActiveNodesWeight() {
		return activeNodesWeight;
	}

	private void setActiveNodesWeight(float activeNodesWeight) {
		this.activeNodesWeight = activeNodesWeight;
	}

	public float getActiveUsers() {
		return activeUsers;
	}

	private void setActiveUsers(float activeUsers) {
		this.activeUsers = activeUsers;
	}

	public float getActiveUsersWeight() {
		return activeUsersWeight;
	}

	private void setActiveUsersWeight(float activeUsersWeight) {
		this.activeUsersWeight = activeUsersWeight;
	}

	public float getAverageFiles() {
		return averageFiles;
	}

	private void setAverageFiles(float averageFiles) {
		this.averageFiles = averageFiles;
	}

	public float getAverageFilesWeight() {
		return averageFilesWeight;
	}

	private void setAverageFilesWeight(float averageFilesWeight) {
		this.averageFilesWeight = averageFilesWeight;
	}

	public float getAverageMb() {
		return averageMb;
	}

	private void setAverageMb(float averageMb) {
		this.averageMb = averageMb;
	}

	public float getAverageMbWeight() {
		return averageMbWeight;
	}

	private void setAverageMbWeight(float averageMbWeight) {
		this.averageMbWeight = averageMbWeight;
	}

	public synchronized void processMessage(Message m) {
		
		switch(m.getMsgType()) {
		
		case Q1: 
				
			setActiveNodes(getActiveNodes() + m.getValue());
			setActiveNodesWeight(getActiveNodesWeight() + m.getWeight());
			break;
			
		case Q2:
			setActiveUsers(getActiveUsers() + m.getValue());
			setActiveUsersWeight(getActiveUsersWeight() + m.getWeight());
			break;
			
		case Q3:
			setAverageFiles(getAverageFiles() + m.getValue());
			setAverageFilesWeight(getAverageFilesWeight() + m.getWeight());
			break;
			
		case Q4:
			setAverageMb(getAverageMb() + m.getValue());
			setAverageMbWeight(getAverageMbWeight() + m.getWeight());
			break;
			
		default: System.out.println("Query type is invalid.");
				break;
		
		}
		
	}
	
	public synchronized Message getMessage(MessageType type) {
		
		Message msg = null;
		
		switch(type) {
		
		case Q1: 
			
			msg = new Message(type, (float)getActiveNodes()/2, (float)getActiveNodesWeight()/2);
			
			break;
		
		case Q2:
			
			msg = new Message(type, getActiveUsers()/2, getActiveUsersWeight()/2);
			
			break;
			
		case Q3:
			
			msg = new Message(type, getAverageFiles()/2, getAverageFilesWeight()/2);
			
			break;
			
		case Q4: 
			
			msg = new Message(type, getAverageMb()/2, getAverageMbWeight()/2);
			
			break;
			
		default:
				System.out.println("Invalid query for processing.");
				msg = new Message(type);
				
			break;

		}
		
		return msg;
		
	}
	
	public void reset() {
	
		this.activeNodes = 1;
		this.activeNodesWeight = 0;
		

	}

	
	
}
