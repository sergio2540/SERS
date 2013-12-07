package gossip.core;

import gossip.message.Message;
import gossip.message.MessageType;

import java.io.IOException;

public class Gossip {

	private double activeNodes;
	private double activeNodesWeight;
	
	private double activeUsers;
	private double activeUsersWeight;
	
	private double averageFiles;
	private double averageFilesWeight; 
	
	private double averageMb;
	private double averageMbWeight;
	
	public Gossip(double activeNodes, double activeNodesWeight) {
		this.activeNodes = activeNodes;
		this.activeNodesWeight = activeNodesWeight;
	}
	
	public Gossip(){}
	
	public void setValues(double activeNodes, double activeNodesWeight, double activeUsers, double activeUsersWeight, double averageFiles, double averageFilesWeight, double averageMb, double averageMbWeight ) {
		this.activeNodes = activeNodes;
		this.activeNodesWeight = activeNodesWeight;
		this.activeUsers = activeUsers;
		this.activeUsersWeight = activeUsersWeight;
		this.averageFiles = averageFiles;
		this.averageFilesWeight = averageFilesWeight;
		this.averageMb = averageMb;
		this.averageMbWeight = averageMbWeight;
	}
	
	public void setQ1Values(double activeNodes, double activeNodesWeight) {
		this.activeNodes = activeNodes;
		this.activeNodesWeight = activeNodesWeight;
	}
	

	public void setQ2Values(double activeUsers, double activeUsersWeight) {
		this.activeUsers = activeUsers;
		this.activeUsersWeight = activeUsersWeight;
	}
	
	public boolean approx(double value1, double value2) {
		
		final double ERROR = 0.01;
		
		if(Math.abs(value1-value2) < ERROR){
			return true;
		}else{
			return false;
		}
	
	}
	

	public void setQ3Values( double averageFiles, double averageFilesWeight) {
		this.averageFiles = averageFiles;
		this.averageFilesWeight = averageFilesWeight;
	}
	

	public void setQ4Values(double averageMb, double averageMbWeight) {
		this.averageMb = averageMb;
		this.averageMbWeight = averageMbWeight;
	}
	
	
	public double getActiveNodes() {
		return activeNodes;
	}

	private void setActiveNodes(double activeNodes) {
		this.activeNodes = activeNodes;
	}

	public double getActiveNodesWeight() {
		return activeNodesWeight;
	}

	private void setActiveNodesWeight(double activeNodesWeight) {
		this.activeNodesWeight = activeNodesWeight;
	}

	public double getActiveUsers() {
		return activeUsers;
	}

	private void setActiveUsers(double activeUsers) {
		this.activeUsers = activeUsers;
	}

	public double getActiveUsersWeight() {
		return activeUsersWeight;
	}

	private void setActiveUsersWeight(double activeUsersWeight) {
		this.activeUsersWeight = activeUsersWeight;
	}

	public double getAverageFiles() {
		return averageFiles;
	}

	private void setAverageFiles(double averageFiles) {
		this.averageFiles = averageFiles;
	}

	public double getAverageFilesWeight() {
		return averageFilesWeight;
	}

	private void setAverageFilesWeight(double averageFilesWeight) {
		this.averageFilesWeight = averageFilesWeight;
	}

	public double getAverageMb() {
		return averageMb;
	}

	private void setAverageMb(double averageMb) {
		this.averageMb = averageMb;
	}

	public double getAverageMbWeight() {
		return averageMbWeight;
	}

	private void setAverageMbWeight(double averageMbWeight) {
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
			
			msg = new Message(type, getActiveNodes()/2, getActiveNodesWeight()/2);
			
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
	
}
