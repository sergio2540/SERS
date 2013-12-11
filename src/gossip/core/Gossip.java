package gossip.core;

import gossip.message.Message;
import gossip.message.MessageType;

import java.io.IOException;

import chord.Client;

public class Gossip {

	private double activeNodes;
	private double activeNodesWeight;
	private double lastActiveNodes;
	private double lastActiveNodesWeight;
	private double resetActiveNodes;
	private double resetActiveNodesWeight;

	private double activeUsers;
	private double activeUsersWeight;
	private double lastActiveUsers;
	private double lastActiveUsersWeight;

	private double resetActiveUsers;
	private double resetActiveUsersWeight;

	private double averageFiles;
	private double averageFilesWeight;

	private double lastAverageFiles;
	private double lastAverageFilesWeight; 

	private double resetAverageFiles;
	private double resetAverageFilesWeight; 

	private double averageMb;
	private double averageMbWeight;

	private double lastAverageMb;
	private double lastAverageMbWeight; 

	private double resetAverageMb;
	private double resetAverageMbWeight;

	public Gossip(){}

	public void initActiveNodes(double activeNodes, double activeNodesWeight){
		this.activeNodes = activeNodes;
		this.activeNodesWeight = activeNodesWeight;

		this.resetActiveNodes = activeNodes;
		this.resetActiveNodesWeight = activeNodesWeight;
	}

	public void initActiveUsers(double activeUsers, double activeUsersWeight){
		this.activeUsers = activeUsers;
		this.activeUsersWeight = activeUsersWeight;

		this.resetActiveUsers = activeUsers;
		this.resetActiveUsersWeight = activeUsersWeight;
	}

	public void initAverageFiles(double averageFiles, double averageFilesWeight){
		this.averageFiles = averageFiles;
		this.averageFilesWeight = averageFilesWeight;

		//nao recisa de valores de reset
	}
	public void initAverageMb(double averageMb, double averageMbWeight){
		this.averageMb = averageMb;
		this.averageMbWeight = averageMbWeight;

		//nao recisa de valores de reset
	}

	public void setQueryActiveNodesValues(double activeNodes, double activeNodesWeight) {

		//this.activeNodes -> antigo valor
		//activeNode -> novo valor

		this.lastActiveNodes = this.activeNodes;
		this.lastActiveNodesWeight = this.activeNodesWeight;

		this.activeNodes = activeNodes;
		this.activeNodesWeight = activeNodesWeight;

	}


	public void setQueryActiveUsersValues(double activeUsers, double activeUsersWeight) {

		this.lastActiveUsers = this.activeUsers;
		this.lastActiveUsersWeight = this.activeUsersWeight;

		this.activeUsers = activeUsers;
		this.activeUsersWeight = activeUsersWeight;

	}

	public void setQueryAverageFilesValues(double averageFiles, double averageFilesWeight) {

		this.lastAverageFiles = this.averageFiles;
		this.lastAverageFilesWeight = this.averageFilesWeight;

		this.averageFiles = averageFiles;
		this.averageFilesWeight = averageFilesWeight;

	}

	public void setQueryAverageMbValues(double averageMb, double averageMbWeight) {

		this.lastAverageMb = this.averageMb;
		this.lastAverageMbWeight = this.averageMbWeight;

		this.averageMb = averageMb;
		this.averageMbWeight = averageMbWeight;

	}

	public void resetQueryActiveNodes() {

		this.activeNodes = this.resetActiveNodes;
		this.activeNodesWeight = this.resetActiveNodesWeight;

	}

	public void resetQueryActiveUsers() {

		this.activeUsers = this.resetActiveUsers;
		this.activeUsersWeight = this.resetActiveUsersWeight;

	}

	public void resetQueryAverageFiles() {

		this.averageFiles = Client.getNumberOfFiles();
		this.averageFilesWeight = 1;

	}

	public void resetQueryAverageMb() {

		this.averageMb = Client.getNumberOfFileMBytes();
		this.averageMbWeight = 1;

	}

	public synchronized void processMessage(Message m) {

		switch(m.getMsgType()) {

		case Q1: 

			//devia guardar valor anterior???
			this.activeNodes = this.activeNodes + m.getValue();
			this.activeNodesWeight = this.activeNodesWeight + m.getWeight();
			break;

		case Q2:

			this.activeUsers = this.activeUsers + m.getValue();
			this.activeUsersWeight = this.activeUsersWeight + m.getWeight();
			break;

		case Q3:

			this.averageFiles = this.averageFiles + m.getValue();
			this.averageMbWeight = this.averageMbWeight + m.getWeight();
			break;

		case Q4:

			this.averageMbWeight = this.averageMbWeight + m.getValue();
			this.averageMbWeight = this.averageMbWeight + m.getWeight();
			break;

		default: System.out.println("Query type is invalid.");
		break;

		}

	}



	//Remover setQ1 pq cliente pode nao receber

	public synchronized Message getMessage(MessageType type) {

		Message msg = null;

		switch(type) {

		case Q1: 

			msg = new Message(type, this.activeNodes/2, this.activeNodesWeight/2);
			setQueryActiveNodesValues(this.activeNodes/2, this.activeNodesWeight/2);
			break;

		case Q2:

			msg = new Message(type, this.activeUsers/2,this.activeUsersWeight/2);

			setQueryActiveUsersValues(this.activeUsers/2, this.activeUsersWeight/2);

			break;

		case Q3:

			msg = new Message(type, this.averageFiles/2, this.averageFilesWeight/2);

			setQueryAverageFilesValues(this.averageFiles/2, this.averageFilesWeight/2);

			break;

		case Q4: 

			msg = new Message(type, this.averageMb/2, this.averageMbWeight/2);
			setQueryAverageMbValues(this.averageMb/2, this.averageMbWeight/2);

			break;

		default:

			System.out.println("Invalid query for processing.");
			msg = new Message(type);

			break;

		}

		return msg;

	}

	public synchronized Message getLogOutMessage(MessageType type) {

		Message msg = null;

		switch(type) {

		case Q1: 
			//this.valueQ1 == valor inicial
			msg = new Message(type, Math.abs(this.lastActiveNodes - this.activeNodes), this.activeNodesWeight - 1);
			break;

		case Q2:

			msg = new Message(type, Math.abs(this.lastActiveUsers - this.activeUsers), this.activeUsersWeight - 1);
			break;

		case Q3:

			msg = new Message(type, Math.abs(this.lastAverageFiles - this.averageFiles), this.averageFilesWeight - 1);
			break;

		case Q4: 
			msg = new Message(type,  Math.abs(this.lastAverageMbWeight - this.averageMb), this.averageMbWeight - 1);
			break;

		default:

			System.out.println("Invalid query for processing.");
			msg = new Message(type);

			break;

		}

		return msg;

	}

	public double approx(MessageType type) {


		switch(type) {

		case Q1:

			return Math.abs(this.lastActiveNodes - this.activeNodes);



		case Q2:

			return Math.abs(this.lastActiveUsers - this.activeUsers);


		case Q3:

			return Math.abs(this.lastAverageFiles - this.averageFiles);



		case Q4: 
			return Math.abs(this.lastAverageMbWeight - this.averageMb);

		default:

			System.out.println("Invalid query for processing.");
			return Integer.MAX_VALUE;

		}

	}

	public double average(MessageType type) {


		switch(type) {

		case Q1:

			return this.activeNodes/this.activeNodesWeight;



		case Q2:

			return this.activeUsers/this.activeUsersWeight;


		case Q3:

			return this.averageFiles/this.averageFilesWeight;



		case Q4: 
			return this.averageMb/this.averageMbWeight;

		default:

			System.out.println("Invalid query for processing.");
			return Integer.MAX_VALUE;

		}

	}

}