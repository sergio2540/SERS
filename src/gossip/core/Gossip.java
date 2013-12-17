package gossip.core;

import gossip.message.Message;
import gossip.message.MessageType;

import chord.Client;

public class Gossip {

	//Q1
	private double activeNodes;
	private double activeNodesWeight;
	private double resetActiveNodes;
	private double resetActiveNodesWeight;

	
	//Q3
	private double averageFiles;
	private double averageFilesWeight;
	private double initAverageFiles;
	private double initAverageFilesWeight;
	
	//Q4
	private double averageMb;
	private double averageMbWeight;
	private double initAverageMb;
	private double initAverageMbWeight;
	
	private double id = 0;

	public Gossip(){}

	public void initActiveNodes(double activeNodes, double activeNodesWeight){
		this.activeNodes = activeNodes;
		this.activeNodesWeight = activeNodesWeight;

		this.resetActiveNodes = activeNodes;
		this.resetActiveNodesWeight = activeNodesWeight;
	}

	
	public void initAverageFiles(double averageFiles, double averageFilesWeight){
		this.averageFiles = averageFiles;
		this.averageFilesWeight = averageFilesWeight;
		
		this.initAverageFiles = averageFiles;
		this.initAverageFilesWeight = averageFilesWeight;  

	}
	
	public void initAverageMb(double averageMb, double averageMbWeight){
		this.averageMb = averageMb;
		this.averageMbWeight = averageMbWeight;

		
		this.initAverageMb = averageMb;
		this.initAverageMbWeight = averageMbWeight;
		
	}

	public void setQueryActiveNodesValues(double activeNodes, double activeNodesWeight) {

		this.activeNodes = activeNodes;
		this.activeNodesWeight = activeNodesWeight;

	}


	
	public void setQueryAverageFilesValues(double averageFiles, double averageFilesWeight) {

		this.averageFiles = averageFiles;
		this.averageFilesWeight = averageFilesWeight;

	}

	public void setQueryAverageMbValues(double averageMb, double averageMbWeight) {

		this.averageMb = averageMb;
		this.averageMbWeight = averageMbWeight;

	}
	
	public void newGossipRound() {
		this.id = this.id + 1;
	}
	
	public void resetAll() {

		resetQueryActiveNodes();
		resetQueryAverageFiles();
		resetQueryAverageMb();

	}
	
	private void resetQueryActiveNodes() {

		this.activeNodes = this.resetActiveNodes;
		this.activeNodesWeight = this.resetActiveNodesWeight;
		this.initActiveNodes(activeNodes, activeNodesWeight);
		

	}

	private void resetQueryAverageFiles() {

		this.averageFiles = Client.getNumberOfFiles();
		this.averageFilesWeight = 1;
		this.initAverageFiles(averageFiles, averageFilesWeight);

	}

	private void resetQueryAverageMb() {

		this.averageMb = Client.getNumberOfFileMBytes();
		this.averageMbWeight = 1;
		this.initAverageMb(averageMb, averageMbWeight);

	}

	public synchronized void processMessage(Message m) {

		if(m.getId() == this.id){
			//so processa
			
		} else if (m.getId() > this.id){
			this.id = m.getId();
			resetAll();
			//altera e process
			
		} else {
			return;
		}
		
		
		
		switch(m.getMsgType()) {

		case Q1: 

			//devia guardar valor anterior???
			this.activeNodes = this.activeNodes + m.getValue();
			this.activeNodesWeight = this.activeNodesWeight + m.getWeight();
			break;

		case Q3:

			this.averageFiles = this.averageFiles + m.getValue();
			this.averageFilesWeight = this.averageFilesWeight + m.getWeight();
			
			break;

		case Q4:

			this.averageMb = this.averageMb + m.getValue();
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

			msg = new Message(type, this.activeNodes/2, this.activeNodesWeight/2, this.id);
			setQueryActiveNodesValues(this.activeNodes/2, this.activeNodesWeight/2);
			
			break;

		case Q3:

			msg = new Message(type, this.averageFiles/2, this.averageFilesWeight/2, this.id);
			setQueryAverageFilesValues(this.averageFiles/2, this.averageFilesWeight/2);

			break;

		case Q4: 

			msg = new Message(type, this.averageMb/2, this.averageMbWeight/2, this.id);
			setQueryAverageMbValues(this.averageMb/2, this.averageMbWeight/2);

			break;

		default:

			System.out.println("Invalid query for processing.");
			
			break;

		}

		return msg;

	}

	public synchronized Message getLogOutMessage(MessageType type) {

		Message msg = null;
		
		switch(type) {

		case Q1: 
			//this.valueQ1 == valor inicial
			msg = new Message(type, this.activeNodes - this.resetActiveNodes, this.activeNodesWeight - this.resetActiveNodesWeight, this.id);
			break;
			
		case Q3:

			msg = new Message(type, this.averageFiles - this.initAverageFiles, this.averageFilesWeight - this.initAverageFilesWeight, this.id);
			break;

		case Q4: 
			
			msg = new Message(type, this.averageMb - this.initAverageMb, this.averageMbWeight - this.initAverageMbWeight, this.id);
			break;

		default:

			System.out.println("Invalid query for processing.");

			break;

		}

		return msg;

	}

	

	public double average(MessageType type) {


		switch(type) {

		case Q1:

			return this.activeNodes/this.activeNodesWeight;


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