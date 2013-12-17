package chord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseManager {

	private RemoteHTable table;

	public void prepareDB(){

		Configuration conf =  HBaseConfiguration.create();
		Cluster cluster = new Cluster();
		cluster.add("ec2-54-194-23-170.eu-west-1.compute.amazonaws.com", 8080);
		Client client = new Client(cluster);
		this.table = new RemoteHTable(client, "DNS");
	}



	public void cleanUp() throws IOException{

		this.table.close();

	}


	public void insert(String newData){

		String row = "1";
		Get get = new Get(Bytes.toBytes(row));
		boolean exists = false;
		Result r = null;

		try {

			exists = table.exists(get);
			if(exists)
				r = table.get(get);

		} catch (IOException e1) {
			System.out.println("Erro ao verificar existencia de entrada em DNS em insert: " + e1.getMessage());
		}

		byte[] oldData = null;
		String dataString = null;


		String toPut = null;

		if(exists){

			oldData = r.getValue(Bytes.toBytes("Peers"), Bytes.toBytes("Peer"));
			dataString = new String(oldData);
			toPut = dataString + " " + newData;

		} else {
			toPut = newData;
		}

		try {
			putToTable(row, "Peers", "Peer", toPut);
		} catch (Exception e) {
			System.out.println("Error while putting data to table on insert: " + e.getMessage());
		}

	}

	public void delete(String data){

		String row = "1";
		Get get = new Get(Bytes.toBytes(row));
		boolean exists = false;
		Result r = null;

		try {

			exists = table.exists(get);
			if(exists)
				r = table.get(get);

		} catch (IOException e1) {
			System.out.println("Erro ao verificar existencia de entrada em DNS em função delete: " + e1.getMessage());
		}

		byte[] oldData = null;
		String dataString = null;

		if(exists){

			oldData = r.getValue(Bytes.toBytes("Peers"), Bytes.toBytes("Peer"));
			dataString = new String(oldData);

			String[] dnsPeers = dataString.split(" ");
			String newData = "";
			int dnsPeersSize = dnsPeers.length;
			String intermediateData = null;

			for(int peerIndex = 0; peerIndex < dnsPeersSize ; peerIndex++){
				intermediateData =dnsPeers[peerIndex];

				if(!intermediateData.equals(data))
					newData += intermediateData;

			}

			try {
				putToTable(row, "Peers", "Peer", intermediateData);
			} catch (Exception e) {
				System.out.println("Error while putting new data on table on delete" + e.getMessage());
			}
		}


	}

	public void putToTable(String row, String family, String  qualifier, String value) throws Exception{

		Put put = new Put(row.getBytes());
		put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());
		this.table.put(put);	

	}

	public String get(){

		String row = "1";
		Get get = new Get(Bytes.toBytes(row));
		boolean exists = false;
		Result r = null;

		try {

			exists = table.exists(get);
			if(exists)
				r = table.get(get);

		} catch (IOException e1) {
			System.out.println("Erro ao verificar existencia de entrada em DNS em get: " + e1.getMessage());
		}

		byte[] oldData = null;
		String dataString = null;

		if(exists){

			oldData = r.getValue(Bytes.toBytes("Peers"), Bytes.toBytes("Peer"));
			dataString = new String(oldData);

		}

		return dataString;
	}



	public void deleteAll(){

		String row = "1";
		Get get = new Get(Bytes.toBytes(row));
		boolean exists = false;
		Result r = null;

		try {

			exists = table.exists(get);
			if(exists)
			r = table.get(get);

		} catch (IOException e1) {
			System.out.println("Erro ao verificar existencia de entrada em DNS em função delete: " + e1.getMessage());
		}

		byte[] oldData = null;
		String dataString = null;

		if(exists){

			oldData = r.getValue(Bytes.toBytes("Peers"), Bytes.toBytes("Peer"));
			dataString = new String(oldData);

			String[] dnsPeers = dataString.split(" ");
		
			try {
				putToTable(row, "Peers", "Peer", dnsPeers[0]);
			} catch (Exception e) {
				System.out.println("Error while putting new data on table on delete" + e.getMessage());
			}
		}

	} 
	
	
	
	
	
}





