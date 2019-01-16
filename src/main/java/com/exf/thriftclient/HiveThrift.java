package com.exf.thriftclient;

import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

import javax.security.sasl.SaslException;
import java.util.Iterator;
import java.util.List;
/*
 *  to make it work add the hive-service and hadoop-client dependencies
 *  in pom.xml
 */
public class HiveThrift {
	public static void main(String[] args) throws TException, SaslException {

		TTransport transport = HiveAuthFactory.getSocketTransport("34.231.82.9", 10000,99999);
		transport = PlainSaslHelper.getPlainTransport("hive", "hive", transport);

		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		TCLIService.Client client = new TCLIService.Client(protocol);  

		transport.open();
		TOpenSessionReq openReq = new TOpenSessionReq();
		TOpenSessionResp openResp = client.OpenSession(openReq);
		TSessionHandle sessHandle = openResp.getSessionHandle();

		TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "SELECT * FROM exf.individual_profile_schema limit 2");
		TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
		TOperationHandle stmtHandle = execResp.getOperationHandle();

		TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle, TFetchOrientation.FETCH_FIRST, 100);
		TFetchResultsResp resultsResp = client.FetchResults(fetchReq);
		List<TColumn> res=resultsResp.getResults().getColumns();
		for(TColumn tCol: res){
			Iterator<String> it = tCol.getStringVal().getValuesIterator();
	
			while (it.hasNext()){
				
				System.out.println(it.next());
			}
		}
		
		TCloseOperationReq closeReq = new TCloseOperationReq();
		closeReq.setOperationHandle(stmtHandle);
		client.CloseOperation(closeReq);
		TCloseSessionReq closeConnectionReq = new TCloseSessionReq(sessHandle);
		client.CloseSession(closeConnectionReq);

		transport.close();
	}

}