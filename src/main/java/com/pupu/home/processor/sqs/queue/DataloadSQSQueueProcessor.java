package com.pupu.home.processor.sqs.queue;

import java.util.List;
import java.util.UUID;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pupu.home.aws.client.factory.AWSClientFactory;
import com.pupu.home.dto.Member;
import com.pupu.home.processor.LambdaProcessor;
import com.pupu.home.utils.AWSClientType;
import com.pupu.home.utils.DataloadConstants;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class DataloadSQSQueueProcessor implements LambdaProcessor {
	
	public void processMemberRecordsInChunk(List<Member> memberList, LambdaLogger logger) {
		logger.log("DataloadSQSQueueBatchSubmitter.processMemberRecordsInChunk:: START", LogLevel.INFO);
		
		if(memberList != null && memberList.size() > 0) {
			
			int chuckSize = 10;
			int dataSize = memberList.size();
			
			int chuckCounter = 1;
			
			for(int i=0; i<dataSize; i+= chuckSize) {
				int end = Math.min(i + chuckSize, dataSize);
				
				List<Member> chunk = memberList.subList(i, end);
				
				// convert the object to JSON
				String jsonString = getMemberDataListAsJSON(chunk, logger);
				
				// send the data to SQS queue
				submitQueueToSQS(jsonString, chuckCounter, logger);
				
				chuckCounter ++;
			}
		}
		
	}
	
	private void submitQueueToSQS(String jsonString, int chuckCounter, LambdaLogger logger) {
		logger.log("DataloadSQSQueueBatchSubmitter.submitSQSEventInBatch:: START", LogLevel.INFO);
		
		SendMessageRequest messageRequest = SendMessageRequest.builder()
				.queueUrl(System.getenv(DataloadConstants.SQS_QUEUE_URL))
				.messageBody(jsonString)
				.messageGroupId(System.getenv(DataloadConstants.SQS_MSG_GRP_ID))
				.messageDeduplicationId(UUID.randomUUID().toString())
				.build();
		
		getSQSClient().sendMessage(messageRequest);
		logger.log("DataloadSQSQueueBatchSubmitter.submitQueueToSQS:: submitted Queue to SQS for chuck=" + chuckCounter, LogLevel.INFO);
	}
	
	private String getMemberDataListAsJSON(List<Member> memberList, LambdaLogger logger) {
		ObjectMapper objectMapper = new ObjectMapper();
		String jsonString = null;
        try {
        	jsonString = objectMapper.writeValueAsString(memberList);
        } catch (Exception e) {
        	logger.log("SQSEventHandler.getMemberList:: Error: " + e.getMessage(), LogLevel.ERROR);
        }
        return jsonString;
	}
	
	private SqsClient getSQSClient() {
		SqsClient sqsClient = (SqsClient)AWSClientFactory.getInstance().getClient(AWSClientType.SQSCLIENT.name());
		return sqsClient;
	}

}
