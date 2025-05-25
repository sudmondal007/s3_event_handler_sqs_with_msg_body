package com.pupu.home.sqs.queue.submitter;

import java.util.List;
import java.util.UUID;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pupu.home.s3.event.dto.Member;
import com.pupu.home.s3.event.utils.DataloadConstants;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class DataloadSQSQueueBatchSubmitter {
	
	public DataloadSQSQueueBatchSubmitter() {}
	
	public DataloadSQSQueueBatchSubmitter(List<Member> memberList, LambdaLogger logger) {
		this.memberList = memberList;
		this.logger = logger;
	}
	
	public void processMemberRecordsInChunk() {
		getLogger().log("DataloadSQSQueueBatchSubmitter.processMemberRecordsInChunk:: START", LogLevel.INFO);
		
		if(getMemberList() != null && getMemberList().size() > 0) {
			
			int chuckSize = 10;
			int dataSize = getMemberList().size();
			
			int chuckCounter = 1;
			
			for(int i=0; i<dataSize; i+= chuckSize) {
				int end = Math.min(i + chuckSize, dataSize);
				
				List<Member> chunk = getMemberList().subList(i, end);
				
				// convert the object to JSON
				String jsonString = getMemberDataListAsJSON(chunk);
				
				// send the data to SQS queue
				submitQueueToSQS(jsonString, chuckCounter);
				
				chuckCounter ++;
			}
		}
		
	}
	
	private void submitQueueToSQS(String jsonString, int chuckCounter) {
		getLogger().log("DataloadSQSQueueBatchSubmitter.submitSQSEventInBatch:: START", LogLevel.INFO);
		
		SendMessageRequest messageRequest = SendMessageRequest.builder()
				.queueUrl(System.getenv(DataloadConstants.SQS_QUEUE_URL))
				.messageBody(jsonString)
				.messageGroupId(System.getenv(DataloadConstants.SQS_MSG_GRP_ID))
				.messageDeduplicationId(UUID.randomUUID().toString())
				.build();
		
		sqsClient.sendMessage(messageRequest);
		getLogger().log("DataloadSQSQueueBatchSubmitter.submitQueueToSQS:: submitted Queue to SQS for chuck=" + chuckCounter, LogLevel.INFO);
	}
	
	private String getMemberDataListAsJSON(List<Member> memberList) {
		ObjectMapper objectMapper = new ObjectMapper();
		String jsonString = null;
        try {
        	jsonString = objectMapper.writeValueAsString(memberList);
        } catch (Exception e) {
        	logger.log("SQSEventHandler.getMemberList:: Error: " + e.getMessage(), LogLevel.ERROR);
        }
        return jsonString;
	}
	
	//SQS Client
	private SqsClient sqsClient = SqsClient.builder()
			.region(Region.of(System.getenv(DataloadConstants.DATALOAD_AWS_REGION)))
			.build();
	
	private List<Member> memberList;
	private LambdaLogger logger;
	
	public SqsClient getSqsClient() {
		return sqsClient;
	}

	public List<Member> getMemberList() {
		return memberList;
	}

	public LambdaLogger getLogger() {
		return logger;
	}

}
