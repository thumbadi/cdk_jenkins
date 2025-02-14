package com.example.awssqsdemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class SqsService {
    private static final Logger logger = LoggerFactory.getLogger(SqsService.class);
    
    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final Map<String, MessageData> messageCache = new HashMap<>();

    @Value("${aws.sqs.queueUrl}")
    private String queueUrl;

    public SqsService(SqsClient sqsClient, ObjectMapper objectMapper) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
    }

    public List<String> getAllMessages() {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(10)
                .build();

        List<String> messages = new ArrayList<>();

        try {
            ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);
            
            for (Message message : response.messages()) {
                try {
                    MessageContent msgContent = objectMapper.readValue(message.body(), MessageContent.class);
                    MessageData msgData = new MessageData(message.messageId(), msgContent);
                    
                    // Store in cache using SQS-generated message ID
                    messageCache.put(msgData.getMessageId(), msgData);
                    messages.add(msgData.getMessageId());
                    
                    // Delete the message after successful processing
                    deleteMessage(message.receiptHandle());
                    
                } catch (JsonProcessingException e) {
                    logger.error("Error processing message with ID: {}", message.messageId(), e);
                }
            }
        } catch (SqsException e) {
            logger.error("Error receiving messages from SQS", e);
            throw new RuntimeException("Failed to receive messages from SQS", e);
        }

        return messages;
    }

    private void deleteMessage(String receiptHandle) {
        try {
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();
            
            sqsClient.deleteMessage(deleteRequest);
            logger.debug("Successfully deleted message with receipt handle: {}", receiptHandle);
            
        } catch (SqsException e) {
            logger.error("Error deleting message with receipt handle: {}", receiptHandle, e);
            throw new RuntimeException("Failed to delete message from SQS", e);
        }
    }

    public MessageData getMessageById(String id) {
        MessageData message = messageCache.get(id);
        if (message == null) {
            logger.debug("Message with ID {} not found in cache", id);
        }
        return message;
    }
}
