import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import net.jpmchase.csorion.message.DatasetAvailability;
import net.jpmchase.csorion.Handler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessRequest;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.*;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class HandlerTest {

    @Mock
    private SsmClient ssmClient;

    @Mock
    private SfnClient sfnClient;

    @Mock
    private AmazonDynamoDBLockClient dynamoDBLockClient;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private Handler handler;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new Handler(objectMapper, ssmClient, sfnClient, dynamoDBLockClient,
                "AVAILABLE", 100L, 10L, "ssm/prefix", "-", ":", "|");
    }

    private SQSEvent createEvent() {
        SQSEvent event = new SQSEvent();
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setBody("{\"raw\":\"raw\",\"dataSource\":\"source\",\"dataset\":\"dataset1\"}");
        event.setRecords(List.of(message));
        return event;
    }

    private void mockSsmGetParametersByPath(List<Parameter> params) {
        GetParametersByPathResponse response = GetParametersByPathResponse.builder()
                .parameters(params)
                .build();
        when(ssmClient.getParametersByPath(any(GetParametersByPathRequest.class))).thenReturn(response);
    }

    private Parameter createParameter(String name) {
        return Parameter.builder().name(name).build();
    }

    // ------------------ Existing tests ------------------

    @Test
    public void testHandlerInstantiation() {
        assertNotNull(handler);
    }

    @Test
    public void testInterruptedExceptionDuringLock() throws Exception {
        when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
                .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

        mockSsmGetParametersByPath(List.of(createParameter("blockingParam")));
        when(dynamoDBLockClient.acquireLock(any())).thenThrow(new InterruptedException());

        assertThrows(RuntimeException.class, () -> handler.accept(createEvent()));
    }

    @Test
    public void testNoBlockingParametersFound() throws Exception {
        when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
                .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

        mockSsmGetParametersByPath(Collections.emptyList());

        handler.accept(createEvent());

        verify(dynamoDBLockClient, never()).acquireLock(any());
    }

    @Test
    public void testBlockingParameterWithEmptyToken() throws Exception {
        when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
                .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

        mockSsmGetParametersByPath(List.of(createParameter("blockingParam")));
        LockItem mockLock = mock(LockItem.class);
        when(dynamoDBLockClient.acquireLock(any())).thenReturn(mockLock);

        when(ssmClient.getParameter(any(GetParameterRequest.class)))
                .thenReturn(GetParameterResponse.builder()
                        .parameter(Parameter.builder().value("").build())
                        .build());

        handler.accept(createEvent());

        verify(sfnClient, never()).sendTaskSuccess(any());
    }

    // ------------------ New missing coverage tests ------------------

    @Test
    public void testHappyPathWithDependenciesAndStepFunction() throws Exception {
        when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
                .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

        mockSsmGetParametersByPath(List.of(createParameter("blockingParam")));
        LockItem mockLock = mock(LockItem.class);
        when(dynamoDBLockClient.acquireLock(any())).thenReturn(mockLock);

        when(ssmClient.getParameter(any(GetParameterRequest.class)))
                .thenReturn(GetParameterResponse.builder()
                        .parameter(Parameter.builder().value("token123").build()).build());

        handler.accept(createEvent());

        verify(dynamoDBLockClient, times(1)).acquireLock(any());
        verify(ssmClient, times(1)).putParameter(any(PutParameterRequest.class));
        verify(sfnClient, times(1)).sendTaskSuccess(any(SendTaskSuccessRequest.class));
    }

    @Test
    public void testMultipleDependencies() throws Exception {
        when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
                .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

        mockSsmGetParametersByPath(List.of(createParameter("dep1"), createParameter("dep2")));
        LockItem mockLock = mock(LockItem.class);
        when(dynamoDBLockClient.acquireLock(any())).thenReturn(mockLock);

        handler.accept(createEvent());

        verify(dynamoDBLockClient, times(1)).acquireLock(any());
    }

    @Test
    public void testExceptionDuringPutParameter() throws Exception {
        when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
                .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

        mockSsmGetParametersByPath(List.of(createParameter("blockingParam")));
        LockItem mockLock = mock(LockItem.class);
        when(dynamoDBLockClient.acquireLock(any())).thenReturn(mockLock);

        doThrow(new RuntimeException("SSM error")).when(ssmClient).putParameter(any(PutParameterRequest.class));

        assertThrows(RuntimeException.class, () -> handler.accept(createEvent()));
    }

    @Test
    public void testJsonProcessingExceptionDuringRead() throws Exception {
        when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
                .thenThrow(new JsonProcessingException("parse error") {});

        handler.accept(createEvent());

        verifyNoInteractions(ssmClient);
        verifyNoInteractions(sfnClient);
    }

    @Test
    public void testLockReleaseReturnsFalse() throws Exception {
        when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
                .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

        mockSsmGetParametersByPath(List.of(createParameter("blockingParam")));
        LockItem mockLock = mock(LockItem.class);
        when(dynamoDBLockClient.acquireLock(any())).thenReturn(mockLock);
        when(dynamoDBLockClient.releaseLock(any())).thenReturn(false);

        handler.accept(createEvent());

        verify(dynamoDBLockClient, times(1)).releaseLock(any());
    }

    @Test
    public void testPaginatedGetParametersByPath() {
        GetParametersByPathResponse firstResponse = GetParametersByPathResponse.builder()
                .parameters(List.of(createParameter("p1")))
                .nextToken("token1")
                .build();

        GetParametersByPathResponse secondResponse = GetParametersByPathResponse.builder()
                .parameters(List.of(createParameter("p2")))
                .build();

        when(ssmClient.getParametersByPath(any(GetParametersByPathRequest.class)))
                .thenReturn(firstResponse)
                .thenReturn(secondResponse);

        List<Parameter> params = handlerTestInvokeGetParametersByPath("prefix");

        assertEquals(2, params.size());
    }

    // Helper to call private getParametersByPath using reflection
    @SuppressWarnings("unchecked")
    private List<Parameter> handlerTestInvokeGetParametersByPath(String prefix) {
        try {
            java.lang.reflect.Method method = Handler.class.getDeclaredMethod("getParametersByPath", String.class);
            method.setAccessible(true);
            return (List<Parameter>) method.invoke(handler, prefix);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
