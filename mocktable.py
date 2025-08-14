@Test
public void testHappyPathWithDependenciesAndStepFunction() throws Exception {
    when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
        .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

    mockSsmGetParametersByPath(List.of(createParameter("blockingParam")));
    when(dynamoDBLockClient.acquireLock(anyString())).thenReturn(mock(AcquireLockOptions.class));

    when(ssmClient.getParameter(any(GetParameterRequest.class)))
        .thenReturn(GetParameterResponse.builder()
            .parameter(Parameter.builder().value("token123").build()).build());

    handler.accept(createEvent());

    verify(dynamoDBLockClient, times(1)).acquireLock(any());
    verify(ssmClient, times(1)).putParameter(any(PutParameterRequest.class));
    verify(sfnClient, times(1)).sendTaskSuccess(any(SendTaskSuccessRequest.class));
}

@Test
public void testBlockingParameterWithBlankToken() throws Exception {
    when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
        .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

    mockSsmGetParametersByPath(List.of(createParameter("blockingParam")));
    when(dynamoDBLockClient.acquireLock(anyString())).thenReturn(mock(AcquireLockOptions.class));

    when(ssmClient.getParameter(any(GetParameterRequest.class)))
        .thenReturn(GetParameterResponse.builder()
            .parameter(Parameter.builder().value("").build()).build());

    handler.accept(createEvent());

    verify(sfnClient, never()).sendTaskSuccess(any());
}

@Test
public void testMultipleDependencies() throws Exception {
    when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
        .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

    mockSsmGetParametersByPath(List.of(createParameter("dep1"), createParameter("dep2")));
    when(dynamoDBLockClient.acquireLock(anyString())).thenReturn(mock(AcquireLockOptions.class));

    handler.accept(createEvent());

    verify(dynamoDBLockClient, times(1)).acquireLock(any());
}

@Test
public void testExceptionDuringPutParameter() throws Exception {
    when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
        .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

    mockSsmGetParametersByPath(List.of(createParameter("blockingParam")));
    when(dynamoDBLockClient.acquireLock(anyString())).thenReturn(mock(AcquireLockOptions.class));

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
    when(dynamoDBLockClient.acquireLock(anyString())).thenReturn(mock(AcquireLockOptions.class));
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

    // Using reflection to invoke the private method for test purposes
    List<Parameter> params = handlerTestInvokeGetParametersByPath("prefix");

    assertEquals(2, params.size());
}

// Helper to call private getParametersByPath
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
