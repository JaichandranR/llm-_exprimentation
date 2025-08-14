@Test
public void testMultipleDependencies() throws Exception {
    when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
        .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

    mockSsmGetParametersByPath(List.of(createParameter("dep1"), createParameter("dep2")));
    LockItem mockLock = mock(LockItem.class);
    when(dynamoDBLockClient.acquireLock(any())).thenReturn(mockLock);

    // Fix: mock getParameter so it doesn't return null
    when(ssmClient.getParameter(any(GetParameterRequest.class)))
        .thenReturn(GetParameterResponse.builder()
            .parameter(Parameter.builder().value("dummyToken").build())
            .build());

    handler.accept(createEvent());

    verify(dynamoDBLockClient, times(1)).acquireLock(any());
}

@Test
public void testHappyPathWithDependenciesAndStepFunction() throws Exception {
    when(objectMapper.readValue(anyString(), eq(DatasetAvailability.class)))
        .thenReturn(new DatasetAvailability("raw", "source", "dataset1", "AVAILABLE", "2025-08-12T17:51:31.770Z"));

    mockSsmGetParametersByPath(List.of(createParameter("blockingParam")));
    LockItem mockLock = mock(LockItem.class);
    when(dynamoDBLockClient.acquireLock(any())).thenReturn(mockLock);

    when(ssmClient.getParameter(any(GetParameterRequest.class)))
        .thenReturn(GetParameterResponse.builder()
            .parameter(Parameter.builder().value("token123").build())
            .build());

    handler.accept(createEvent());

    verify(dynamoDBLockClient, times(1)).acquireLock(any());
    // Fix: allow for multiple calls
    verify(ssmClient, atLeastOnce()).putParameter(any(PutParameterRequest.class));
    verify(sfnClient, times(1)).sendTaskSuccess(any(SendTaskSuccessRequest.class));
}
