/*
 *  Copyright (c) 2020 Temporal Technologies, Inc. All Rights Reserved
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.samples.hello;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.workflowservice.v1.TerminateWorkflowExecutionRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Sample Temporal workflow that demonstrates how to use workflow signal methods to signal from
 * external sources.
 */
public class HelloSignal {

  // Define the task queue name
  static final String TASK_QUEUE = "HelloSignalTaskQueue";

  // Define the workflow unique id
  static final String WORKFLOW_ID = "HelloSignalWorkflow";

  /**
   * The Workflow Definition's Interface must contain one method annotated with @WorkflowMethod.
   *
   * <p>Workflow Definitions should not contain any heavyweight computations, non-deterministic
   * code, network calls, database operations, etc. Those things should be handled by the
   * Activities.
   *
   * @see io.temporal.workflow.WorkflowInterface
   * @see io.temporal.workflow.WorkflowMethod
   */
  @WorkflowInterface
  public interface GreetingWorkflow {
    /**
     * This is the method that is executed when the Workflow Execution is started. The Workflow
     * Execution completes when this method finishes execution.
     */
    @WorkflowMethod
    List<String> getGreetings();

    // Define the workflow waitForName signal method. This method is executed when the workflow
    // receives a signal.
    @SignalMethod
    void waitForName(String name);

    // Define the workflow exit signal method. This method is executed when the workflow receives a
    // signal.
    @SignalMethod
    void exit();
  }

  // Define the workflow implementation which implements the getGreetings workflow method.
  public static class GreetingWorkflowImpl implements GreetingWorkflow {

    // messageQueue holds up to 10 messages (received from signals)
    List<String> messageQueue = new ArrayList<>(10);
    boolean exit = false;

    @Override
    public List<String> getGreetings() {
      List<String> receivedMessages = new ArrayList<>(10);

      while (true) {
        // Block current thread until the unblocking condition is evaluated to true
        Workflow.await(() -> !messageQueue.isEmpty() || exit);
        if (messageQueue.isEmpty() && exit) {
          // no messages in queue and exit signal was sent, return the received messages
          return receivedMessages;
        }
        String message = messageQueue.remove(0);
        receivedMessages.add(message);
      }
    }

    @Override
    public void waitForName(String name) {
      messageQueue.add("Hello " + name + "!");
    }

    @Override
    public void exit() {
      exit = true;
    }
  }

  private static WorkflowClient getRealWorkflowClient() {
    WorkflowServiceStubs service = WorkflowServiceStubs.newInstance();
    return WorkflowClient.newInstance(service);
  }

  private static WorkerFactory getRealWorkerFactory(WorkflowClient client) {
    return WorkerFactory.newInstance(client);
  }

  private static TestWorkflowEnvironment testEnv;

  private static WorkflowClient getTestWorkflowClient() {
    testEnv =
        TestWorkflowEnvironment.newInstance(
            TestEnvironmentOptions.newBuilder()
                .setWorkerFactoryOptions(WorkerFactoryOptions.newBuilder().build())
                .build());

    return testEnv.getWorkflowClient();
  }

  private static WorkerFactory getTestWorkerFactory() {
    return testEnv.getWorkerFactory();
  }

  /**
   * With the Workflow and Activities defined, we can now start execution. The main method starts
   * the worker and then the workflow.
   */
  public static void main(String[] args) throws Exception {

    /*
     * Get a Workflow service client which can be used to start, Signal, and Query Workflow Executions.
     */
    WorkflowClient client;
    WorkerFactory factory;
    boolean useReal = false;
    if (useReal) {
      client = getRealWorkflowClient();
      factory = getRealWorkerFactory(client);
    } else {
      client = getTestWorkflowClient();
      factory = getTestWorkerFactory();
    }

    /*
     * Define the workflow worker. Workflow workers listen to a defined task queue and process
     * workflows and activities.
     */
    Worker worker = factory.newWorker(TASK_QUEUE);

    /*
     * Register the workflow implementation with the worker.
     * Workflow implementations must be known to the worker at runtime in
     * order to dispatch workflow tasks.
     */
    worker.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class);

    /*
     * Start all the workers registered for a specific task queue.
     * The started workers then start polling for workflows and activities.
     */
    factory.start();

    // Create the workflow options
    WorkflowOptions workflowOptions =
        WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(WORKFLOW_ID).build();

    // Create the workflow client stub. It is used to start the workflow execution.
    WorkflowStub workflow = client.newUntypedWorkflowStub("GreetingWorkflow", workflowOptions);

    // Start workflow asynchronously and call its getGreeting workflow method
    WorkflowExecution execution = workflow.start();

    // Don't send any signals - just let the workflow sit there so we can terminate it and see what
    // happens.

    TerminateWorkflowExecutionRequest terminateWorkflowExecutionRequest =
        TerminateWorkflowExecutionRequest.newBuilder()
            .setReason("testing")
            .setWorkflowExecution(execution)
            .setNamespace("default")
            .build();

    client
        .getWorkflowServiceStubs()
        .blockingStub()
        .terminateWorkflowExecution(terminateWorkflowExecutionRequest);

    try {
      System.out.println("Calling getResult");
      workflow.getResult(5_000, TimeUnit.MILLISECONDS, List.class);
      throw new RuntimeException("getResult should have thrown a TerminatedFailure");
    } catch (WorkflowFailedException e) {
      System.out.println("getResult failed-fast as expected");
      // This is expected
    } finally {
      factory.shutdown();
    }
  }
}
