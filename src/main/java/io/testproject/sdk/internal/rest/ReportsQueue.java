/*
 * Copyright (c) 2020 TestProject LTD. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.testproject.sdk.internal.rest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.testproject.sdk.internal.exceptions.AgentConnectException;
import io.testproject.sdk.internal.rest.messages.Report;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * A runnable class to manage reports queue.
 */
public class ReportsQueue implements Runnable {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ReportsQueue.class);

    /**
     * An instance of the Google JSON serializer to serialize and deserialize objects.
     */
    protected static final Gson GSON = new GsonBuilder().create();

    /**
     * Progress report delay in seconds.
     */
    private static final int PROGRESS_REPORT_DELAY = 3;

    /**
     * In case of failure during report - attempt maximum 4 times.
     */
    protected static final int MAX_REPORT_FAILURE_ATTEMPTS = 4;

    /**
     * Queue to synchronize reports sent to Agent.
     */
    private final ArrayBlockingQueue<QueueItem> queue = new ArrayBlockingQueue<>(1024 * 1024);

    /**
     * HTTP client to submit reports to the Agent.
     */
    private final CloseableHttpClient httpClient;

    /**
     * Driver session ID.
     */
    private final String sessionId;

    /**
     * Flag to keep running the loop of taking items from the queue.
     */
    private boolean running;

    /**
     * Future to report remaining reports in queue.
     */
    private Future<?> progressFuture;

    /**
     *  In case of 4 failures to send reports to the agent.
     */
    private boolean brokenReports = false;

    /**
     * Initializes a new instance of the class.
     *
     * @param httpClient HTTP client ot use for communicating with the Agent.
     * @param sessionId  Driver session ID.
     */
    public ReportsQueue(final CloseableHttpClient httpClient, final String sessionId) {
        this.httpClient = httpClient;
        this.sessionId = sessionId;
    }

    /**
     * Access method to get the queue.
     * @return the reports queue.
     */
    public ArrayBlockingQueue<QueueItem> getQueue() {
        return queue;
    }

    /**
     * Access method to get the httpClient.
     * @return the httpClient.
     */
    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    /**
     * Adds a report to the queue.
     *
     * @param request Request to be sent over HTTP.
     * @param report  Report that this request contains.
     */
    void submit(final HttpEntityEnclosingRequestBase request, final Report report) {
        if (!this.brokenReports) {
            this.queue.add(new QueueItem(request, report));
        }
    }

    /**
     * Handle the report.
     * From version 3.1.0 -> send reports in batches.
     * For lower versions -> Send standalone report
     * @throws InterruptedException
     * @throws AgentConnectException in case of 4 failures to send reports to the agent
     */
    void handleReport() throws InterruptedException, AgentConnectException {
        sendReport(this.queue.take());
    }

    /**
     * Runnable flow that looks into the queue and waits for new items.
     */
    @Override
    public void run() {
        this.running = true;
        while (this.running || !this.queue.isEmpty()) {
            try {
                handleReport();
            } catch (InterruptedException e) {
                LOG.error("Reports queue was interrupted");
                break;
            } catch (AgentConnectException e) {
                this.brokenReports = true;
            }
        }

        LOG.trace("Reports queue for session [{}] has been stopped.", sessionId);

        if (!this.queue.isEmpty()) {
            LOG.warn("There are {} unreported items in the queue", this.queue.size());
        }
    }

    /**
     * Submits a report to the Agent via HTTP RESTFul API endpoint.
     *
     * @param item Item retrieved from the queue (report & HTTP request).
     * @throws AgentConnectException in case of 4 failures to send reports to the agent
     */
    void sendReport(final QueueItem item) throws AgentConnectException {
        if (item.getRequest() == null && item.getReport() == null) {
            if (this.running) {
                // There nulls are not OK, something went wrong preparing the report/request.
                LOG.error("Empty report and request were submitted to the queue!");
            }

            // These nulls are OK - they were added by stop() method on purpose.
            return;
        }

        int reportAttemptsCount = MAX_REPORT_FAILURE_ATTEMPTS;
        CloseableHttpResponse response = null;
        do {
            try {
                response = this.httpClient.execute(item.getRequest());
            } catch (IOException e) {
                LOG.error("Failed to submit report: [{}]", item.getReport(), e);
                return;
            } finally {
                // Consume response to release the resources
                if (response != null) {
                    EntityUtils.consumeQuietly(response.getEntity());
                }
            }

            // Handle unsuccessful response
            if (response != null && response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                LOG.error("Agent responded with an unexpected status {} to report: [{}]",
                        response.getStatusLine().getStatusCode(), item.getReport());
                reportAttemptsCount--;
                if (reportAttemptsCount == 0) {
                    LOG.error("Failed to send reports to the agent.");
                    throw new AgentConnectException("Failed to send reports to the agent.");
                }

                LOG.error("Attempt to send report again to the Agent. {} more attempts are left.", reportAttemptsCount);
            }
        } while (response != null && response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK);
    }


    /**
     * Stops the runnable and the queue processing.
     */
    public void stop() {
        LOG.trace("Raising flag to stop reports queue for session [{}]", sessionId);
        this.running = false;

        // Feed the queue with one more (null) object.
        // This is required to to let it proceed with the loop to evaluate the condition (running?) again.
        // Note: Sending null as QueueItem is not possible since ArrayBlockingQueue prohibit null elements.
        this.queue.add(new ReportsQueue.QueueItem(null, null));

        // Start a scheduled future when stopping the queue to log to console the remaining items left to be reported.
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        if (progressFuture == null) {
            progressFuture = scheduler.scheduleAtFixedRate(() -> {
                        Thread.currentThread().setName("Queue-Progress-Report");
                        LOG.info("There are [{}] outstanding reports that should be transmitted to the Agent before"
                                + " the process exits.", queue.size());
                        if (queue.isEmpty()) {
                            LOG.trace("Reporting queue is empty, stopping progress report...");
                            progressFuture.cancel(true);
                            scheduler.shutdown();
                        }
                    },
                    0, PROGRESS_REPORT_DELAY, TimeUnit.SECONDS);
        }
    }

    static class QueueItem {

        /**
         * HTTP request.
         */
        private final HttpEntityEnclosingRequestBase request;

        /**
         * Report that the request will transmit.
         */
        private final Report report;

        /**
         * Getter for {@link #request} field.
         *
         * @return value of {@link #request} field
         */
        HttpEntityEnclosingRequestBase getRequest() {
            return request;
        }

        /**
         * Getter for {@link #report} field.
         *
         * @return value of {@link #report} field
         */
        Report getReport() {
            return report;
        }

        /**
         * Initializes a new instance of the class.
         *
         * @param request HTTP request to be transmitted to the Agent.
         * @param report  Report that the request contains.
         */
        QueueItem(final HttpEntityEnclosingRequestBase request, final Report report) {
            this.request = request;
            this.report = report;
        }
    }

}
