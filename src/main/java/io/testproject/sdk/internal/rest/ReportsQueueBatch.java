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

import io.testproject.sdk.internal.rest.messages.Report;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

public class ReportsQueueBatch extends ReportsQueue {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ReportsQueueBatch.class);

    /**
     * remote url to send the reports to.
     */
    private final URL remoteAddress;

    /**
     * Array list to store represent the reports batch before sending them.
     * Those reports will be extract from the queue.
     */
    private List<Report> batchReports = new LinkedList<>();

    /**
     * Batch report size is maximum 10 reports.
     */
    private static final int MAX_REPORTS_BATCH_SIZE = 10;

    /**
     * Initializes a new instance of the class.
     *
     * @param httpClient HTTP client ot use for communicating with the Agent.
     * @param sessionId  Driver session ID.
     * @param remoteAddress Reports remote address.
     */
    ReportsQueueBatch(final CloseableHttpClient httpClient, final String sessionId, final URL remoteAddress) {
        super(httpClient, sessionId);
        this.remoteAddress = remoteAddress;
    }


    /**
     * Handle the report.
     * From version 3.1.0 -> send reports in batches.
     * For lower versions -> Send standalone report
     */
    @Override
    void handleReport() throws InterruptedException {
        // Extract and remove up to 10 items or till queue is empty from queue - without blocking it.
        if (!getQueue().isEmpty()) {
            QueueItem item = null;
            do {
                // Get the first item in the queue without blocking it.
                item = getQueue().poll();
                if (null != item && batchReports.size() < MAX_REPORTS_BATCH_SIZE) {
                    batchReports.add(item.getReport());
                }
            } while (!getQueue().isEmpty() && batchReports.size() < MAX_REPORTS_BATCH_SIZE);

            // Create json from reports list
            String json = null;
            try {
                json = getGSON().toJson(batchReports);
            } catch (Exception e) {
                LOG.error("Failed to create json from list: [{}]", batchReports, e);
                return;
            }

            StringEntity entity = new StringEntity(json, StandardCharsets.UTF_8);

            // Create httpPost and execute
            HttpPost httpPost = new HttpPost(this.remoteAddress + AgentClient.Routes.REPORT_BATCH);
            httpPost.setEntity(entity);

            CloseableHttpResponse response = null;
            try {
                response =  this.getHttpClient().execute(httpPost);
            } catch (IOException e) {
                LOG.error("Failed to submit report: [{}]", batchReports, e);
                return;
            } finally {
                // Consume response to release the resources
                if (response != null) {
                    EntityUtils.consumeQuietly(response.getEntity());
                }
                batchReports.clear();
            }

            // Handle unsuccessful response
            if (response != null && response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                LOG.error("Agent responded with an unexpected status {} to report: [{}]",
                        response.getStatusLine().getStatusCode(), batchReports);
            }

        }
    }

}
