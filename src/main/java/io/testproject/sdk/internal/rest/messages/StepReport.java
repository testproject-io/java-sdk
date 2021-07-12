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

package io.testproject.sdk.internal.rest.messages;


import java.util.UUID;

/**
 * Data model to accommodate step report.
 */
public final class StepReport extends Report {

    /**
     * Report GUID.
     */
    private final String guid;

    /**
     * Step description.
     */
    private final String description;

    /**
     * Step message.
     */
    private final String message;

    /**
     * Step Screenshot.
     */
    private final String screenshot;

    /**
     * Flag to indicate pass/fail state.
     */
    private final boolean passed;

    /**
     * Define type as Step for batch report support.
     */
    private final ReportItemType type = ReportItemType.Step;


    /**
     * Getter for {@link #guid} field.
     *
     * @return value of {@link #guid} field
     */
    public String getGuid() {
        return guid;
    }

    /**
     * Getter for {@link #description} field.
     *
     * @return value of {@link #description} field
     */
    public String getDescription() {
        return description;
    }

    /**
     * Getter for {@link #message} field.
     *
     * @return value of {@link #message} field
     */
    public String getMessage() {
        return message;
    }

    /**
     * Getter for {@link #screenshot} field.
     *
     * @return value of {@link #screenshot} field
     */
    public String getScreenshot() {
        return screenshot;
    }

    /**
     * Getter for {@link #passed} field.
     *
     * @return value of {@link #passed} field
     */
    public boolean isPassed() {
        return passed;
    }

    /**
     * Initializes a new instance of a Step Report.
     * @param description Step description.
     * @param message Step message.
     * @param passed Flag to indicate pass/fail state.
     * @param screenshot Screenshot (PNG as base64 string)
     */
    public StepReport(final String description,
                      final String message,
                      final boolean passed,
                      final String screenshot) {
        this.guid = UUID.randomUUID().toString();
        this.description = description;
        this.message = message;
        this.passed = passed;
        this.screenshot = screenshot;
    }

    /**
     * String representation of the class fields.
     * @return String based on {@link #description}.
     */
    @Override
    public String toString() {
        return this.description;
    }

}
