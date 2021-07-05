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

/**
 * Data model to accommodate test report.
 */
public final class TestReport extends Report {

    /**
     * Test name.
     */
    private String name;

    /**
     * Passed / Failed flag.
     */
    private boolean passed;

    /**
     * Result message.
     */
    private String message;

    /**
     * Define type as Test for batch report support.
     */
    private final ReportItemType type = ReportItemType.Test;

    /**
     * Initializes a new instance of a Test Report.
     *
     * @param name Test name.
     */
    public TestReport(final String name) {
        this.name = name;
    }

    /**
     * Initializes a new instance of a Test Report.
     *
     * @param name    Test name.
     * @param passed  True if passed, otherwise False.
     * @param message Result message.
     */
    public TestReport(final String name, final boolean passed, final String message) {
        this.name = name;
        this.passed = passed;
        this.message = message;
    }

    /**
     * Getter for {@link #name} field.
     *
     * @return value of {@link #name} field
     */
    public String getName() {
        return name;
    }

    /**
     * Setter for {@link #name} field.
     *
     * @param name Value to be set.
     */
    public void setName(final String name) {
        this.name = name;
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
     * Setter for {@link #passed} field.
     *
     * @param passed Value to be set.
     */
    public void setPassed(final boolean passed) {
        this.passed = passed;
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
     * Setter for {@link #message} field.
     *
     * @param message Value to be set.
     */
    public void setMessage(final String message) {
        this.message = message;
    }

    /**
     * String representation of the class fields.
     * @return String based on {@link #name}.
     */
    @Override
    public String toString() {
        return this.name;
    }

}
