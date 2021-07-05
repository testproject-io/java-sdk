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

import java.util.Map;

/**
 * Payload object sent to the Agent when reporting a driver command.
 */
public final class DriverCommandReport extends Report {
    /**
     * Command executed by the driver.
     */
    private final String commandName;

    /**
     * Command parameters.
     */
    private final Map<String, ?> commandParameters;

    /**
     * Command execution result in a form of a String.
     */
    private final Object result;

    /**
     * Boolean flag to indicate command successful execution or failure.
     */
    private final boolean passed;

    /**
     * Screenshot as base64 string.
     */
    private String screenshot;

    /**
     * Define type as Command for batch report support.
     */
    private final ReportItemType type = ReportItemType.Command;


    /**
     * Getter for {@link #screenshot} field.
     *
     * @return value of {@link #screenshot} field
     */
    public String getScreenshot() {
        return screenshot;
    }

    /**
     * Setter for {@link #screenshot} field.
     * @param screenshot Screenshot (as base64 string) to be set.
     */
    public void setScreenshot(final String screenshot) {
        this.screenshot = screenshot;
    }

    /**
     * Creates a new instance using provided commandName, result and boolean success/failure flag.
     *
     * @param commandName       Name of the commandName executed by the driver.
     * @param commandParameters Executed commandName commandParameters.
     * @param result            Command result formatted as String
     * @param passed            Boolean flag to indicate commandName successful execution or failure.
     */
    public DriverCommandReport(final String commandName, final Map<String, ?> commandParameters,
                               final Object result, final boolean passed) {
        this.commandName = commandName;
        this.commandParameters = commandParameters;
        this.result = result;
        this.passed = passed;
    }

    /**
     * Getter for {@link #commandName} field.
     *
     * @return value of {@link #commandName} field
     */
    public String getCommandName() {
        return commandName;
    }

    /**
     * Getter for {@link #commandParameters} field.
     *
     * @return value of {@link #commandParameters} field
     */
    public Map<String, ?> getCommandParameters() {
        return commandParameters;
    }

    /**
     * Getter for {@link #result} field.
     *
     * @return value of {@link #result} field
     */
    public Object getResult() {
        return result;
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
     * String representation of the class fields.
     * @return String based on {@link #commandName}.
     */
    @Override
    public String toString() {
        return this.commandName;
    }

}
