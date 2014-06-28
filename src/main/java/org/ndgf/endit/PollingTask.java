/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.ndgf.endit;

import java.nio.file.Path;
import java.util.List;

/**
 * A polling task has an initiating action (start), followed by periodic polls
 * for the result.
 *
 * @param <T> The type of the result of the PollingTask
 */
interface PollingTask<T>
{
    /** Returns the list of files to watch for events. */
    List<Path> getFilesToWatch();

    /** Called to initiate the task. */
    void start() throws Exception;

    /**
     * Called when any of the files to watch have an event occur on them.
     *
     * @return The result or null if the task has not completed yet.
     */
    T poll() throws Exception;

    /**
     * Called to abort the task.
     *
     * @return true if the task was aborted, false if the task could not be aborted
     * (presumably because the task already completed).
     */
    boolean abort() throws Exception;
}
