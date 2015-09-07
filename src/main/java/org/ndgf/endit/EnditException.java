/* dCache Endit Nearline Storage Provider
 *
 * Copyright (C) 2015 Gerd Behrmann
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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import java.util.List;

public class EnditException extends Exception
{
    private static final long serialVersionUID = -8875114957535690454L;

    private final int rc;

    public static EnditException create(List<String> lines)
    {
        String error;
        int errorCode;
        if (lines.isEmpty()) {
            errorCode = 1;
            error = "Endit reported a stage failure without providing a reason.";
        } else {
            try {
                errorCode = Integer.parseInt(lines.get(0));
                error = Joiner.on("\n").join(Iterables.skip(lines, 1));
            } catch (NumberFormatException e) {
                errorCode = 1;
                error = Joiner.on("\n").join(lines);
            }
        }
        return new EnditException(errorCode, error);
    }

    public EnditException(int rc, String message)
    {
        super(message);
        this.rc = rc;
    }

    public EnditException(int rc, String message, Throwable cause)
    {
        super(message, cause);
        this.rc = rc;
    }

    public int getReturnCode()
    {
        return rc;
    }
}
