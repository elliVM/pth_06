/*
 * Teragrep Archive Datasource (pth_06)
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_06.planner;

import com.teragrep.pth_06.config.Config;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SQLConfigurationTest {

    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";

    @Test
    public void testBloomMatchFunctionMissingException() {
        Map<String, String> opts = options();
        opts.put("bloom.enabled", "true");
        SQLException e = Assertions.assertThrows(SQLException.class, () -> new StreamDBClient(new Config(opts)));
        String expectedMessage = "Expected function 'bloommatch' was not present";
        Assertions.assertEquals(expectedMessage, e.getMessage());
    }

    @Test
    public void testBloomMatchFunctionPresent() {
        Assertions.assertDoesNotThrow(() -> {
            Connection conn = DriverManager.getConnection(url, userName, password);
            conn.prepareStatement("DROP ALIAS IF EXISTS BLOOMMATCH");
            // create bloommatch placeholder udf to test database
            String bloommatch = "CREATE ALIAS BLOOMMATCH " + "AS $$ " + "boolean areEqual(int a, int b) {"
                    + "    return a == b;" + "}" + "$$;";
            conn.prepareStatement(bloommatch).execute();
        });
        Map<String, String> opts = options();
        opts.put("bloom.enabled", "true");
        // throws other errors but check that bloommatch not present error is not thrown
        DataAccessException e = Assertions
                .assertThrows(DataAccessException.class, () -> new StreamDBClient(new Config(opts)));
        Assertions.assertFalse(e.getMessage().contains("bloommatch"));
    }

    @Test
    public void testBloomMatchFunctionCheckSkippedIfBloomNotEnabled() {
        Map<String, String> opts = options();
        opts.put("bloom.enabled", "false");
        // throws other errors but check that bloommatch not present error is not thrown
        DataAccessException e = Assertions
                .assertThrows(DataAccessException.class, () -> new StreamDBClient(new Config(opts)));
        Assertions.assertFalse(e.getMessage().contains("bloommatch"));
    }

    // provides minimal options needed to avoid exceptions
    private Map<String, String> options() {
        Map<String, String> opts = new HashMap<>();
        opts.put("queryXML", "<index value=\"haproxy\" operation=\"EQUALS\"/>");
        opts.put("S3endPoint", "S3endPoint");
        opts.put("S3identity", "S3identity");
        opts.put("S3credential", "S3credential");
        opts.put("DBusername", userName);
        opts.put("DBpassword", password);
        opts.put("DBurl", url);
        opts.put("archive.enabled", "true");
        opts.put("kafka.enabled", "false");
        return opts;
    }
}
