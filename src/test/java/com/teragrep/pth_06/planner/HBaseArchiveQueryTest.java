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

import com.teragrep.pth_06.ast.analyze.ScanRanges;
import com.teragrep.pth_06.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class HBaseArchiveQueryTest {

    final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final String userName = "sa";
    final String password = "";
    final Connection conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, userName, password));
    Map<String, String> opts = new HashMap<>();
    TestingHBaseCluster testCluster;
    LogfileTable logfileTable;

    @BeforeAll
    public void setupDefaultOpts() {
        opts.put("queryXML", "query");
        opts.put("archive.enabled", "true");
        opts.put("hbase.enabled", "true");
        opts.put("S3endPoint", "S3endPoint");
        opts.put("S3identity", "S3identity");
        opts.put("S3credential", "S3credential");
        opts.put("DBusername", userName);
        opts.put("DBpassword", password);
        opts.put("DBurl", url);
        opts.put("quantumLength", "15");
        final TestingHBaseClusterOption clusterOption = TestingHBaseClusterOption
                .builder()
                .numMasters(1)
                .numRegionServers(1)
                .build();
        testCluster = TestingHBaseCluster.create(clusterOption);
        Configuration conf = testCluster.getConf();
        conf.set("hbase.master.hostname", "localhost");
        conf.set("hbase.regionserver.hostname", "localhost");
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Assertions.assertDoesNotThrow(testCluster::start);
    }

    @AfterAll
    public void stop() {
        if (testCluster.isClusterRunning()) {
            Assertions.assertDoesNotThrow(testCluster::stop);
        }
        Assertions.assertDoesNotThrow(conn::close);
    }

    @BeforeEach
    public void setup() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS STREAMDB").execute();
            conn.prepareStatement("USE STREAMDB").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS host").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS stream").execute();
            conn.prepareStatement("DROP TABLE IF EXISTS log_group").execute();
            conn
                    .prepareStatement(
                            "CREATE TABLE `log_group` (\n" + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
                                    + "  `name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  PRIMARY KEY (`id`)\n" + ")"
                    )
                    .execute();
            conn
                    .prepareStatement(
                            "CREATE TABLE `host` (\n" + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
                                    + "  `name` varchar(175) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  `gid` int(10) unsigned NOT NULL,\n" + "  PRIMARY KEY (`id`),\n"
                                    + "  KEY `host_gid` (`gid`),\n" + "  KEY `idx_name_id` (`name`,`id`),\n"
                                    + "  CONSTRAINT `host_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE\n"
                                    + ")"
                    )
                    .execute();
            conn
                    .prepareStatement(
                            "CREATE TABLE `stream` (\n" + "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
                                    + "  `gid` int(10) unsigned NOT NULL,\n"
                                    + "  `directory` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  `stream` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  `tag` varchar(48) COLLATE utf8mb4_unicode_ci NOT NULL,\n"
                                    + "  PRIMARY KEY (`id`),\n" + "  KEY `stream_gid` (`gid`),\n"
                                    + "  CONSTRAINT `stream_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE\n"
                                    + ") "
                    )
                    .execute();
        });
        Assertions.assertTrue(testCluster.isClusterRunning());
        logfileTable = Assertions.assertDoesNotThrow(() -> new LogfileTable(testCluster.getConf(), new Config(opts)));
        MockDBData mockDBData = new MockDBData();
        Assertions.assertDoesNotThrow(() -> logfileTable.insertResults(mockDBData.getVirtualDatabaseMap().values()));
    }

    @Test
    public void testResultsWithTestData() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        String queryString = "<AND><index operation=\"EQUALS\" value=\"f17_v2\"/><earliest operation=\"EQUALS\" value=\"1262905200\"/></AND>";
        opts.put("queryXML", queryString);
        // opts.put("batch.size.totalObjectCountLimit", "1"); // limit to 1 object for hourly slice
        Config config = new Config(opts);
        HBaseArchiveQuery query = new HBaseArchiveQuery(
                config,
                new ScanRanges(config),
                new TreeMap<>(),
                new LogfileTable(testCluster.getConf(), config),
                1300000000L
        );
        Assertions.assertFalse(query.isStub());
        Long initialOffset = query.getInitialOffset();
        Assertions.assertEquals(1262905200L, initialOffset);
        Long incrementedOffset = query.incrementAndGetLatestOffset();
        Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> result = query
                .processBetweenUnixEpochHours(initialOffset, incrementedOffset);
        System.out.println("RESULT: " + result.get(0) + "\nresult size: " + result.size());
    }

    @Test
    public void testNoScanRangesZeroOffset() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        opts
                .put(
                        "queryXML",
                        "<AND><index value=\"*\" operation=\"EQUALS\"/><earliest operation=\"EQUALS\" value=\"1643207821\"/></AND>"
                );
        Config config = new Config(opts);
        ArchiveQuery hBaseArchiveQuery = new HBaseArchiveQuery(config);
        Assertions.assertEquals(1643207821L, hBaseArchiveQuery.getInitialOffset());
    }

    @Test
    public void testIncrement() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        opts
                .put(
                        "queryXML",
                        "<AND><index value=\"*\" operation=\"EQUALS\"/><earliest operation=\"EQUALS\" value=\"1757422772\"/></AND>"
                );
        Config config = new Config(opts);
        ArchiveQuery hBaseArchiveQuery = new HBaseArchiveQuery(config);
        Long firstOffset = hBaseArchiveQuery.incrementAndGetLatestOffset();
    }

    @Test
    public void testDefaultEarliest() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        opts.put("queryXML", "<index value=\"test_directory\" operation=\"EQUALS\"/>");
        Config config = new Config(opts);
        ArchiveQuery hBaseArchiveQuery = new HBaseArchiveQuery(config);
        Long initialOffset = hBaseArchiveQuery.getInitialOffset();
        // initial offset is withing 5 seconds of expected
        Long difference = Math
                .abs(ZonedDateTime.now().minusHours(config.archiveConfig.defaultEarliestMinusHours).toEpochSecond() - initialOffset);
        Assertions.assertTrue(difference < 5);
    }

    @Test
    public void testInitialOffsetFromQuery() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        opts
                .put(
                        "queryXML",
                        "<AND><index value=\"test_directory\" operation=\"EQUALS\"/><earliest operation=\"EQUALS\" value=\"1643207821\"/></AND>"
                );
        Config config = new Config(opts);
        ArchiveQuery hBaseArchiveQuery = new HBaseArchiveQuery(config);
        Long initialOffset = hBaseArchiveQuery.getInitialOffset();
        Assertions.assertEquals(1643207821L, initialOffset);
    }

    @Test
    public void testHostValue() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        opts
                .put(
                        "queryXML",
                        "<AND><host value=\"test_host\" operation=\"EQUALS\"/><earliest operation=\"EQUALS\" value=\"1643207821\"/></AND>"
                );
        Config config = new Config(opts);
        ArchiveQuery hBaseArchiveQuery = new HBaseArchiveQuery(config);
        Long initialOffset = hBaseArchiveQuery.getInitialOffset();
        Assertions.assertEquals(1643207821L, initialOffset);
    }

    @Test
    public void testSourceSystem() {
        Assertions.assertDoesNotThrow(this::insertTestValues);
        opts
                .put(
                        "queryXML",
                        "<AND><sourcetype value=\"test_stream_2\" operation=\"EQUALS\"/><earliest operation=\"EQUALS\" value=\"1643207821\"/></AND>"
                );
        Config config = new Config(opts);
        ArchiveQuery hBaseArchiveQuery = new HBaseArchiveQuery(config);
        Long initialOffset = hBaseArchiveQuery.getInitialOffset();
        Assertions.assertEquals(1643207821L, initialOffset);
    }

    private void insertTestValues() throws SQLException {
        conn.prepareStatement("USE STREAMDB").execute();
        conn.prepareStatement("INSERT INTO `log_group` (`name`) VALUES ('test_group');").execute();
        conn.prepareStatement("INSERT INTO `host` (`name`, `gid`) VALUES ('sc-99-99-14-108', 1);").execute();
        conn
                .prepareStatement(
                        "INSERT INTO `stream` (`gid`, `directory`, `stream`, `tag`) VALUES (1, 'f17_v2', 'log:f17_v2:0', 'test_tag');"
                )
                .execute();
        conn
                .prepareStatement(
                        "INSERT INTO `stream` (`gid`, `directory`, `stream`, `tag`) VALUES (1, 'f17', 'log:f17', 'test_tag');"
                )
                .execute();

    }
}
