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
package com.teragrep.pth_06.planner.walker.conditions;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.Condition;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class StreamingConditionTest {

    @Test
    public void testValidTags() {
        String[] tags = {
                "index", "sourcetype", "host"
        };
        String value = "123456";
        String operation = "EQUALS";
        List<Condition> conditions = new ArrayList<>();
        int loops = 0;
        for (String tag : tags) {
            Condition result = Assertions
                    .assertDoesNotThrow(() -> new StreamingCondition(tag, value, operation).condition());
            Assertions.assertNotEquals(DSL.noCondition(), result);
            conditions.add(result);
            loops++;
        }
        Assertions.assertEquals(3, loops);
        List<Condition> expectedResults = new ArrayList<>();
        expectedResults.add(new IndexCondition(value, operation, true).condition());
        expectedResults.add(new SourceTypeCondition(value, operation, true).condition());
        expectedResults.add(new HostCondition(value, operation, true).condition());
        Assertions.assertEquals(expectedResults, conditions);
    }

    @Test
    public void testStreamingQueryPassThroughTags() {
        String[] tags = {
                "earliest", "latest", "index_earliest", "index_latest", "indexstatement"
        };
        String value = "123456";
        String operation = "EQUALS";
        int loops = 0;
        for (String tag : tags) {
            Condition result = Assertions
                    .assertDoesNotThrow(() -> new StreamingCondition(tag, value, operation).condition());
            Assertions.assertEquals(DSL.noCondition(), result);
            loops++;
        }
        Assertions.assertEquals(5, loops);
    }

    @Test
    public void testUnsupportedElementName() {
        String tag = "unsupported_tag";
        String value = "123456";
        String operation = "EQUALS";
        IllegalArgumentException ex = Assertions
                .assertThrows(IllegalArgumentException.class, () -> new StreamingCondition(tag, value, operation).condition());
        String expectedMessage = "Unsupported element tag <unsupported_tag>";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testEquality() {
        String tag = "index";
        String value = "value";
        String operation = "EQUALS";
        StreamingCondition cond1 = new StreamingCondition(tag, value, operation);
        StreamingCondition cond2 = new StreamingCondition(tag, value, operation);
        Assertions.assertEquals(cond1, cond2);
    }

    @Test
    public void testNonEquality() {
        String tag = "index";
        String value = "value";
        String operation = "EQUALS";
        StreamingCondition base = new StreamingCondition(tag, value, operation);
        StreamingCondition cond1 = new StreamingCondition("indexstatement", value, operation);
        StreamingCondition cond2 = new StreamingCondition(tag, "not_value", operation);
        StreamingCondition cond3 = new StreamingCondition(tag, value, "NOT_EQUALS");
        Assertions.assertNotEquals(base, cond1);
        Assertions.assertNotEquals(base, cond2);
        Assertions.assertNotEquals(base, cond3);
    }

    @Test
    public void testHashCode() {
        String tag = "index";
        String value = "value";
        String operation = "EQUALS";
        StreamingCondition base = new StreamingCondition(tag, value, operation);
        StreamingCondition base2 = new StreamingCondition(tag, value, operation);
        StreamingCondition cond1 = new StreamingCondition("indexstatement", value, operation);
        StreamingCondition cond2 = new StreamingCondition(tag, "not_value", operation);
        StreamingCondition cond3 = new StreamingCondition(tag, value, "NOT_EQUALS");
        Assertions.assertEquals(base.hashCode(), base2.hashCode());
        Assertions.assertNotEquals(base.hashCode(), cond1.hashCode());
        Assertions.assertNotEquals(base.hashCode(), cond1.hashCode());
        Assertions.assertNotEquals(base.hashCode(), cond2.hashCode());
        Assertions.assertNotEquals(base.hashCode(), cond3.hashCode());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(StreamingCondition.class).withNonnullFields(new String[] {
                "tag", "value", "operation"
        }).verify();
    }

}
