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
package com.teragrep.pth_06.ast.analyze;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.ScanGroupExpression;
import com.teragrep.pth_06.ast.ScanRange;
import com.teragrep.pth_06.ast.transform.TransformToScanGroups;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;

import java.util.ArrayList;
import java.util.List;

public final class ScanRanges {

    private final Expression root;
    private final List<ScanRange> scanRanges;

    public ScanRanges(TransformToScanGroups transformToScanGroups) {
        this(transformToScanGroups.transformed());
    }

    public ScanRanges(final Expression root) {
        this.root = root;
        this.scanRanges = new ArrayList<>();
    }

    ScanRanges(final Expression root, final List<ScanRange> scanRanges) {
        this.root = root;
        this.scanRanges = scanRanges;
    }

    public List<ScanRange> rangeList() {
        if (scanRanges.isEmpty()) {
            findScanRanges(root);
        }
        return scanRanges;
    }

    private Expression findScanRanges(final Expression expression) {
        final Expression.Tag tag = expression.tag();
        final Expression result;
        if (expression.isLogical()) {
            final List<Expression> children = expression.asLogical().children();
            final List<Expression> traversedChildren = new ArrayList<>();
            for (final Expression child : children) {
                final Expression traversedOr = findScanRanges(child);
                traversedChildren.add(traversedOr);
            }
            if (tag.equals(Expression.Tag.AND)) {
                result = new AndExpression(traversedChildren);
            }
            else {
                result = new OrExpression(traversedChildren);
            }
        }
        else {
            ScanGroupExpression scanGroupExpression = (ScanGroupExpression) expression.asLeaf();
            scanRanges.addAll(scanGroupExpression.value());
            result = expression;
        }

        return result;
    }
}
