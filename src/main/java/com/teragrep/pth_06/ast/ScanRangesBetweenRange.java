package com.teragrep.pth_06.ast;

import java.util.ArrayList;
import java.util.List;

public final class ScanRangesBetweenRange {
    private final List<ScanRange> originalRanges;
    private final Long startOffset;
    private final Long stopOffset;

    public ScanRangesBetweenRange(final List<ScanRange> originalRanges, final Long startOffset, final Long stopOffset) {
        this.originalRanges = originalRanges;
        this.startOffset = startOffset;
        this.stopOffset = stopOffset;
    }

    public List<ScanRange> rangesBetweenRange() {
        final List<ScanRange> updatedRanges = new ArrayList<>(originalRanges.size());
        for (final ScanRange range : originalRanges) {
            final Long rangeEarliest = range.earliest();
            final Long rangeLatest = range.earliest();
            if (rangeLatest >= startOffset && rangeEarliest <= stopOffset) {
                final Long newEarliest = Math.max(rangeEarliest, startOffset);
                final Long newLatest = Math.min(rangeLatest, stopOffset);
                if (newEarliest <= newLatest) {
                    updatedRanges.add(new ScanRangeImpl(range.streamId(), newEarliest, newLatest, range.filterList()));
                }
            }
        }
        return updatedRanges;
    }
}
