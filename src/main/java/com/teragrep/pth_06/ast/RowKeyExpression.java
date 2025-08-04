package com.teragrep.pth_06.ast;

import java.util.List;

public interface RowKeyExpression {

    public abstract byte[] rowKeyBytes();
}
