package com.orientechnologies.orient.parser;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyOrientSqlParserListener extends OrientSqlParserBaseListener {
    private static final Logger logger = LoggerFactory.getLogger(DummyOrientSqlParserListener.class);

    @Override
    public void visitErrorNode(@NotNull ErrorNode node) {
        throw new RuntimeException("!!!!! " + node.toString() + " - " + node.getText());
    }
}
