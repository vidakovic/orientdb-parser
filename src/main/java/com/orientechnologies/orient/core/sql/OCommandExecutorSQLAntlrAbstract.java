package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.parser.OrientSqlLexer;
import com.orientechnologies.orient.parser.OrientSqlParser;
import com.orientechnologies.orient.parser.OrientSqlParserBaseListener;
import com.orientechnologies.orient.parser.OrientSqlParserListener;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ErrorNode;

// NOTE: this class only adds minimal ANTLR functionality and disables all manual parser methods (just to demonstrate
//       that they are not used/needed in the child class)
public abstract class OCommandExecutorSQLAntlrAbstract extends OCommandExecutorSQLResultsetAbstract {
    protected CharStream stream;
    protected OrientSqlParser parser;

    @Override
    public OCommandExecutorSQLResultsetAbstract parse(OCommandRequest iRequest) {
        super.parse(iRequest);

        stream = new ANTLRInputStream(parserText);
        parser = new OrientSqlParser(new CommonTokenStream(new OrientSqlLexer(stream)));
        addParseListener(new CommonOrientSqlParserListener());

        return this;
    }

    protected void addParseListener(OrientSqlParserListener parserListener) {
        parser.addParseListener(parserListener);
    }

    protected class CommonOrientSqlParserListener extends OrientSqlParserBaseListener {
        // TODO: let

        @Override
        public void exitLimit(@NotNull OrientSqlParser.LimitContext ctx) {
            limit = Integer.valueOf(ctx.INTEGER_LITERAL().getText());

            if (limit == 0 || limit < -1) {
                throw new IllegalArgumentException("Limit must be > 0 or = -1 (no limit)");
            }
        }

        @Override
        public void exitSkip(@NotNull OrientSqlParser.SkipContext ctx) {
            skip = Integer.valueOf(ctx.INTEGER_LITERAL().getText());
        }

        @Override
        public void exitTimeout(@NotNull OrientSqlParser.TimeoutContext ctx) {
            System.out.println(ctx.getText());
            //timeoutMs = Long.valueOf(ctx.);
        }

        @Override
        public void visitErrorNode(@NotNull ErrorNode node) {
            throwParsingException("Invalid keyword '" + node.getText() + "'");
        }
    }

    protected void throwParsingException(final String iText) {
        // TODO: exception text could be improved
        throw new OCommandSQLParsingException(iText, parserText, parser.getCurrentToken().getCharPositionInLine());
    }

    @Override
    @Deprecated
    protected void parseLet() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected int parseLimit(String w) throws OCommandSQLParsingException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected int parseSkip(String w) throws OCommandSQLParsingException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected String parseLock() throws OCommandSQLParsingException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected boolean parseTimeout(String w) throws OCommandSQLParsingException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected String parseOptionalWord(boolean iUpperCase, String... iWords) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public char parserGetCurrentChar() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public int parserGetCurrentPosition() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public char parserGetLastSeparator() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public String parserGetLastWord() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public int parserGetPreviousPosition() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected int parserGoBack() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public boolean parserIsEnded() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected boolean parserMoveCurrentPosition(int iOffset) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected int parserNextChars(boolean iUpperCase, boolean iMandatory, String... iCandidateWords) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected void parserNextWord(boolean iForceUpperCase) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected void parserNextWord(boolean iForceUpperCase, String iSeparatorChars) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected boolean parserOptionalKeyword(String... iWords) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected String parserOptionalWord(boolean iUpperCase) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected void parserRequiredKeyword(String... iWords) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected String parserRequiredWord(boolean iUpperCase) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected String parserRequiredWord(boolean iUpperCase, String iCustomMessage) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected String parserRequiredWord(boolean iUpperCase, String iCustomMessage, String iSeparators) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected boolean parserSetCurrentPosition(int iPosition) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected void parserSetEndOfText() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void parserSetLastSeparator(char iSeparator) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected boolean parserSkipWhiteSpaces() {
        throw new UnsupportedOperationException();
    }
}
