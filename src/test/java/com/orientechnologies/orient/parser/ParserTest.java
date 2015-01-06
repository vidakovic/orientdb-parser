package com.orientechnologies.orient.parser;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelectAntlr;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.InputStream;
import java.util.Collections;
import java.util.logging.LogManager;

import static org.junit.Assert.*;

public class ParserTest {
    private static final Logger logger = LoggerFactory.getLogger(ParserTest.class);

    static {
        // Note: trick for slf4j to manage log output of OrientLogManager (which uses JUL)
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
    }

    private ODatabaseDocumentTx db;

    @Before
    public void setUp() {
        String url = "memory:test";
        db = new ODatabaseDocumentTx(url);
        if(!db.exists()) {
            db.create();
        }
    }

    /**
    @Test
    public void testParseMultiStatements() throws Exception {
        parse(ParserTest.class.getClassLoader().getResourceAsStream("select.sql"));
    }
     */

    @Test
    public void testSelectLimit() throws Exception {
        parse("select from OUser limit 1");
    }

    private void parse(InputStream is) throws Exception {
        parse(new ANTLRInputStream(is));
    }

    private void parse(String s) throws Exception {
        parse(new ANTLRInputStream(s));
    }

    private void parse(CharStream stream) throws Exception {
        OrientSqlParser parser = new OrientSqlParser(new CommonTokenStream(new OrientSqlLexer(stream)));
        parser.addParseListener(new DummyOrientSqlParserListener());
        parser.parse();
    }

    @Test
    public void testCommand() throws Exception {
        OCommandSQL sql = new OCommandSQL("select from OUser order by name DESC");
        OCommandExecutorSQLSelectAntlr command = new OCommandExecutorSQLSelectAntlr();

        Object result = command.parse(sql).execute(Collections.emptyMap());

        assertEquals(OResultSet.class, result.getClass());

        OResultSet<ODocument> resultSet = (OResultSet)result;

        assertEquals("writer", resultSet.get(0).field("name"));

        for(ODocument doc : resultSet) {
            assertEquals("OUser", doc.getClassName());
            assertNotNull(doc.field("name"));
            logger.info("User: {}", doc.field("name"));
        }
    }
}
