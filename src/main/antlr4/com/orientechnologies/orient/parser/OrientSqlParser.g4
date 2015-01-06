parser grammar OrientSqlParser;

options { tokenVocab=OrientSqlLexer; }

/**
parse
 : statementList* EOF
 ;

statementList
 : statement ( SEMICOLON+ statement )* SEMICOLON*
 ;
 */
parse
 : statement
 ;

statement
 : selectStatement
 ;

selectStatement
 : SELECT (STAR | projection)? FROM fromClause whereClause? orderBy? skip? limit? lock? timeout? parallel? fetchplan?
 ;

projection
 : projectionItem ( COMMA projectionItem )*
 ;

projectionItem
 : name projectionItemAlias?
 ;

projectionItemAlias
 : AS name
 ;

fromClause
 : name
 ;

// TODO: continue here
whereClause
 : name
 ;

orderBy
 : ORDER BY orderByItem ( COMMA orderByItem )*
 ;

orderByItem
 : name ordering
 ;

ordering
 : ASC
 | DESC
 ;

groupBy
 : GROUP BY name ( COMMA name )*
 ;

skip
 : SKIP INTEGER_LITERAL
 ;

limit
 : LIMIT INTEGER_LITERAL
 ;

lock
 : LOCK lockStrategy
 ;

lockStrategy
 : DEFAULT
 | NONE
 | RECORD
 ;

timeout
 : TIMEOUT INTEGER_LITERAL
 ;

parallel
 : PARALLEL
 ;

fetchplan
 : FETCHPLAN STRING_LITERAL
 ;

keyword
 : ADD
 | ALTER
 | AND
 | AS
 | ASC
 | BETWEEN
 | BY
 | CLUSTER
 | CREATE
 | DELETE
 | DESC
 | DISTINCT
 | DROP
 | EDGE
 | FETCHPLAN
 | FROM
 | GROUP
 | IN
 | INSERT
 | INTO
 | IS
 | ISNULL
 | LET
 | LIKE
 | LIMIT
 | NOT
 | NULL
 | OF
 | OFFSET
 | OR
 | ORDER
 | SELECT
 | SET
 | SKIP
 | TO
 | UNION
 | UNIONALL
 | UNIQUE
 | UPDATE
 | VALUES
 | VERTEX
 | WHERE
 ;

recordAttribute
 : CLASS
 | RID
 | SIZE
 | THIS
 | TYPE
 | VERSION
 ;

name
 : vertexName
 | edgeName
 | anyName
 ;

vertexName
 : VERTEX
 ;

edgeName
 : EDGE
 ;

anyName
 : IDENTIFIER
 | keyword
 | STRING_LITERAL
 | LPAREN anyName RPAREN
 ;
