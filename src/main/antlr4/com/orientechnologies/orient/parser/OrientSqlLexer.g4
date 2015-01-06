lexer grammar OrientSqlLexer;

AT : '@';
SEMICOLON : ';';
DOT : '.';
LPAREN : '(';
RPAREN : ')';
COMMA : ',';
STAR : '*';
PLUS : '+';
MINUS : '-';
TILDE : '~';
PIPE2 : '||';
DIV : '/';
MOD : '%';
LT2 : '<<';
GT2 : '>>';
AMP : '&';
PIPE : '|';
LT : '<';
LE : '<=';
GT : '>';
GE : '>=';
EQ : '=';
NEQ1 : '!=';
NEQ2 : '<>';

ADD : A D D;
ALL : A L L LPAREN RPAREN;
ALTER : A L T E R;
AND : A N D;
ANY : A N Y LPAREN RPAREN;
AS : A S;
ASC : A S C;
BETWEEN : B E T W E E N;
BY : B Y;
CLUSTER : C L U S T E R;
CREATE : C R E A T E;
DEFAULT : D E F A U L T;
DELETE : D E L E T E;
DESC : D E S C;
DISTINCT : D I S T I N C T;
DROP : D R O P;
EDGE : E D G E;
FETCHPLAN : F E T C H P L A N;
FROM : F R O M;
GROUP : G R O U P;
IN : I N;
INSERT : I N S E R T;
INTO : I N T O;
IS : I S;
ISNULL : I S N U L L;
LET : L E T;
LIKE : L I K E;
LIMIT : L I M I T;
LOCK : L O C K;
NONE : N O N E;
NOT : N O T;
NULL : N U L L;
OF : O F;
OFFSET : O F F S E T;
OR : O R;
ORDER : O R D E R;
PARALLEL : P A R A L L E L;
RECORD : R E C O R D;
RID : AT R I D;
CLASS : AT C L A S S;
SELECT : S E L E C T;
SET : S E T;
SIZE : AT S I Z E;
SKIP : S K I P;
THIS : AT T H I S;
TIMEOUT : T I M E O U T;
TO : T O;
TYPE : AT T Y P E;
UNION : U N I O N;
UNIONALL : U N I O N A L L;
UNIQUE : U N I Q U E;
UPDATE : U P D A T E;
VALUES : V A L U E S;
VERSION : AT V E R S I O N;
VERTEX : V E R T E X;
WHERE : W H E R E;

IDENTIFIER
 : '"' (~'"' | '""')* '"'
 | [a-zA-Z_] [a-zA-Z_0-9]* // TODO check: needs more chars in set
 ;

NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
 ;

INTEGER_LITERAL
 : DIGIT+
 ;

BIND_PARAMETER
 : '?' DIGIT*
 | [:@$] IDENTIFIER
 ;

STRING_LITERAL
 : '\'' ( ~'\'' | '\'\'' )* '\''
 ;

BLOB_LITERAL
 : X STRING_LITERAL
 ;

SINGLE_LINE_COMMENT
 : '--' ~[\r\n]* -> channel(HIDDEN)
 ;

MULTILINE_COMMENT
 : '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN)
 ;

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR
 : .
 ;

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];

