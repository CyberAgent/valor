grammar Valor;

tokens{
TOK_EXPLAIN,
TOK_QUERY,
TOK_SELECT,
TOK_CREATE_RELATION,
TOK_DROP_RELATION,
TOK_RELATION_ATTRS,
TOK_RELATION_ATTR,
TOK_ATTR_TYPE,
TOK_ARRAY_ATTR_TYPE,
TOK_MAP_ATTR_TYPE,
TOK_RELATION_OPTS,
TOK_RELATION_OPT,
TOK_KEY_LIST,
TOK_CREATE_SCHEMA,
TOK_DROP_SCHEMA,
TOK_OVERWRITE,
TOK_SCHEMA_DEF,
TOK_PRIMARY_SCHEMA,
TOK_SCHEMA_MODE,
TOK_FORMAT_DEF,
TOK_ELEMENT,
TOK_ELEMENT_DEF,
TOK_ATTR_ELEMENT_DEF,
TOK_CONST_ELEMENT_DEF,
TOK_DECORATOR_DEF,
TOK_SCHEMA_OPTION,
TOK_ALTER_SET_SCHEMA_MODE,
TOK_ALTER_RELATION,
TOK_REPAIR_RELATION,
TOK_USING,
TOK_DESCRIBE,
TOK_SHOW_RELATIONS,
TOK_SHOW_SCHEMAS,
TOK_INSERT,
TOK_UPDATE,
TOK_DELETE,
TOK_FROM,
TOK_INTO,
TOK_ATTR_NAMES,
TOK_VALUES,
TOK_ATTRS,
TOK_ATTR,
TOK_ATTR_NAME,
TOK_ATTR_VALUE,
TOK_SELECT_ITEMS,
TOK_SELECT_ITEM,
TOK_WHERE,
TOK_LIMIT,
TOK_OFFSET,
TOK_NULL,
TOK_ISNULL,
TOK_ISNOTNULL,
TOK_BETWEEN,
TOK_IN,
TOK_WITH_UNKNOWN,
TOK_WITHOUT_UNKNOWN,
TOK_FUNCTION,
TOK_FUNCTION_ARGUMENT,
TOK_COMPLEX_CONST,
TOK_TRUE,
TOK_FALSE,
TOK_BOOLEAN
}

@header {
package jp.co.cyberagent.valor.ql.grammer.gen;
}

// starting rule
statement:
  execStatement EOF
  ;

execStatement:
  showRelationsStatement
  | showSchemasStatement
  | describeStatement
  | insertStatementExpression
  | updateStatementExpression
  | deleteStatementExpression
  | selectStatementExpression
  ;

showRelationsStatement:
  KW_SHOW KW_RELATIONS
  ;

showSchemasStatement:
   KW_SHOW KW_SCHEMAS KW_FOR Identifier
   ;

describeStatement:
   KW_DESCRIBE relationId=Identifier (DOT schemaId=Identifier)?
   ;

insertStatementExpression:
  KW_INSERT KW_INTO relationSource (KW_VALUES LPAREN attrNames RPAREN)? valuesClause
  ;

relationSource:
  relationId=Identifier (DOT schemaId=Identifier)?
;

attrNames:
   Identifier (COMMA Identifier)*
   ;

valuesClause:
  KW_VALUES valueItem+
  ;

valueItem:
  LPAREN constant (COMMA constant)* RPAREN
  ;

attributesValueClause:
  attribute (COMMA attribute)*
  ;

// TODO use attrName instead of Identifier
attribute:
  attrName=attributeName EQUAL attrValue=expression
  ;

updateStatementExpression:
  KW_UPDATE relationId=Identifier (DOT schemaId=Identifier)? KW_SET attributesValueClause whereClause
  ;

deleteStatementExpression:
  KW_DELETE fromClause whereClause limitClause?
  ;

selectStatementExpression :
  KW_SELECT selectItems fromClause whereClause? limitClause? offsetClause?
  ;

selectItems:
  selectItem (COMMA selectItem)*
  ;

selectItem:
  expression
  | STAR
  ;

fromClause:
  KW_FROM relationSource
  ;

whereClause:
  KW_WHERE searchCondition
  ;

searchCondition:
  expression ;

attributeName:
  BACKQUOTE? Identifier BACKQUOTE?
  | BACKQUOTE? keywords BACKQUOTE?
  ;

limitClause:
  KW_LIMIT num=IntegerLiteral
  ;

offsetClause:
  KW_OFFSET num=IntegerLiteral
  ;

atomExpression :
    constant
    | attributeName
    | functionExpression
    | LPAREN expression RPAREN
    ;

expression:
  orExpression
  ;

orOperator:
  KW_OR  ;

orExpression:
  andExpression (orOperator andExpression)*;

andOperator:
  KW_AND  ;

andExpression:
  notExpression (andOperator notExpression)*;

notOperator:
  KW_NOT;

notExpression :
 notOperator? predicate
 ;

compareOperator :
    EQUAL
    |NOTEQUAL
    |LIKE
    |REGEXP
    |GREATERTHAN
    |GREATERTHANOREQUAL
    |LESSTHAN
    |LESSTHANOREQUAL
    ;

predicate :
 atomExpression
 | binaryPredicateExpression
 | isNullExpression
 | betweenExpression
 | inExpression
;

binaryPredicateExpression:
  left=plusExpression compareOperator right=plusExpression
  ;

isNullExpression:
 left=plusExpression KW_IS KW_NOT? KW_NULL
 ;

betweenExpression:
 left=plusExpression KW_BETWEEN (min = plusExpression) KW_AND (max=plusExpression)
 ;

inExpression:
 left=plusExpression KW_IN LPAREN candidates += plusExpression (COMMA candidates+=plusExpression)* RPAREN
 ;

plusOperator:
    PLUS | MINUS
    ;

plusExpression:
    multiplyExpression (plusOperator multiplyExpression)*
    ;

multiplyOperator:
    STAR | DIV
    ;

multiplyExpression:
    collectionElementExpression (multiplyOperator collectionElementExpression)*
    ;

collectionElementExpression:
   collection=atomExpression (LSQUARE index=atomExpression RSQUARE)?
   ;

functionExpression:
   functionName=Identifier LPAREN (expression (COMMA expression)*)? RPAREN
   ;

constant :
    LongLiteral
    | IntegerLiteral
    | BytesLiteral
    | DoubleLiteral
    | FloatLiteral
    | StringLiteral
    | BooleanLiteral
    | arrayLiteral
    | mapLiteral
    | setLiteral
    | QUESTION
    | KW_NULL
    ;

arrayLiteral:
  KW_ARRAY LPAREN ((constant (COMMA constant)*))? RPAREN
  ;

setLiteral:
  KW_SET LPAREN ((constant (COMMA constant)*))? RPAREN
  ;

mapLiteral:
  KW_MAP LPAREN (constant COMMA constant (COMMA constant COMMA constant)*)? RPAREN
  ;

keywords:
KW_EXPLAIN
| KW_UNKNOWN
 |KW_RELATION
 |KW_TABLE
 |KW_INCLUDE
 |KW_EXCLUDE
 |KW_FOR
 |KW_ROW
 |KW_FAMILY
 |KW_QUALIFIER
 |KW_VALUE
 |KW_SELECT
 |KW_CREATE
 |KW_EXISTS
 |KW_EVEN
 |KW_ALTER
 |KW_REPAIR
 |KW_USING
 |KW_MODE
 |KW_DROP
 |KW_INSERT
 |KW_UPDATE
 |KW_DELETE
 |KW_DESCRIBE
 |KW_SHOW
 |KW_RELATIONS
 |KW_PRIMARY
 |KW_SCHEMA
 |KW_SCHEMAS
 |KW_VALUES
 |KW_INTO
 |KW_FROM
 |KW_SET
 |KW_WHERE
 |KW_LIMIT
 |KW_OFFSET
 |KW_MAP
 |KW_ARRAY
;

KW_EXPLAIN: 'EXPLAIN';
KW_UNKNOWN: 'UNKNOWN';
KW_RELATION: 'RELATION';
KW_TABLE: 'TABLE';
KW_INCLUDE: 'INCLUDE';
KW_EXCLUDE: 'EXCLUDE';
KW_FOR: 'FOR';
KW_ROW: 'ROW';
KW_FAMILY: 'FAMILY';
KW_QUALIFIER: 'QUALIFIER';
KW_VALUE: 'VALUE';
KW_SELECT: 'SELECT';
KW_CREATE: 'CREATE';
KW_IF: 'IF';
KW_NOT: 'NOT';
KW_EXISTS: 'EXISTS';
KW_EVEN: 'EVEN';
KW_ALTER: 'ALTER';
KW_REPAIR: 'REPAIR';
KW_USING: 'USING';
KW_MODE: 'MODE';
KW_DROP: 'DROP';
KW_INSERT: 'INSERT';
KW_UPDATE: 'UPDATE';
KW_DELETE: 'DELETE';
KW_DESCRIBE: 'DESCRIBE';
KW_SHOW: 'SHOW';
KW_RELATIONS: 'RELATIONS';
KW_PRIMARY: 'PRIMARY';
KW_SCHEMA: 'SCHEMA';
KW_SCHEMAS: 'SCHEMAS';
KW_VALUES: 'VALUES';
KW_INTO: 'INTO';
KW_FROM: 'FROM';
KW_SET: 'SET';
KW_WHERE: 'WHERE';
KW_LIMIT: 'LIMIT';
KW_OFFSET: 'OFFSET';
KW_AND: 'AND';
KW_OR: 'OR';
KW_IS: 'IS';
KW_NULL: 'NULL';
KW_BETWEEN: 'BETWEEN';
KW_IN: 'IN';
KW_MAP: 'MAP';
KW_ARRAY: 'ARRAY';

// operators
DOT : '.'; // generated as a part of Number rule
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;
X :'X';
QUESTION : '?';
BACKQUOTE: '`';

LPAREN : '(' ;
RPAREN : ')' ;
LSQUARE : '[' ;
RSQUARE : ']' ;
LCURLY : '{';
RCURLY : '}';

EQUAL : '='|'==';
NOTEQUAL : '!=';
LIKE : 'LIKE';
REGEXP : 'REGEXP';
GREATERTHAN : '>';
GREATERTHANOREQUAL : '>=';
LESSTHAN : '<';
LESSTHANOREQUAL : '<=';
PLUS: '+';
MINUS: '-';
STAR : '*';
DIV: '/';

// LITERALS
fragment
Letter:
  NameStartChar NameChar*
 ;

fragment
NameChar
   : NameStartChar
   | '0'..'9'
   | '_'
   | '.'
   | '\u00B7'
   | '\u0300'..'\u036F'
   | '\u203F'..'\u2040'
   ;
fragment
NameStartChar
   : 'A'..'Z' | 'a'..'z'
   | '\u00C0'..'\u00D6'
   | '\u00D8'..'\u00F6'
   | '\u00F8'..'\u02FF'
   | '\u0370'..'\u037D'
   | '\u037F'..'\u1FFF'
   | '\u200C'..'\u200D'
   | '\u2070'..'\u218F'
   | '\u2C00'..'\u2FEF'
   | '\u3001'..'\uD7FF'
   | '\uF900'..'\uFDCF'
   | '\uFDF0'..'\uFFFD'
   ;

fragment
Digit
    :
    '0'..'9'
    ;

fragment
IntegerNumber
    :   '0'
    |   '1'..'9' ('0'..'9')*    ;

fragment
LongSuffix:
    'l'|'L';

fragment
FloatSuffix:
    'f'|'F';

fragment
ByteNumber
    : '0' X(('0'..'9'|'a'..'z' | 'A'..'Z') ('0'..'9'|'a'..'z' | 'A'..'Z'))+;

fragment
EscapeSequence
    : '\\' [btnfr"'\\]
;

StringLiteral
    :
     '"' (~["\\\r\n] | EscapeSequence)* '"'
    | '\'' (~['\\\r\n] | EscapeSequence)* '\''
    ;

LongLiteral
    :
    (MINUS)? IntegerNumber LongSuffix
    ;


IntegerLiteral
    :
    (MINUS)? IntegerNumber
    ;

DoubleLiteral
    :
    (MINUS)? IntegerNumber DOT IntegerNumber+
    ;

FloatLiteral
    :
    (MINUS)? IntegerNumber DOT IntegerNumber+ FloatSuffix
    ;

BytesLiteral
    :
    ByteNumber
    ;

Identifier
    :
    (Letter | Digit) (Letter | Digit | '_' | '-' )*
    ;

WS  :  (' '|'\t'|'\r'|'\n') -> skip
  ;

COMMENT
    :   '/*' .*? '*/' -> skip
    ;

LINE_COMMENT
    : '//' ~('\n'|'\r')* '\r'? '\n' -> skip
    ;

