package jp.co.cyberagent.valor.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

public class ValorJdbcMetaData implements DatabaseMetaData {
  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("allProceduresAreCallable() is not implemented");
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("allTablesAreSelectable() is not implemented");
  }

  @Override
  public String getURL() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getURL() is not implemented");
  }

  @Override
  public String getUserName() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getUserName() is not implemented");
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("isReadOnly() is not implemented");
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("nullsAreSortedHigh() is not implemented");
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("nullsAreSortedLow() is not implemented");
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("nullsAreSortedAtStart() is not implemented");
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("nullsAreSortedAtEnd() is not implemented");
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getDatabaseProductName() is not implemented");
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getDatabaseProductVersion() is not implemented");
  }

  @Override
  public String getDriverName() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getDriverName() is not implemented");
  }

  @Override
  public String getDriverVersion() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getDriverVersion() is not implemented");
  }

  @Override
  public int getDriverMajorVersion() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getDriverMajorVersion() is not implemented");
  }

  @Override
  public int getDriverMinorVersion() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getDriverMinorVersion() is not implemented");
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("usesLocalFiles() is not implemented");
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("usesLocalFilePerTable() is not implemented");
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsMixedCaseIdentifiers() is not implemented");
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("storesUpperCaseIdentifiers() is not implemented");
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("storesLowerCaseIdentifiers() is not implemented");
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("storesMixedCaseIdentifiers() is not implemented");
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsMixedCaseQuotedIdentifiers() is not implemented");
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "storesUpperCaseQuotedIdentifiers() is not implemented");
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "storesLowerCaseQuotedIdentifiers() is not implemented");
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "storesMixedCaseQuotedIdentifiers() is not implemented");
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getIdentifierQuoteString() is not implemented");
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getSQLKeywords() is not implemented");
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getNumericFunctions() is not implemented");
  }

  @Override
  public String getStringFunctions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getStringFunctions() is not implemented");
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getSystemFunctions() is not implemented");
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getTimeDateFunctions() is not implemented");
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getSearchStringEscape() is not implemented");
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getExtraNameCharacters() is not implemented");
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsAlterTableWithAddColumn() is not implemented");
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsAlterTableWithDropColumn() is not implemented");
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsColumnAliasing() is not implemented");
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("nullPlusNonNullIsNull() is not implemented");
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsConvert() is not implemented");
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsConvert() is not implemented");
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsTableCorrelationNames() is not implemented");
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsDifferentTableCorrelationNames() is not implemented");
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsExpressionsInOrderBy() is not implemented");
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsOrderByUnrelated() is not implemented");
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsGroupBy() is not implemented");
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsGroupByUnrelated() is not implemented");
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsGroupByBeyondSelect() is not implemented");
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsLikeEscapeClause() is not implemented");
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsMultipleResultSets() is not implemented");
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsMultipleTransactions() is not implemented");
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsNonNullableColumns() is not implemented");
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsMinimumSQLGrammar() is not implemented");
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsCoreSQLGrammar() is not implemented");
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsExtendedSQLGrammar() is not implemented");
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsANSI92EntryLevelSQL() is not implemented");
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsANSI92IntermediateSQL() is not implemented");
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsANSI92FullSQL() is not implemented");
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsIntegrityEnhancementFacility() is not implemented");
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsOuterJoins() is not implemented");
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsFullOuterJoins() is not implemented");
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsLimitedOuterJoins() is not implemented");
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getSchemaTerm() is not implemented");
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getProcedureTerm() is not implemented");
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getCatalogTerm() is not implemented");
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("isCatalogAtStart() is not implemented");
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getCatalogSeparator() is not implemented");
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsSchemasInDataManipulation() is not implemented");
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsSchemasInProcedureCalls() is not implemented");
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsSchemasInTableDefinitions() is not implemented");
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsSchemasInIndexDefinitions() is not implemented");
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsSchemasInPrivilegeDefinitions() is not implemented");
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsCatalogsInDataManipulation() is not implemented");
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsCatalogsInProcedureCalls() is not implemented");
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsCatalogsInTableDefinitions() is not implemented");
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsCatalogsInIndexDefinitions() is not implemented");
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsCatalogsInPrivilegeDefinitions() is not implemented");
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsPositionedDelete() is not implemented");
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsPositionedUpdate() is not implemented");
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsSelectForUpdate() is not implemented");
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsStoredProcedures() is not implemented");
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsSubqueriesInComparisons() is not implemented");
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsSubqueriesInExists() is not implemented");
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsSubqueriesInIns() is not implemented");
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsSubqueriesInQuantifieds() is not implemented");
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsCorrelatedSubqueries() is not implemented");
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsUnion() is not implemented");
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsUnionAll() is not implemented");
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsOpenCursorsAcrossCommit() is not implemented");
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsOpenCursorsAcrossRollback() is not implemented");
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsOpenStatementsAcrossCommit() is not implemented");
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsOpenStatementsAcrossRollback() is not implemented");
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxBinaryLiteralLength() is not implemented");
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxCharLiteralLength() is not implemented");
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxColumnNameLength() is not implemented");
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxColumnsInGroupBy() is not implemented");
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxColumnsInIndex() is not implemented");
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxColumnsInOrderBy() is not implemented");
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxColumnsInSelect() is not implemented");
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxColumnsInTable() is not implemented");
  }

  @Override
  public int getMaxConnections() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxConnections() is not implemented");
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxCursorNameLength() is not implemented");
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxIndexLength() is not implemented");
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxSchemaNameLength() is not implemented");
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxProcedureNameLength() is not implemented");
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxCatalogNameLength() is not implemented");
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxRowSize() is not implemented");
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("doesMaxRowSizeIncludeBlobs() is not implemented");
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxStatementLength() is not implemented");
  }

  @Override
  public int getMaxStatements() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxStatements() is not implemented");
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxTableNameLength() is not implemented");
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxTablesInSelect() is not implemented");
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getMaxUserNameLength() is not implemented");
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getDefaultTransactionIsolation() is not implemented");
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsTransactions() is not implemented");
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsTransactionIsolationLevel() is not implemented");
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsDataDefinitionAndDataManipulationTransactions() is not implemented");
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsDataManipulationTransactionsOnly() is not implemented");
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "dataDefinitionCausesTransactionCommit() is not implemented");
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "dataDefinitionIgnoredInTransactions() is not implemented");
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getProcedures() is not implemented");
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                       String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getProcedureColumns() is not implemented");
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                             String[] types) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getTables() is not implemented");
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getSchemas() is not implemented");
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getSchemas() is not implemented");
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getCatalogs() is not implemented");
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getTableTypes() is not implemented");
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                              String columnNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getColumns() is not implemented");
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                       String columnNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getColumnPrivileges() is not implemented");
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getTablePrivileges() is not implemented");
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
                                        boolean nullable) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getBestRowIdentifier() is not implemented");
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getVersionColumns() is not implemented");
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getPrimaryKeys() is not implemented");
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getImportedKeys() is not implemented");
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getExportedKeys() is not implemented");
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                     String foreignCatalog, String foreignSchema,
                                     String foreignTable) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getCrossReference() is not implemented");
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getTypeInfo() is not implemented");
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
                                boolean approximate) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getIndexInfo() is not implemented");
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsResultSetType() is not implemented");
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsResultSetConcurrency() is not implemented");
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("ownUpdatesAreVisible() is not implemented");
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("ownDeletesAreVisible() is not implemented");
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("ownInsertsAreVisible() is not implemented");
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("othersUpdatesAreVisible() is not implemented");
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("othersDeletesAreVisible() is not implemented");
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("othersInsertsAreVisible() is not implemented");
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updatesAreDetected() is not implemented");
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("deletesAreDetected() is not implemented");
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("insertsAreDetected() is not implemented");
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsBatchUpdates() is not implemented");
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
                           int[] types) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getUDTs() is not implemented");
  }

  @Override
  public Connection getConnection() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getConnection() is not implemented");
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsSavepoints() is not implemented");
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsNamedParameters() is not implemented");
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsMultipleOpenResults() is not implemented");
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsGetGeneratedKeys() is not implemented");
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getSuperTypes() is not implemented");
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getSuperTables() is not implemented");
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                 String attributeNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getAttributes() is not implemented");
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsResultSetHoldability() is not implemented");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getResultSetHoldability() is not implemented");
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getDatabaseMajorVersion() is not implemented");
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getDatabaseMinorVersion() is not implemented");
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getJDBCMajorVersion() is not implemented");
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getJDBCMinorVersion() is not implemented");
  }

  @Override
  public int getSQLStateType() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getSQLStateType() is not implemented");
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("locatorsUpdateCopy() is not implemented");
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("supportsStatementPooling() is not implemented");
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getRowIdLifetime() is not implemented");
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "supportsStoredFunctionsUsingCallSyntax() is not implemented");
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
        "autoCommitFailureClosesAllResultSets() is not implemented");
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getClientInfoProperties() is not implemented");
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getFunctions() is not implemented");
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                      String functionNamePattern, String columnNamePattern)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getFunctionColumns() is not implemented");
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                    String columnNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getPseudoColumns() is not implemented");
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("generatedKeyAlwaysReturned() is not implemented");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("unwrap() is not implemented");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("isWrapperFor() is not implemented");
  }
}
