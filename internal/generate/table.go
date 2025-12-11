package generate

import (
	"context"
	"errors"

	"gorm.io/gorm"

	"gorm.io/gen/internal/model"
)

// ITableInfo table info interface
type ITableInfo interface {
	GetTableColumns(schemaName string, tableName string) (result []*model.Column, err error)

	GetTableIndex(schemaName string, tableName string) (indexes []gorm.Index, err error)
}

func getTableInfo(db *gorm.DB) ITableInfo {
	return &tableInfo{db}
}

func getTableComment(db *gorm.DB, tableName string) string {
	table, err := getTableType(db, tableName)
	if err != nil || table == nil {
		return ""
	}
	if comment, ok := table.Comment(); ok {
		return comment
	}
	return ""
}

func getTableType(db *gorm.DB, tableName string) (result gorm.TableType, err error) {
	if db == nil || db.Migrator() == nil {
		return
	}
	return db.Migrator().TableType(tableName)
}

func getTableColumns(db *gorm.DB, schemaName string, tableName string, indexTag bool) (result []*model.Column, err error) {
	if db == nil {
		return nil, errors.New("gorm db is nil")
	}

	mt := getTableInfo(db)
	result, err = mt.GetTableColumns(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	if !indexTag || len(result) == 0 {
		return result, nil
	}

	index, err := mt.GetTableIndex(schemaName, tableName)
	if err != nil { //ignore find index err
		db.Logger.Warn(context.Background(), "GetTableIndex for %s,err=%s", tableName, err.Error())
		return result, nil
	}
	if len(index) == 0 {
		return result, nil
	}

	// Get index column sequences from database metadata
	indexColumnSeq, err := getIndexColumnSequences(db, schemaName, tableName)
	if err != nil {
		db.Logger.Warn(context.Background(), "GetIndexColumnSequences for %s,err=%s", tableName, err.Error())
		// Fall back to original behavior if query fails
		indexColumnSeq = make(map[string]map[string]int32)
	}

	im := model.GroupByColumnWithSequences(index, indexColumnSeq)
	for _, c := range result {
		c.Indexes = im[c.Name()]
	}
	return result, nil
}

type tableInfo struct{ *gorm.DB }

// GetTableColumns  struct
func (t *tableInfo) GetTableColumns(schemaName string, tableName string) (result []*model.Column, err error) {
	types, err := t.Migrator().ColumnTypes(tableName)
	if err != nil {
		return nil, err
	}
	for _, column := range types {
		result = append(result, &model.Column{ColumnType: column, TableName: tableName, UseScanType: t.Dialector.Name() != "mysql" && t.Dialector.Name() != "sqlite"})
	}
	return result, nil
}

// GetTableIndex  index
func (t *tableInfo) GetTableIndex(schemaName string, tableName string) (indexes []gorm.Index, err error) {
	return t.Migrator().GetIndexes(tableName)
}

// getIndexColumnSequences queries the database to get the correct column order for each index
// Returns a map: indexName -> columnName -> sequence (1-based)
func getIndexColumnSequences(db *gorm.DB, schemaName string, tableName string) (map[string]map[string]int32, error) {
	dialector := db.Dialector.Name()
	indexColumnSeq := make(map[string]map[string]int32)

	var rows *gorm.DB
	var err error

	switch dialector {
	case "postgres":
		// PostgreSQL query to get index column sequences
		// Use generate_subscripts to get the position of each column in the indkey array
		// Note: pg_index.indkey is 0-indexed, so we add 1 to get 1-based priority
		pgSchema := schemaName
		if pgSchema == "" {
			pgSchema = "public" // Default PostgreSQL schema
		}
		query := `
			SELECT 
				i.relname AS index_name,
				a.attname AS column_name,
				(pos + 1) AS seq_in_index
			FROM pg_index ix
			JOIN pg_class i ON i.oid = ix.indexrelid
			JOIN pg_class t ON t.oid = ix.indrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			JOIN LATERAL generate_subscripts(ix.indkey, 1) AS pos ON true
			JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ix.indkey[pos]
			WHERE n.nspname = ? AND t.relname = ?
			ORDER BY i.relname, pos`
		rows = db.Raw(query, pgSchema, tableName)
	case "mysql":
		// MySQL query to get index column sequences
		// If schemaName is empty, use the current database
		mysqlSchema := schemaName
		if mysqlSchema == "" {
			var currentDB string
			db.Raw("SELECT DATABASE()").Scan(&currentDB)
			mysqlSchema = currentDB
		}
		query := `
			SELECT INDEX_NAME AS index_name, COLUMN_NAME AS column_name, SEQ_IN_INDEX AS seq_in_index
			FROM information_schema.STATISTICS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
			ORDER BY INDEX_NAME, SEQ_IN_INDEX`
		rows = db.Raw(query, mysqlSchema, tableName)
	case "sqlserver":
		// SQL Server query to get index column sequences
		query := `
			SELECT 
				i.name AS index_name,
				c.name AS column_name,
				ic.key_ordinal AS seq_in_index
			FROM sys.indexes i
			JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
			JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
			JOIN sys.tables t ON i.object_id = t.object_id
			JOIN sys.schemas s ON t.schema_id = s.schema_id
			WHERE s.name = ? AND t.name = ?
			ORDER BY i.name, ic.key_ordinal`
		rows = db.Raw(query, schemaName, tableName)
	default:
		// For other databases, return empty map (fallback to original behavior)
		return indexColumnSeq, nil
	}

	sqlRows, err := rows.Rows()
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	for sqlRows.Next() {
		var indexName, columnName string
		var seqInIndex int32
		if err := sqlRows.Scan(&indexName, &columnName, &seqInIndex); err != nil {
			return nil, err
		}
		if indexColumnSeq[indexName] == nil {
			indexColumnSeq[indexName] = make(map[string]int32)
		}
		indexColumnSeq[indexName][columnName] = seqInIndex
	}

	if err := sqlRows.Err(); err != nil {
		return nil, err
	}

	return indexColumnSeq, nil
}
