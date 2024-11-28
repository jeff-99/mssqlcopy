package mssql

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"

	"database/sql"

	mssql "github.com/microsoft/go-mssqldb"
	azuresql "github.com/microsoft/go-mssqldb/azuread"
)

type TableRef struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

func (t TableRef) String() string {
	quoter := mssql.TSQLQuoter{}
	return fmt.Sprintf("%s.%s", quoter.ID(t.Schema), quoter.ID(t.Table))
}

type MSSQLDB struct {
	db *sql.DB

	schemaDefs    map[string]map[string]string
	schemaDefLock *sync.Mutex
}

func Connect(host string, database string) (*MSSQLDB, error) {

	dsn := fmt.Sprintf("%s://%s?database=%s&fedauth=%s", "sqlserver", host, database, azuresql.ActiveDirectoryDefault)
	db, err := sql.Open(azuresql.DriverName, dsn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return &MSSQLDB{
		db:            db,
		schemaDefs:    make(map[string]map[string]string),
		schemaDefLock: &sync.Mutex{},
	}, nil
}

func (db *MSSQLDB) GetTablesFromFilter(ctx context.Context, schema string, filter string) ([]string, error) {
	query := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @schema AND TABLE_NAME LIKE @table_filter AND TABLE_TYPE = 'BASE TABLE'"
	rows, err := db.db.QueryContext(ctx, query, sql.Named("schema", schema), sql.Named("table_filter", filter))
	if err != nil {
		return nil, err
	}

	tables := make([]string, 0)
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			log.Fatal(err)
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func (db *MSSQLDB) GetCount(ctx context.Context, table TableRef, queryFilter string) (int, error) {
	filter, err := parseFilter(queryFilter)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", table.String(), filter.String())
	rows, err := db.db.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}

	var count int
	for rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			return 0, err
		}
	}

	return count, nil
}

func (db *MSSQLDB) GetSchemaDefinition(ctx context.Context, table TableRef) (map[string]string, error) {
	db.schemaDefLock.Lock()
	defer db.schemaDefLock.Unlock()

	if _, ok := db.schemaDefs[table.String()]; !ok {
		query := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", table.Schema, table.Table)
		rows, err := db.db.QueryContext(ctx, query)
		if err != nil {
			return nil, err
		}

		schemaMap := make(map[string]string)
		for rows.Next() {
			var column, dataType string
			err := rows.Scan(&column, &dataType)
			if err != nil {
				return nil, err
			}
			schemaMap[column] = dataType
		}

		db.schemaDefs[table.String()] = schemaMap
	}

	schema, _ := db.schemaDefs[table.String()]

	return schema, nil
}

func (db *MSSQLDB) EmptyTable(ctx context.Context, table TableRef) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s.%s", table.Schema, table.Table)
	_, err := db.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

type RowIterator struct {
	columnCount int
	rows        *sql.Rows
}

func (ri *RowIterator) Next() ([]interface{}, error) {
	if !ri.rows.Next() {
		return []interface{}{}, nil
	}

	values := make([]interface{}, ri.columnCount)
	for i := range values {
		values[i] = new(interface{})
	}

	err := ri.rows.Scan(values...)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (db *MSSQLDB) SelectFrom(ctx context.Context, table TableRef, columns []string, queryFilter string) (*RowIterator, error) {

	quoter := mssql.TSQLQuoter{}

	// Copy columns to avoid modifying the original slice
	columnsCopy := make([]string, len(columns))

	for i, column := range columns {
		columnsCopy[i] = quoter.ID(column)
	}

	filter, err := parseFilter(queryFilter)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(columnsCopy, ", "), table.String(), filter.String())
	rows, err := db.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &RowIterator{
		columnCount: len(columnsCopy),
		rows:        rows,
	}, nil
}

type ForeingKeyConstraint struct {
	Name             string
	Schema           string
	Table            string
	Column           string
	ReferencedSchema string
	ReferencedTable  string
	ReferencedColumn string
	NoCheck          string
}

func (db *MSSQLDB) GetForeignKeys(ctx context.Context, table TableRef) ([]ForeingKeyConstraint, error) {
	query := `
	SELECT 
		fk.name AS 'fk_name',
		OBJECT_SCHEMA_NAME(fk.parent_object_id) AS 'schema',
		OBJECT_NAME(fk.parent_object_id) AS 'table',
		COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS 'column',
		OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS 'referenced_schema',
		OBJECT_NAME(fk.referenced_object_id) AS 'referenced_table',
		COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS 'referenced_column_name',
		is_disabled as "no_check"
	FROM sys.foreign_keys fk
	INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
	WHERE OBJECT_NAME(fk.parent_object_id) = @table
	AND SCHEMA_NAME(fk.schema_id) =  @schema
	AND fk.type = 'F'
	`
	rows, err := db.db.QueryContext(ctx, query, sql.Named("table", table.Table), sql.Named("schema", table.Schema))
	if err != nil {
		return nil, err
	}

	foreingKeys := make([]ForeingKeyConstraint, 0)
	for rows.Next() {
		var fk ForeingKeyConstraint
		err := rows.Scan(&fk.Name, &fk.Schema, &fk.Table, &fk.Column, &fk.ReferencedSchema, &fk.ReferencedTable, &fk.ReferencedColumn, &fk.NoCheck)
		if err != nil {
			return nil, err
		}
		foreingKeys = append(foreingKeys, fk)
	}

	return foreingKeys, nil

}

func (db *MSSQLDB) GetReferencedForeignKeys(ctx context.Context, table TableRef) ([]ForeingKeyConstraint, error) {
	query := `
	SELECT 
		fk.name AS 'fk_name',
		OBJECT_SCHEMA_NAME(fk.parent_object_id) AS 'schema',
        OBJECT_NAME(fk.parent_object_id) AS 'table',
		COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS 'column',
		OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS 'referenced_schema',
		OBJECT_NAME(fk.referenced_object_id) AS 'referenced_table',
		COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS 'referenced_column_name',
		is_disabled as "no_check"
	FROM sys.foreign_keys fk
	INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
	WHERE OBJECT_NAME(fk.referenced_object_id) = @table
	AND SCHEMA_NAME(fk.schema_id) =  @schema
	AND fk.type = 'F'
	`
	rows, err := db.db.QueryContext(ctx, query, sql.Named("table", table.Table), sql.Named("schema", table.Schema))
	if err != nil {
		return nil, err
	}

	foreingKeys := make([]ForeingKeyConstraint, 0)
	for rows.Next() {
		var fk ForeingKeyConstraint
		err := rows.Scan(&fk.Name, &fk.Schema, &fk.Table, &fk.Column, &fk.ReferencedSchema, &fk.ReferencedTable, &fk.ReferencedColumn, &fk.NoCheck)
		if err != nil {
			return nil, err
		}
		foreingKeys = append(foreingKeys, fk)
	}

	return foreingKeys, nil

}

func (db *MSSQLDB) AddForeignKeys(ctx context.Context, foreignKeys []ForeingKeyConstraint) error {
	for _, fk := range foreignKeys {
		err := db.AddForeignKey(ctx, fk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *MSSQLDB) AddForeignKey(ctx context.Context, foreignKey ForeingKeyConstraint) error {
	query := `
	ALTER TABLE @schema.@table 
	WITH NOCHECK
	ADD CONSTRAINT @fk_name
	FOREIGN KEY (@column) REFERENCES @referenced_schema.@referenced_table(@referenced_column)`

	_, err := db.db.ExecContext(
		ctx,
		query,
		sql.Named("schema", foreignKey.Schema),
		sql.Named("table", foreignKey.Table),
		sql.Named("column", foreignKey.Table),
		sql.Named("fk_name", foreignKey.Name),
		sql.Named("referenced_column", foreignKey.Column),
		sql.Named("referenced_schema", foreignKey.ReferencedSchema),
		sql.Named("referenced_table", foreignKey.ReferencedTable),
	)
	if err != nil {
		return err
	}

	return nil
}

func (db *MSSQLDB) DropForeignKeys(ctx context.Context, table TableRef) error {
	foreingKeys, err := db.GetForeignKeys(ctx, table)
	if err != nil {
		return err
	}

	for _, fk := range foreingKeys {
		err := db.DropForeignKey(ctx, fk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *MSSQLDB) DropReferencedForeignKeys(ctx context.Context, table TableRef) error {
	foreingKeys, err := db.GetReferencedForeignKeys(ctx, table)
	if err != nil {
		return err
	}

	for _, fk := range foreingKeys {
		err := db.DropForeignKey(ctx, fk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *MSSQLDB) DropForeignKey(ctx context.Context, foreignKey ForeingKeyConstraint) error {
	query := fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s", foreignKey.Schema, foreignKey.Table, foreignKey.Name)
	_, err := db.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (db *MSSQLDB) BulkInsert(ctx context.Context, table TableRef, columns []string) (*BulkInsert, error) {

	// schemaDef, err := db.GetSchemaDefinition(ctx, table)
	// if err != nil {
	// 	return nil, err
	// }
	// columns := make([]string, 0, len(schemaDef))
	// for column := range schemaDef {
	// 	columns = append(columns, column)
	// }
	return NewBulkInsert(table, columns, db.db), nil
}

func (db *MSSQLDB) Close() error {
	return db.db.Close()
}

type expression struct {
	column   string
	operator string
	value    string
}

func (e expression) String() string {
	quoter := mssql.TSQLQuoter{}
	return fmt.Sprintf("( %s %s %s )", quoter.ID(e.column), strings.ToUpper(e.operator), quoter.Value(e.value))
}

type filter struct {
	expressions []expression
	operators   []string
}

func (f filter) String() string {
	if len(f.expressions) == 0 {
		return "1=1"
	}

	expressionIndex := 0
	operatorIndex := 0

	expressionCount := len(f.expressions)
	operatorCount := len(f.operators)

	var sb strings.Builder

	for {
		if expressionIndex == expressionCount && operatorIndex == operatorCount {
			break
		}

		if expressionIndex < expressionCount {
			sb.WriteString(f.expressions[expressionIndex].String())
			expressionIndex++
		}

		if operatorIndex < operatorCount {
			sb.WriteString(" ")
			sb.WriteString(f.operators[operatorIndex])
			sb.WriteString(" ")
			operatorIndex++
		}
	}

	return sb.String()
}

func splitExpressions(s string) []string {
	allParts := strings.Split(s, " ")
	parts := make([]string, 0)
	var partBuilder strings.Builder
	for _, part := range allParts {
		switch part {
		case "AND", "OR", "and", "or":
			parts = append(parts, strings.Trim(partBuilder.String(), " "))
			parts = append(parts, strings.ToUpper(part))
			partBuilder.Reset()
		default:
			partBuilder.WriteString(part + " ")

		}
	}

	if partBuilder.Len() > 0 {
		parts = append(parts, strings.Trim(partBuilder.String(), " "))
	}

	return parts
}

var expressionPattern = regexp.MustCompile(`([\[\]\"a-zA-Z0-9_ ]+?) ([=<>]{1,2}) (.+)`)

func parseFilter(queryFilter string) (filter, error) {
	if queryFilter == "" {
		return filter{}, nil
	}
	f := filter{}
	expressions := splitExpressions(queryFilter)
	for _, filterPart := range expressions {
		if filterPart == "AND" || filterPart == "OR" {
			f.operators = append(f.operators, filterPart)
			continue
		}

		matches := expressionPattern.FindStringSubmatch(filterPart)

		if len(matches) < 3 {
			return filter{}, fmt.Errorf("expression (\"%s\") only has %d parts, while we expect 3 parts", filterPart, len(matches))
		}
		f.expressions = append(f.expressions, expression{column: strings.Trim(matches[1], "\"[]"), operator: matches[2], value: strings.Trim(matches[3], "'")})
	}

	return f, nil
}
