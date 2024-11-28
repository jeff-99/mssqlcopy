package mssql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)
	


func TestParseFilterSimpleStatement(t *testing.T) {
	filter, err := parseFilter("column = 'value'")
	assert.NoError(t, err)
	assert.Equal(t, filter.String(), "( [column] = 'value' )")
	
}

func TestParseFilterMultipleStatements(t *testing.T) {
	filter, err := parseFilter("column = 'value' AND column2 = 'value2' OR column3 = 'value3'")
	assert.NoError(t, err)
	assert.Equal(t, filter.String(), "( [column] = 'value' ) AND ( [column2] = 'value2' ) OR ( [column3] = 'value3' )")
	
}

func TestParseFilterSQLInjection(t *testing.T) {
	filter, err := parseFilter("column1 = ; DROP TABLE users --")
	assert.NoError(t, err)
	assert.Equal(t, filter.String(), "( [column1] = '; DROP TABLE users --' )")
	
}