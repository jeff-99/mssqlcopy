package cli

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jeff-99/mssqlcopy/pkg/azure"
)

func Wizard(sourceHost, sourceDB, targetHost, targetDB, schema, filter, queryFilter string, parrallel int, ci bool) {
	var dbs []azure.DatabaseRef
	if sourceHost == "" || sourceDB == "" || targetHost == "" || targetDB == "" {
		fmt.Println("Scanning for databases... (this may take a while)")

		azureClient, err := azure.NewAzureClient()
		if err != nil {
			log.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
		defer cancel()

		dbs, err = azureClient.ListDatabases(ctx)
		if err != nil {
			log.Fatal(err)
		}

		sort.Slice(dbs, func(i, j int) bool {
			return dbs[i].DatabaseName() < dbs[j].DatabaseName()
		})

		for i, db := range dbs {
			fmt.Println(fmt.Sprintf("  %d: Server: %s, Database: %s", i, db.ServerName(), db.DatabaseName()))
		}

	}

	var sourceDBRef azure.DatabaseRef
	if sourceHost == "" || sourceDB == "" {
		sourceDBIndexStr := input("Select a source database, by entering it's number: ")
		sourceDBIndex, err := strconv.Atoi(strings.Trim(sourceDBIndexStr, "\n"))
		if err != nil {
			log.Fatal(err)
		}
		sourceDBRef = dbs[sourceDBIndex]
	} else {
		sourceDBRef = azure.NewDatabaseRef(sourceHost, sourceDB)
	}

	var targetDBRef azure.DatabaseRef
	if targetHost == "" || targetDB == "" {

		targetDBIndexStr := input("Select a target database, by entering it's number: ")
		targetDBIndex, err := strconv.Atoi(strings.Trim(targetDBIndexStr, "\n"))
		if err != nil {
			log.Fatal(err)
		}
		targetDBRef = dbs[targetDBIndex]
	} else {
		targetDBRef = azure.NewDatabaseRef(targetHost, targetDB)
	}

	if schema == "" {
		schema = input("Enter the schema to copy: ")
	}

	if filter == "" {
		filter = input("Enter the filter to apply to the tables (wildcard: %): ")
	}

	fmt.Println("COMMAND: azsqlcp --sourceHost", sourceDBRef.ServerName(), "--sourceDB", sourceDBRef.DatabaseName(), "--targetHost", targetDBRef.ServerName(), "--targetDB", targetDBRef.DatabaseName(), "--schema", schema, "--tableFilter", fmt.Sprintf("\"%s\"", filter), "--parrallel ", parrallel)

	Copy(sourceDBRef.ServerName(), sourceDBRef.DatabaseName(), targetDBRef.ServerName(), targetDBRef.DatabaseName(), schema, filter, queryFilter, 10, ci)

}
