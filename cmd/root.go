package cmd

import (
	"fmt"
	"os"

	"github.com/jeff-99/mssqlcopy/pkg/cli"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "asqlcp",
	Short: "Azure SQL Server Copy - Copy data from one Azure SQL Server to another",
	Long: `Azure SQL Server Copy - Copy data from one Azure SQL Server to another
	Example: 
	
	asqlcp -s source.database.windows.net -d sourceDB -t target.database.windows.net -p targetDB -c schema -f filter -o 5


	`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {

		sourceHost, _ := cmd.Flags().GetString("sourceHost")
		sourceDB, _ := cmd.Flags().GetString("sourceDB")
		targetHost, _ := cmd.Flags().GetString("targetHost")
		targetDB, _ := cmd.Flags().GetString("targetDB")
		schema, _ := cmd.Flags().GetString("schema")
		tableFilter, _ := cmd.Flags().GetString("tableFilter")
		queryFilter, _ := cmd.Flags().GetString("queryFilter")
		parrallel, _ := cmd.Flags().GetInt("parrallel")
		ci, _ := cmd.Flags().GetBool("ci")

		if sourceHost == "" || sourceDB == "" || targetHost == "" || targetDB == "" || schema == "" || tableFilter == "" {
			fmt.Println("Not all required flags are set, redirecting to interactive mode")
			cli.Wizard(sourceHost, sourceDB, targetHost, targetDB, schema, tableFilter, queryFilter, parrallel, ci)
			os.Exit(1)
		}

		cli.Copy(sourceHost, sourceDB, targetHost, targetDB, schema, tableFilter, queryFilter, parrallel, ci)

	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {

	rootCmd.Flags().String("sourceHost", "", "The source database host")
	rootCmd.Flags().String("sourceDB", "", "The source database name")
	rootCmd.Flags().String("targetHost", "", "The target database host")
	rootCmd.Flags().String("targetDB", "", "The target database name")
	rootCmd.Flags().String("schema", "", "The schema to copy")
	rootCmd.Flags().String("tableFilter", "", "The filter to apply to the tables")
	rootCmd.Flags().String("queryFilter", "", "The filter to apply to the tables")
	rootCmd.Flags().Int("parrallel", 5, "The number of tables to copy in parallel")
	rootCmd.Flags().Bool("ci", false, "Enables CI runner output instead of interactive mode")

}
