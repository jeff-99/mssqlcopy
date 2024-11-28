package azure

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armsubscriptions"
)

type AzureClient struct {
	// cred *azidentity.ChainedTokenCredential	
	cred *azidentity.DefaultAzureCredential
}

func NewAzureClient() (*AzureClient, error) {
	cred, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{
		AdditionallyAllowedTenants: []string{"*"},
		DisableInstanceDiscovery: false,
	})
	if err != nil {
		return nil, err
	}
	return &AzureClient{cred: cred}, nil
}

type DatabaseRef struct {
	serverName string
	databaseName string
}

func NewDatabaseRef(serverName, databaseName string) DatabaseRef {
	return DatabaseRef{
		serverName: serverName,
		databaseName: databaseName,
	}
}

func (db DatabaseRef) String() string {
	return fmt.Sprintf(fmt.Sprintf("%s://%s.database.windows.net?database=%s", "sqlserver", db.serverName, db.databaseName))
}

func(db DatabaseRef) ServerName() string {
	return fmt.Sprintf("%s.database.windows.net", db.serverName)
}

func(db DatabaseRef) DatabaseName() string {
	return db.databaseName
}
 

func(az *AzureClient) ListDatabases(ctx context.Context) ([]DatabaseRef, error) {
	
	subClient, err := armsubscriptions.NewClient(az.cred, &arm.ClientOptions{})

	if err != nil {
		return nil, err
	}

	pager := subClient.NewListPager(&armsubscriptions.ClientListOptions{})
	if err != nil {
		return nil, err
	}

	subscriptions := make([]*armsubscriptions.Subscription, 0)
	for pager.More(){
		subs, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, sub := range subs.Value{
			subscriptions = append(subscriptions, sub)
		}
	}

	databaseResources := make([]*armresources.GenericResourceExpanded, 0)

	for _, sub := range subscriptions {
		client, err := armresources.NewClient(*sub.SubscriptionID, az.cred, nil)
		if err != nil {
			return nil, err
		}

		filter := "resourceType eq 'Microsoft.Sql/servers/databases'"
		expand := "createdTime"
		pager := client.NewListPager(&armresources.ClientListOptions{
			Expand: &expand,
			Filter: &filter,
		})

		for pager.More() {
			resources, err := pager.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, resource := range resources.Value {
				databaseResources = append(databaseResources, resource)
			}
		}
	}


	dbs := make([]DatabaseRef, 0, len(databaseResources))
	for _, resource := range databaseResources {
		resourceName := *resource.Name

		// Parse the resource name by splitting on the `/` character
		parts := strings.Split(resourceName, "/")
		dbs = append(dbs, DatabaseRef{
			serverName: parts[0],
			databaseName: parts[1],
		})
			
	}

	return dbs, nil
}