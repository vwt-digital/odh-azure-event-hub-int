# Pull to Azure Event Hub function

This function is used to pull Pub/Sub subscription messages towards an Azure Event Hub instances.

## Configuration
These variables have to be defined within the environment of the function:
- `PROJECT_ID` `[string]`: The Project ID of the current GCP project;
- `CONNECTION_SECRET` `[string]`: The ID of the Secret Manager secret where the Azure Event Hub connection string is 
  defined;
- `EVENTHUB_SECRET` `[string]`: The ID of the Secret Manager secret where the Azure Event Hub access key is 
  defined;

## Invoking
The function can be invoked by sending a request towards the HTTP-endpoint of the function. Don't
forget to ensure the sender has Function Invoking permission.

Function entrypoint: `handler`

## License
[GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html)
