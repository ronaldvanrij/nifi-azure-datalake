# NiFi Processor for Azure Data Lake

This code implements a processor to store NiFi flowfiles in Azure Data Lake.

## Usage
Copy the [NAR file](http://git.openenergi.net/projects/DAT/repos/nifi-azure-datalake/browse/azure-datalake-1.0-SNAPSHOT.nar)
 to NiFi's `lib` folder and restart.  Configuration requires the following parameters from Azure:
* Client Id, Client secret, Subscription Id, and Storage account name from the [Azure portal](https://portal.azure.com).
* Tenant Id from Windows Powershell (type `Login-AzureRmAccount` and login to get this).

The content of the flow file will be written to the path specified in Azure Data Lake, using the filename provided
in the flow file's attribute.  If the file already exists, it can either be overwritten, appended or routed to the
processor's failure output.

This doesn't yet support chunking, so may not work well for very large individual flow files.

## To compile
Compile using `mvn clean install`.  It is currently compiled for Java 8.

The component relies on Microsoft's Azure Data Lake Store Java SDK, which is documented
[here](https://azure.microsoft.com/en-gb/documentation/articles/data-lake-store-get-started-java-sdk/) and
[here](https://github.com/Azure/azure-sdk-for-java).