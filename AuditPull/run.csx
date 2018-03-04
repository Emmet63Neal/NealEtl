#r "O365ETL.dll"

using System;


public static void Run(TimerInfo myTimer, TraceWriter log)
{
    string connstring =  System.Configuration.ConfigurationManager.ConnectionStrings["AuditDb"].ConnectionString;
    string schema = System.Configuration.ConfigurationManager.ConnectionStrings["Schema"].ConnectionString;
    string clientSecret = System.Configuration.ConfigurationManager.ConnectionStrings["ClientSecret"].ConnectionString;
    string tenant = System.Configuration.ConfigurationManager.ConnectionStrings["Tenant"].ConnectionString;
    string clientId = System.Configuration.ConfigurationManager.ConnectionStrings["ClientId"].ConnectionString;
    var productKeyConfig = System.Configuration.ConfigurationManager.ConnectionStrings["ProductKey"];
    string productKey = string.Empty;
    if(productKeyConfig != null)
       productKey = productKeyConfig.ConnectionString;
    int daysToRetrieve;
    daysToRetrieve = 2;
    O365ETL.ConsoleWriter.GetInstance().Writer = log;
    for (int i = 0; i < daysToRetrieve; i++)
	{
		DateTime dateToProcess = DateTime.UtcNow.AddDays(-1*i);
	    O365ETL.ConsoleWriter.GetInstance().Write("\nProcessing " + dateToProcess);
		try
		{
			
			var result =
				O365ETL.Processor.Process(clientId, clientSecret, tenant, dateToProcess, connstring, schema, productKey)
				.Result;

		}
		catch (Exception ex)
		{
			throw(ex);
		}
	}
	
	var sql = O365ETL.SQLClient.GetInstance(connstring, schema);

	sql.CreateSP();
	sql.RunStoredProc(uspMoveStaging");
}
