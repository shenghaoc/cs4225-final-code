import { AzureFunction, Context, HttpRequest } from "@azure/functions";
import * as db from "../lib/azure-cs4225-output-russia";

const httpTrigger: AzureFunction = async function (
  context: Context,
  req: HttpRequest
): Promise<void> {
  try {
    let response = null;

    // create 1 db connection for all functions
    await db.init();

    switch (req.method) {
      case "GET":
  
          // allows empty query to return all items
          const dbQuery =
            req?.query?.dbQuery || (req?.body && req?.body?.dbQuery);
          response = {
            documentResponse: await db.findItems(dbQuery),
          };
        
        break;
      default:
        throw Error(`${req.method} not allowed`)
    }

    context.res = {
      body: response,
    };
  } catch (err) {
    context.log(`*** Error throw: ${JSON.stringify(err)}`);

    context.res = {
      status: 500,
      body: err,
    };
  }
};

export default httpTrigger;