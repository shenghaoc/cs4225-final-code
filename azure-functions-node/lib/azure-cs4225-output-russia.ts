import { MongoClient } from 'mongodb'

let db=null;

const client = new MongoClient(process.env["CosmosDbConnectionString"])

export const init = async () => {
  if(!db) {
    await client.connect()
    db = client.db('sentanaly')
  }
};
export const findItems = async (query = {}) => {
  return db.collection("sentiments_russia").find({}).toArray();
};