const { MongoClient } = require('mongodb');
const fs = require('fs');
const parse = require('csv-parse');

const dbName = 'crypto_trades';
const collectionName = 'trades';

// CSV File location
const csvFilePath = '"C:\Users\KIIT\Downloads\KoinX Assignment CSV Sample.csv"';

async function main() {
    try {
        // Connecting to MongoDB
        const client = new MongoClient(mongoUrl);
        await client.connect();
        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        const parser = fs.createReadStream("C:\Users\KIIT\Downloads\KoinX Assignment CSV Sample.csv").pipe(parse({ columns: true }));
        for await (const record of parser) {
            const trade = {
                UTC_Time: new Date(record.UTC_Time),
                Operation: record.Operation.toLowerCase(), // Normalizing to lowercase
                Market: record.Market,
                BaseCoin: record.Market.split('/')[0],
                QuoteCoin: record.Market.split('/')[1],
                Amount: parseFloat(record['Buy/Sell Amount']),
                Price: parseFloat(record.Price),
            };
            await collection.insertOne(trade);
        }

        console.log('Data inserted successfully!');
    } catch (error) {
        console.error('Error:', error);
    } finally {
        client.close();
    }
}

main();
