const express = require('express');
const bodyParser = require('body-parser');
const MongoClient = require('mongodb').MongoClient;

const app = express();
const port = 3000;

app.use(bodyParser.json());

const url = 'mongodb://localhost:27017';
const dbName = 'trade';

MongoClient.connect(url, (err, client) => {
  if (err) {
    console.error('Error connecting to MongoDB:', err);
    return;
  }
  const db = client.db(trade);

  app.post('/api/balance', async (req, res) => {
    try {
      const { timestamp } = req.body;
      const result = await db.collection('transactions').aggregate([
        {
          $match: { timestamp: { $lt: new Date(timestamp) } }
        },
        {
          $group: {
            _id: '$asset',
            totalBuy: {
              $sum: { $cond: [{ $eq: ['$type', 'buy'] }, '$amount', 0] }
            },
            totalSell: {
              $sum: { $cond: [{ $eq: ['$type', 'sell'] }, '$amount', 0] }
            }
          }
        },
        {
          $project: {
            _id: 0,
            asset: '$_id',
            balance: { $subtract: ['$totalBuy', '$totalSell'] }
          }
        }
      ]).toArray();

      const balanceMap = {};
      result.forEach(({ asset, balance }) => {
        balanceMap[asset] = balance;
      });

      res.json(balanceMap);
    } catch (error) {
      console.error('Error calculating balance:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  app.listen(port, () => {
    console.log(`Server running at http://127.0.0.1/:${5000}`);
  });
});
