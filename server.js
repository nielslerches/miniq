const BaseServer = require('./index').Server;
const BaseJob = require('./index').Job;

const fetch = require('node-fetch');
const uuid = require('uuid/v4');


class Reservation extends BaseJob {
  // data = { sku: string, quantity: number, order: string }

  canExecute(precedingRevervations, callback) {
    fetch(`http://localhost:5000/api/inventory/${this.data.sku}`).then(res => res.json()).then(({ data }) => {
      const totalPrecedingQuantityRequired = precedingRevervations.reduce((prev, curr) => prev + curr.data.quantity, 0);
      callback((totalPrecedingQuantityRequired + this.data.quantity) <= data);
    });
  }
}


class Server extends BaseServer {
  constructor(...args) {
    super(...args);

    // Submit Reservation to the QueueSet
    this.express.post('/:key(\\d+)', (req, res) => {
      const key = parseInt(req.params.key);
      const data = req.body;

      const reservation = new Reservation(data.id, data);
      this.queueSet.push(key, reservation);
    });
  }
}


const server = new Server('localhost', 3000, 1);
server.start();
