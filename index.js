// @ts-check

const http = require('http');

const pathToReg = require('path-to-regexp');
const qs = require('qs');
const uuid = require('uuid/v4');
const winston = require('winston');

const express = require('express');
const bodyParser = require('body-parser');
const morgan = require('morgan');
const cors = require('cors');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.simple(),
  defaultMeta: { service: 'miniq' },
  transports: []
});

if (process.env.NODE_ENV !== 'production') {
  logger.add(new winston.transports.Console({
    format: winston.format.simple()
  }));
} else {
  logger.add(new winston.transports.File({ filename: 'error.log', level: 'error' }));
  logger.add(new winston.transports.File({ filename: 'combined.log' }));
}

/**
 * @param {string[]} keys
 */
const createSortCb = (keys) => {
  return (a, b) => {
    for (let _key of keys) {
      let key = _key;
      if (key.startsWith('-')) {
        key = key.slice(1, key.length);       
      }
      const asc = key == _key;

      if (a[key] < b[key]) return (asc ? -1 : 1);
      if (a[key] > b[key]) return (asc ? 1 : -1);
    }
  };
}

class QueueSet {
  /**
   * Create a new QueueSet
   * @param {number} n Number of queues
   * @param {number} intervalMs Milliseconds for queue event-loop
   */
  constructor(n, intervalMs = 250) {
    if (n <= 0) {
      throw new Error('number of queues in QueueSet needs to greater than 0');
    }

    this.intervalMs = intervalMs;
    /** @type {Queue[]} */
    this.queues = [];
    /** @type {[number, Job][]} */
    this.buffer = [];

    this.start = this.start.bind(this);
    this.stop = this.stop.bind(this);
    this.scale = this.scale.bind(this);
    this.push = this.push.bind(this);
    this.bufferJob = this.bufferJob.bind(this);

    this.scale(n, false, false);
  }

  /**
   * Start the QueueSet
   */
  start() {
    logger.log('info', 'Starting QueueSet');
    for (let queue of this.queues) {
      queue.start();
    }
    logger.log('info', 'Started QueueSet');
  }

  /**
   * Stop the QueueSet
   */
  stop() {
    logger.log('info', 'Stopping QueueSet');
    for (let queue of this.queues) {
      queue.stop();
    }
    logger.log('info', 'Stopped QueueSet');
  }

  /**
   * Scale a QueueSet's number of queues up or down
   * @param {number} n Number of queues
   * @param {boolean} dropJobs (DANGEROUS) Use this to remove pending jobs instead of exhausting the queue.
   */
  scale(n, dropJobs = false, startQueues = true) {
    logger.log('info', 'Checking if QueueSet needs scaling');
    const oldScale = Number(this.queues.length);
    if (n > this.queues.length) {
      logger.log('info', 'Started scaling QueueSet up', { "old": oldScale, "new": n });
      for (let i = this.queues.length; i < n; i++) {
        const queue = new Queue(i, this.intervalMs);
        this.queues.push(queue);
      }
      logger.log('info', 'Stopped scaling QueueSet up', { "old": oldScale, "new": n });
    } else if (n < this.queues.length) {
      logger.log('info', 'Started scaling QueueSet down', { "old": oldScale, "new": n });
      const queueIndicesToRemove = [];

      for (let i = n; i < this.queues.length; i++) {
        const queue = this.queues[i];
        queue.stop();
        queueIndicesToRemove.push(i); logger.log('info', 'Marked Queue for removal', { key: i });
      }

      for (let i of queueIndicesToRemove) {
        logger.log('info', 'Started exhausting Queue', { key: i });
        const queue = this.queues[i];
        if (dropJobs) {
          queue.jobs = [];
        } else {
          while(queue.jobs.length > 0) {
            queue.loop();
          }
        }
        logger.log('info', 'Stopped exhausting Queue', { key: i });
      }

      for (let i of queueIndicesToRemove) {
        this.queues[i] = undefined;
      }

      this.queues = this.queues.filter(queue => queue !== undefined);

      let keyJob = this.buffer.shift();
      while (keyJob !== undefined) {
        const [key, job] = keyJob;
        this.push(key, job);
        keyJob = this.buffer.shift();
      }

      logger.log('info', 'Stopped scaling QueueSet down', { "old": oldScale, "new": n });
    }

    if (startQueues) {
      for (let queue of this.queues) {
        queue.start();
      }
    }
  }

  /**
   * Push a new job to the QueueSet. Distributes the job to the appropriate Queue by the provided key
   * @param {number} key Queue distribution key (key % Number of queues) 
   * @param {Job} job Job to execute
   */
  push(key, job) {
    const queue = this.queues[key % this.queues.length];
    if (queue.canPush) {
      queue.push(job);
    } else {
      this.bufferJob(key, job);
    }
  }

  /**
   * Adds a job to the buffer.
   * @param {number} key
   * @param {Job} job
   */
  bufferJob(key, job) {
    if (this.buffer.filter(([_key, _job]) => _job.id === job.id).length === 0) {
      this.buffer = this.buffer.concat([[key, job]]).sort(createSortCb(['key', 'utcTimestamp']));
    }
  }
}

class Queue {
  /**
   * Create a new Queue
   * @param {number} key Distribution key
   * @param {number} intervalMs Milliseconds for queue event-loop
   * @param {boolean} initialCanPush If jobs can be pushed to the queue before it's started processing
   */
  constructor(key, intervalMs, initialCanPush = true) {
    this.key = key;
    this.intervalMs = intervalMs;
    this.canPush = initialCanPush;

    /** @type {Job[]} */
    this.jobs = [];
    this.interval = null;

    this.start = this.start.bind(this);
    this.stop = this.stop.bind(this);
    this.loop = this.loop.bind(this);
    this.push = this.push.bind(this);
  }

  /**
   * Start the Queue
   */
  start() {
    this.canPush = true;
    logger.log('info', 'Checking if internal Queue loop is running', { key: this.key });
    if (this.interval === null) {
      this.interval = setInterval(this.loop, this.intervalMs);
      logger.log('info', 'Started internal Queue loop', { key: this.key });
    }
  }

  /**
   * Stop the Queue
   */
  stop() {
    this.canPush = false;
    logger.log('info', 'Checking if internal Queue loop is stopped', { key: this.key });
    if (this.interval !== null) {
      clearInterval(this.interval);
      this.interval = null;
      logger.log('info', 'Stopped internal Queue loop', { key: this.key });
    }
  }

  /**
   * Poll for pending jobs to process.
   */
  loop() {
    let jobIndexToPop = null;

    // Schedule job to execution
    for (let i = 0; i < this.jobs.length; i++) {
      const job = this.jobs[i];

      if (!job.executed && !job.executing) {
        jobIndexToPop = i;
        break;
      }
    }

    // Execute the job
    if (jobIndexToPop !== null) {
      const job = this.jobs[jobIndexToPop];

      if (!job.executed && !job.executing) {
        job.canExecute(this.jobs.slice(0, jobIndexToPop), (res) => {
          if (res) {
            job.execute();
            logger.log('info', 'Checking if Job can be removed from Queue', { key: this.key, id: job.id });
            if (!job.executing && job.executed) {
              this.jobs.splice(jobIndexToPop, 1);
              logger.log('info', 'Removed Job from Queue', { key: this.key, id: job.id });
            }
          }
        });
      }
    }
  }

  /**
   * Push a new job to the Queue
   * @param {Job} job Job to execute
   */
  push(job) {
    this.jobs.push(job);
  }
}

class Job {
  /**
   * Create a new Job
   * @param {string} id Id of the job instance.
   * @param {object} data Data for the job instance
   * @param {null | Date} utcTimestamp Timestamp
   */
  constructor(id, data, utcTimestamp = null) {
    this.id = id;
    this.data = data;
    this.utcTimestamp = utcTimestamp || new Date(Date.now());

    this.executed = false;
    this.executing = false;

    this.execute = this.execute.bind(this);
    this.canExecute = this.canExecute.bind(this);
  }

  /**
   * Execute the job
   */
  execute() {
    logger.log('info', 'Executing job', { id: this.id });
    this.executing = true;
    this.executed = true;
    logger.log('info', 'Executed job', { id: this.id });
    this.executing = false;
  }

  /**
   * Job execution predicate
   * @param {Job[]} precedingJobs 
   */
  canExecute(precedingJobs, callback) {
    callback(true);
  }
}

class Server {
  /**
   * Create a new miniq server
   * @param {string} host 
   * @param {number} port 
   * @param {number} n
   * @param {number} intervalMs
   */
  constructor(host, port, n, intervalMs = 250) {
    this.serializeQueueSet = this.serializeQueueSet.bind(this);
    this.start = this.start.bind(this);
    this.stop = this.stop.bind(this);

    this.host = host;
    this.port = port;

    const queueSet = new QueueSet(n, intervalMs);
    const app = express();

    app.use(morgan('combined'));
    app.use(bodyParser.urlencoded({ extended: false }));
    app.use(bodyParser.json());
    app.use(cors());

    app.get('/', (req, res) => {
      res.send({
        data: this.serializeQueueSet()
      });
    });

    app.get('/:key(\\d+)', (req, res) => {
      const key = parseInt(req.params.key);
      const _queueSet = this.serializeQueueSet()
      const queue = _queueSet.queues.filter(queue => queue.key === key)[0];

      res.send({
        data: queue || null
      });
    });

    this.queueSet = queueSet;
    this.express = app;
  }

  serializeQueueSet() {
    const _queueSet = { queues: [], buffer: this.queueSet.buffer };
    for (let _queue of this.queueSet.queues) {
      _queueSet.queues.push({
        key: _queue.key,
        canPush: _queue.canPush,
        jobs: _queue.jobs
      });
    }
    return _queueSet;
  }

  /**
   * Start the miniq server
   */
  start() {
    logger.log('info', 'Starting miniq server');
    const _exit = () => {
      this.stop();
      process.exit();
    };
    process.on('SIGINT', _exit);
    process.on('SIGTSTP', _exit);

    this.queueSet.start();
    logger.log('info', 'Starting HTTP server');
    this.express.listen(this.port, this.host);
    logger.log('info', 'Started HTTP server');
    logger.log('info', 'Started miniq server');
    logger.log('info', `Listening on http://${this.host}:${this.port}`);
  }

  /**
   * Stop the miniq server
   */
  stop() {
    logger.log('info', 'Stopping miniq server');
    logger.log('info', 'Stopping HTTP server');
    this.express.removeAllListeners();
    logger.log('info', 'Stopped HTTP server');
    this.queueSet.stop();
    logger.log('info', 'Stopped miniq server');
  }
}

module.exports = {
  Server: Server,
  Job: Job,
  QueueSet: QueueSet,
  Queue: Queue
};
