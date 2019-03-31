'use strict';

const {PubSub} = require('@google-cloud/pubsub');
const {Storage} = require('@google-cloud/storage');
const readline = require('readline');
const topicManager = require('./topic_manager');

const waitFor = async (seconds) => {
  return new Promise((resolve, reject) => {
    setTimeout(resolve, seconds * 1000);
  });
};

const startPublisher = async (bucket, filePath, waitTime) => {
  const pubsub = new PubSub();
  const storage = new Storage();

  const topicName = 'meetup-gcp-topic';
  await topicManager.createTopicIfNotExists(topicName);

  const myBucket = storage.bucket(bucket);
  const file = myBucket.file(filePath);

  const rl = readline.createInterface({
    input: file.createReadStream(),
    crlfDelay: Infinity
  });

  const lines = [];

  rl.on('line', line => {
    lines.push(line);
  });

  rl.on('close', async () => {
    for (const line of lines) {
      console.log("Publishing " + line);
      const messageId = await pubsub.topic(topicName).publish(Buffer.from(line));
      console.log(`Message ${messageId} published.`);
      await waitFor(waitTime);
    }
  });
};

if (process.argv.length < 5) {
  console.log("Usage:\n node pubsub_publisher.js <bucket> <file path> <wait time between event>");
  process.exit(1);
}
startPublisher(process.argv[2], process.argv[3], process.argv[4]);
