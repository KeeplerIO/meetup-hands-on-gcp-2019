'use strict';

const {PubSub} = require('@google-cloud/pubsub');
const topicManager = require('./topic_manager');

const startSubscriber = async () => {
  const pubsub = new PubSub();

  const topicName = 'meetup-gcp-topic';
  await topicManager.createTopicIfNotExists(topicName);

  const subscriptionName = 'meetup-gcp-subscription';
  const timeout = 60;

  // Lists all subscriptions for the topic
  const [subscriptions] = await pubsub.topic(topicName).getSubscriptions();
  const subscriptionNameRegexp = /\/([^\/]*)$/g;
  const subscriptionNames = subscriptions.map(sub => subscriptionNameRegexp.exec(sub.name)[1]);
  console.log('Subscriptions:');
  subscriptionNames.forEach(s => console.log(s));
  // Create subscription if needed
  if(!subscriptionNames.includes(subscriptionName)) {
    console.log("Creating subscription");
    await pubsub.topic(topicName).createSubscription(subscriptionName);
  }

  const subscription = pubsub.subscription(subscriptionName);

  // Create an event handler to handle messages
  let messageCount = 0;
  const messageHandler = message => {
    console.log(`Received message ${message.id}:`);
    console.log(`\tData: ${message.data}`);
    console.log(`\tAttributes: ${message.attributes}`);
    messageCount += 1;

    // "Ack" (acknowledge receipt of) the message
    message.ack();
  };

  // Listen for new messages until timeout is hit
  subscription.on(`message`, messageHandler);
  console.log("Waiting for messages");

  setTimeout(() => {
    subscription.removeListener('message', messageHandler);
    console.log(`${messageCount} message(s) received.`);
  }, timeout * 1000);
};

startSubscriber();
