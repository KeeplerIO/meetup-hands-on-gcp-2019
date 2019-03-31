'use strict';

const {PubSub} = require('@google-cloud/pubsub');

exports.createTopicIfNotExists = async topicName => {
    const pubsub = new PubSub();
    // Lists all topics in the current project
    const [topics] = await pubsub.getTopics();
    console.log('Topics:');
    const topicNameRegexp = /\/([^\/]*)$/g;
    const topicNames = topics.map(topic => topicNameRegexp.exec(topic.name)[1]);
    topicNames.forEach(topicName => console.log(topicName));

    if (!topicNames.includes(topicName)) {
        // Create topic
        await pubsub.createTopic(topicName);
        console.log(`Topic ${topicName} created.`);
    }
};
