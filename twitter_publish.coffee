logger = require("./logger").forContext("tweetPublish")
_ = require "underscore"

exports.watch = (redis,twitter,topic) ->
  twitter.verifyCredentials (err) ->
    if err
      logger.error err
      process.exit 1
    listen(redis,twitter,topic)

listen = (redis,twitter,topic) ->
  redis.on("message",handleMessage.bind(null,twitter))
  redis.subscribe(topic)
  logger.info("Listening for '#{topic}'")

handleMessage = (twitter,_channel,message) ->
  twitter.updateStatus message, (err,data) ->
    if err
      logger.error "Couldn't send: '#{message}': #{err}"
