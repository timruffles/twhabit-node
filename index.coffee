# # Backend processes
sys = require("sys")
env = process.env
redis = require("redis")
events = require('events')
logger = require("./logger").forContext("Main")
url = require("url")

_ = require("underscore")

getEnv = (k,def) ->
  v = env[k] ? def
  throw new Error("Missing env var #{k}") unless v
  v

redis.debug_mode = false

createRedisClient = ->
  logger.debug "Connecting to redis"
  redisUrl = url.parse getEnv("REDIS_URL","redis://localhost:6379")
  [_user, pass] = (redisUrl.auth || ":").split(":")
  client = redis.createClient(redisUrl.port,redisUrl.hostname,auth_pass: pass)
  client.on "error", (err) ->
    logger.error "Redis client had an error"
    logger.error err
  client

redisClient = createRedisClient()

Twitter = require("ntwitter")
twit = new Twitter
  consumer_key: getEnv("TW_KEY")
  consumer_secret: getEnv("TW_SECRET")
  access_token_key: getEnv("TW_ACCESS_TOKEN")
  access_token_secret: getEnv("TW_ACCESS_SECRET")

TwitterWatcher = require("./twitter_watcher").TwitterWatcher
twitterWatcher = new TwitterWatcher(twit,redisClient)

twitterWatcher.on "tweet", (tweet) ->
  # cut down tweet to essentials, ignore retweets
  return if tweet.retweeted
  redisClient.publish getEnv("TWEET_TOPIC"), JSON.stringify({
    id: tweet.id_str
    text: tweet.text
    created_at: tweet.created_at
    hashtags: _.pluck(tweet.entities.hashtags,"text")
    user_id: tweet.user.id_str
    screen_name: tweet.user.screen_name
  })

twitterWatcher.connect {track: getEnv("USER_ID","@twhabit")}

publisher = require("./twitter_publish")
publisher.watch(redisClient,twit,"tweets_sent")
