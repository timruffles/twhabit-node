logger = require("./logger").forContext("TweetWatcher")
_ = require "underscore"

# https://dev.twitter.com/docs/streaming-api/concepts
class TweetWatcher extends require("events").EventEmitter
  constructor: (@twit,@redis) ->
    this

  connect: (filter) =>
    logger.log "Connection request with #{JSON.stringify filter}"
    @_connectionIssues = []
    @_desiredFilter = filter
    @_connect()

  _networkError: ->
    limitingIssues = @_connectionIssues.filter (i) -> i == "networkError"
    # twdocks: back off linearly. Perhaps start at 250 milliseconds and cap at 16 seconds
    delay = Math.min(16 * seconds,(limitingIssues + 1) * 250)
    @_connectionIssues.push "networkError"
    @_reconnect(new Date + delay)

  _potentialLimiting: ->
    limitingIssues = @_connectionIssues.filter (i) -> i == "potentialLimiting"
    # twdocs: back off exponentially. Perhaps start with a 10 second wait, double on each subsequent failure, and finally cap the wait at 240 seconds
    delay = Math.min(240 * seconds,Math.pow(2,limitingIssues) * 10 * seconds)
    @_connectionIssues.push "potentialLimiting"
    @_reconnect(new Date + delay)

  _reconnect: =>
    lazyDefine this, "_reconnect", afterLongestDelay =>
      logger.log "Reconnecting to stream"
      @_connect()

  # twdocs: reconnect no more than twice every four minutes
  _connect: =>
    lazyDefine this, "_connect", _.throttle(( =>
      # load keywords, establish stream
      @_makeStream @_desiredFilter, (newStream) =>
        logger.log "First data on new connection, it's working, removing any old connections"
        @_connectionIssues = []
        if @stream
          @stream.removeAllListeners("end")
          @stream.removeAllListeners("destroy")
          @stream.destroy()
        @stream = newStream
        connected = true
    ), 120 * seconds)

  _makeStream: (filter, established) ->
    twitterEvents = this
    onEstablished = _.once established
    @twit.stream "statuses/filter", filter, (stream) =>
      logger.log "Connection established, tracking '#{JSON.stringify @_desiredFilter}'"
      stream.on "data", (data) =>
        # twdocs: clear connected on first response
        onEstablished()
        # tweet IDs are too long for JS, need to use the string everywhere
        logger.debug "Tweet received, #{data.id}, #{data.id_str} #{data.text}"
        data.id = data.id_str
        twitterEvents.emit("tweet",data)
      stream.on "end", (evt) =>
        logger.error "Tweet stream ended with #{evt.statusCode}"
        logger.error "Is the system clock set correctly? #{new Date().toString()} OAuth can fail if it's not" if evt.statusCode == 401
        @_potentialLimiting()
      stream.on "error", (evt) =>
        logger.error "ERROR on tweet stream"
        logger.dir arguments
        @_networkError()
      stream.on "destroy", =>
        logger.error "Tweet stream destroyed"
        logger.dir arguments
        @_potentialLimiting()

afterLongestDelay = (delay,fn) ->
  currentDelay = 0
  timeout = false
  ->
    return if currentDelay > +new Date + delay
    currentDelay = +new Date + delay
    clearTimeout timeout
    timeout = setTimeout fn, delay
lazyDefine = (ctx,as,fn) ->
  ctx[as] = fn
  fn()

seconds = 1000

exports.TwitterWatcher = TweetWatcher
