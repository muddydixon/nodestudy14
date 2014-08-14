"use strict"
# ------------------------------------------------------------
#
# Server
#
kue       = require "kue"
deferred  = require "deferred"
express   = require "express"

module.exports = class Server
  #
  # ## constructor
  #
  # * @param  redis{Object}     optional
  #   * redis.host{String}      default "localhost"
  #   * redis.port{Number}      default 6379
  #   * redis.db{Number}        default 0
  #   * redis.auth{String}      default ""
  # * @param  options{Object}   optional
  #
  constructor: (@redis = {}, @options = {})->
    @jobs = null
    @Job = null
    @web  = null

  #
  # ## run
  #
  # * @return {Promise({null|Error})}
  #
  run: ->
    @Job = kue.Job
    @jobs = kue.createQueue(
      prefix: @options.prefix or "q"
      redis:
        host: @redis.host or "localhost"
        port: @redis.port or 6379
        db:   @redis.db or 0
        auth: @redis.auth or null
    )

    # completeした Job は基本見ないので Redis を圧迫するくらいなら削除
    # expireをかけるタイミングがない！(直接、kue.jobから登録すれば可能)
    @jobs.on "job complete", (id, result)=>
      console.log result
      @getJob(id)
      .then((job)=>
        d = deferred()
        job.remove((err)->
          return d.reject err if err
          d.resolve()
        )
        d.promise
      )
      .catch((err)-> console.error "Server: #{err.message}")

    @jobs.on "job failed", (id)=>
      @getJob(id)
      .catch((err)-> console.error "Server: #{err.message}")

    return deferred(@) unless @options.port

    app = express()
    if @options.auth?.user? and @options.auth?.pass?
      app.use express.basicAuth @options.auth.user, @options.auth.pass
    app.use kue.app
    @web = app.listen @options.port
    d = deferred()
    @web.on "listening", =>
      d.resolve @
    d.promise


  #
  # ## getJob
  #
  getJob: (id)->
    d = deferred()
    @Job.get(id, (err, job)->
      return d.reject err if err
      d.resolve job
    )
    d.promise

  #
  # ## close
  #
  # * @return {Promise({null|Error})}
  close: ->
    deferred(
      =>
        d = deferred()
        @jobs.close (err)=>
          return d.reject err if err
          d.resolve(null)
        d.promise
      =>
        d = deferred()
        @web.close()
        @web.on "close", (err)->
          return d.reject err if err
          d.resolve(null)
        d.promise
    )

unless module.parent
  commander = require "commander"
  program = commander
    .option("-p,--port <port>", "port if use web app", Number)
    .option("--redis-host <host>", "redis host", String)
    .option("--redis-port <port>", "redis port", Number)
    .option("--redis-db   <db>"  , "redis db",   Number)
    .option("--redis-auth <auth>", "redis auth", String)
    .parse(process.argv)

  redisConf =
    host: program.redisHost
    port: program.redisPort
    db:   program.redisDb
    auth: program.redisAuth

  # start server
  server = new Server(redisConf, {port: program.port})
  server.run().then(-> console.log "Server: start" + (if program.port? then " on port #{program.port}" else ""))

  process.on "SIGINT", ->
    console.log "Server: close"
    server.close()
    .then(-> process.exit 0)
    .catch((err)-> console.error err; process.exit -1)
