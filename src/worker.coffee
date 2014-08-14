"use strict"
# ------------------------------------------------------------
#
# Worker class
#
kue       = require "kue"
deferred  = require "deferred"
Domain    = require "domain"

sleep = (msec)->
  d = deferred()
  setTimeout ->
    d.resolve(null)
  , msec
  d.promise

module.exports = class Worker
  @uid: do ->
    id = 0
    -> id++

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
    @id      = Worker.uid()
    @jobs    = null
    @ctx     = null
    @current = null

  #
  # ## process
  #
  # * @param  job{Object}       required
  #   * job.type
  #   * job.data
  # * @return {Promise({null|Error})}
  #
  process: (job)->
    # console.error "Worker: define class extends this"
    sleep(2000)
    .then(->
      if Math.random() < 0.
        throw new Error("random error")
      else
        null
    )

  #
  # ## subscribe
  #
  # * @param  type{String}      required
  # * @return {Worker}
  #
  subscribe: (type)->
    throw new Error("type required") unless type? and typeof type is "string"
    console.log "Worker(#{@id}): subscribes #{type}"
    @jobs = kue.createQueue(
      prefix: @options.prefix or "q"
      redis:
        host: @redis.host or "localhost"
        port: @redis.port or 6379
        db:   @redis.db or 0
        auth: @redis.auth or null
    )

    @jobs.process(type, (job, done, ctx)=>
      console.log "Worker(#{@id}): start  processing #{job.type}:#{job.data.title}"
      @current =
        ctx: ctx
        job: job
      domain = Domain.create()
      domain.run =>
        @process(job)
        .then((result)-> done(null, result))
        .catch((err)-> console.log err; done(err))
        .finally(=>
          console.log "Worker(#{@id}): finish processing #{job.type}:#{job.data.title}"
          @current = null
        )

      domain.on("error", (err)->
        @current = null
        done(err)
      )
    )
    @

  #
  # ## close
  #
  # * @return {Promise({null|Error})}
  #
  close: ->
    d = deferred()
    # @current がなければ shutdonw して完了
    unless @current
      @jobs.shutdown((err)->
        return d.reject err if err
        d.resolve()
      )
      return d.promise

    # @current があれば pause して、次が来ないようにしておく
    @current.ctx.pause((err)=>
      # @current の Job が完了するのを待つ
      @jobs.shutdown((err)->
        return d.reject err if err
        d.resolve(null)
      )
    )
    d.promise

# ------------------------------------------------------------
#
# # WorkerDaemon
#
class WorkerDaemon
  #
  # ## constructor
  #
  constructor: ->
    @workers = {}

  #
  # ## addWorker
  #
  # * @param  type{String}      required
  # * @param  workers{Worker}   required
  # * @return {WorkerDaemon}
  #
  addWorker: (type, worker)->
    throw new Error("type is require") unless type? and typeof type is "string"
    throw new Error("worker is require") unless worker? and worker instanceof Worker

    console.log "Daemon: add worker (#{worker.id}) for #{type}"
    @workers[type] = worker
    @

  #
  # ## run
  #
  # * @return {Promise({null|Error})}
  #
  run: ->
    workers = (worker: worker, type: type for type, worker of @workers)
    deferred.map(workers, (worker)=>
      worker.worker.subscribe(worker.type)
    )

  #
  # ## close
  #
  # * @return {Promise({null|Error})}
  #
  close: ->
    workers = (worker: worker, type: type for type, worker of @workers)
    deferred.map(workers, (worker)=>
      console.log "Daemon: shuting down worker (#{worker.worker.id})"
      worker.worker.close()
      .then(->
        console.log "Daemon: worker (#{worker.worker.id}) shutdown"
      )
      .catch((err)->
        console.log "Daemon: worker (#{worker.worker.id}) shutdown failure: #{err.message}"
        throw err
      )
    )

unless module.parent
  commander = require "commander"
  program = commander
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

  # instantiate workerDaemon
  workerDaemon = new WorkerDaemon()
  # add workers
  workerDaemon
    .addWorker("hoge", new Worker(redisConf))
    .addWorker("fuga", new Worker(redisConf))

  # workerDaemon start and workers subscribe it's own type
  workerDaemon.run().then -> console.log "start workerDaemon"

  process.on "SIGINT", ->
    console.log "closing workerDaemon"
    workerDaemon.close()
    .then(-> process.exit 0)
    .catch((err)-> console.error err; process.exit -1)
