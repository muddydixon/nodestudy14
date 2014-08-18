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
  # * @param  options{Object}   optional
  #
  constructor: (@options = {})->

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
      if Math.random() < 0
        throw new Error("random error")
      else
        null
    )

# ------------------------------------------------------------
#
# # WorkerDaemon
#
class WorkerDaemon
  #
  # ## constructor
  #
  constructor: (@redis = {}, @options = {})->
    @workers = {}
    @active = {}

  #
  # ## addWorker
  #
  # * @param  type{String}      required
  # * @param  workers{Worker}   required
  # * @return {WorkerDaemon}
  #
  addWorker: (type, worker)->
    throw new Error("type is require") unless type? and typeof type is "string"

    console.log "Daemon: add worker (#{worker.id}) for #{type}"
    @workers[type] = worker
    @

  #
  # ## run
  #
  # * @return {Promise({null|Error})}
  #
  run: ->
    @jobs = kue.createQueue(
      prefix: @options.prefix or "q"
      redis:
        host: @redis.host or "localhost"
        port: @redis.port or 6379
        db:   @redis.db or 0
        auth: @redis.auth or null
    )

    for type of @workers
      do (type)=>
        @jobs.process(type, (job, done, ctx)=>
          id = job.data?.title or uuid()
          console.log "WorkerDaemon: start  processing #{job.type}:#{job.data.title}"
          @active[id] =
            ctx: ctx
            job: job
          domain = Domain.create()
          domain.run =>
            worker = new @workers[job.type](@options)
            worker.process(job)
            .then((result)-> done(null, result))
            .catch((err)-> console.log err.stack; done(err))
            .finally(=>
              console.log "WorkerDaemon: finish processing #{job.type}:#{job.data.title}"
              delete @active[id]
            )

          domain.on("error", (err)->
            console.log "catch err #{err.message} and delete active[#{id}]"
            delete @active[id]
            done(err)
          )
        )
    deferred(@)

  #
  # ## close
  #
  # * @return {Promise({null|Error})}
  #
  close: ->
    console.log "WorkerDaemon close #{Object.keys(@active).length}"
    console.log Object.keys(@active)
    d = deferred()
    # @active がなければ shutdonw して完了
    if Object.keys(@active).length is 0
      @jobs.shutdown((err)->
        return d.reject err if err
        d.resolve()
      )
      return d.promise

    # @active があれば pause して、次が来ないようにしておく
    actives = (active for type, active of @active)
    deferred.map(actives, (active)->
      dd = deferred()
      console.log "worker #{active.job.data.title} pause"
      active.ctx.pause((err)=>
        console.log "worker #{active.job.data.title} paused"
        return dd.reject err if err
        dd.resolve()
      )
      dd.promise
    )
    .then(=>
      console.log "worker all active is done"
      # @active の Job が完了するのを待つ
      @jobs.shutdown((err)->
        return d.reject err if err
        d.resolve(null)
      )
    )
    d.promise

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
  workerDaemon = new WorkerDaemon(redisConf)
  # add workers
  workerDaemon
    .addWorker("hoge", Worker)
    .addWorker("fuga", Worker)

  # workerDaemon start and workers subscribe it's own type
  workerDaemon.run().then -> console.log "start workerDaemon"

  process.on "SIGINT", ->
    console.log "closing workerDaemon"
    workerDaemon.close()
    .then(-> process.exit 0)
    .catch((err)-> console.error err; process.exit -1)
