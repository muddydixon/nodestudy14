"use strict"
# ------------------------------------------------------------
#
# Producer
#
request   = require "request"
commander = require "commander"
uuid      = require "uuid"

program = commander
  .option("-t,--type <type>", "job type", String, "hoge")
  .option("-p,--port <port>", "server port", Number, 1222)
  .option("-h,--host <host>", "server host", String, "localhost")
  .parse(process.argv)

opt =
  url: "http://#{program.host}:#{program.port}/job"
  method: "POST"
  form:
    type: program.type
    data:
      title: uuid()
      data: 0|Math.random() * 1000

request(opt, (err, body, res)->
  console.log
)
