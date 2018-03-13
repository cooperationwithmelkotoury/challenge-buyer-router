var body = require('body/json')
var send = require('send-data/json')

var Buyers = require('./model')

module.exports = {
  getRoute: getRoute,
  getBuyer: getBuyer,
  putBuyer: putBuyer
}

function getRoute (req, res, opts, cb) {
  var visitor = {
    state: opts.query.state,
    device: opts.query.device,
    timestamp: opts.query.timestamp
  }

  Buyers.routeVisitor(visitor, function (err, location) {
    if (err) return cb(err)

    res.writeHead(302, {Location: location})
    res.end()
  })
}

function getBuyer (req, res, opts, cb) {
  Buyers.get(opts.params.id, function (err, data) {
    if (err) return cb(err)

    send(req, res, data)
  })
}

function putBuyer (req, res, opts, cb) {
  body(req, res, function (err, data) {
    if (err) return cb(err)

    Buyers.put(data, function (err) {
      if (err) return cb(err)

      res.statusCode = 201
      send(req, res, data)
    })
  })
}
