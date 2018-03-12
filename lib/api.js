var body = require('body/json')
var send = require('send-data/json')

var redis = require('../src/redis')
var redisClient = redis({host: 'localhost', port: 6379})

module.exports = {
  getRoute: getRoute,
  getBuyer: getBuyer,
  put: put
}

function getRoute (req, res, opts, cb) {
  var cursor = '0'

  redisClient.scan(cursor, 'MATCH', 'buyer-*', function (err, dbset) {
    if (err) return cb(err)

    var redisMulti = redisClient.multi()
    dbset[1].forEach(function (key) {
      redisMulti.get(key)
    })

    redisMulti.exec(function (err, results) {
      if (err) return cb(err)

      var candidates = getCandidates(results, opts)
      var location = getLocation(candidates)

      res.writeHead(302, {Location: location})
      res.end()
    })
  })
}

function getBuyer (req, res, opts, cb) {
  const key = 'buyer-' + opts.params.id
  redisClient.get(key, function (err, val) {
    if (err) return cb(err)

    send(req, res, JSON.parse(val))
  })
}

function put (req, res, opts, cb) {
  body(req, res, function (err, data) {
    if (err) return cb(err)

    const key = 'buyer-' + data.id
    redisClient.set(key, JSON.stringify(data), function (err) {
      if (err) return cb(err)

      res.statusCode = 201
      send(req, res, data)
    })
  })
}

function getLocation (candidates) {
  var topCandidate = candidates[0] // can winner be undefined?

  candidates.forEach(function (candidate) {
    if (candidate.value > topCandidate.value) topCandidate = candidate
  })

  return topCandidate.location
}

function isCandidate (offer, opts) {
  const state = opts.query.state
  const device = opts.query.device
  const hour = new Date(opts.query.timestamp).getUTCHours()
  const day = new Date(opts.query.timestamp).getUTCDay()

  const hasState = offer.criteria.state.includes(state)
  const hasDevice = offer.criteria.device.includes(device)
  const hasHour = offer.criteria.hour.includes(hour)
  const hasDay = offer.criteria.day.includes(day)

  if (hasState && hasDevice && hasHour && hasDay) return true
  return false
}

function getCandidates (results, opts) {
  var candidates = []

  results.forEach(function (result) {
    JSON.parse(result).offers.forEach(function (offer) {
      if (isCandidate(offer, opts)) candidates.push(offer)
    })
  })

  return candidates
}
