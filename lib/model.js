var redis = require('../src/redis')
var redisClient = redis({host: 'localhost', port: 6379})

module.exports = {
  routeVisitor: routeVisitor,
  get: get,
  put: put
}

function get (id, cb) {
  const key = 'buyer-' + id
  redisClient.get(key, function (err, val) {
    if (err) return cb(err)

    cb(err, JSON.parse(val))
  })
}

function put (data, cb) {
  const key = 'buyer-' + data.id
  redisClient.set(key, JSON.stringify(data), function (err) {
    if (err) return cb(err)

    cb(err, data)
  })
}

function routeVisitor (visitor, cb) {
  redisClient.scan('0', 'MATCH', 'buyer-*', function (err, dbset) {
    if (err) return cb(err)

    var redisMulti = redisClient.multi()
    dbset[1].forEach(function (key) {
      redisMulti.get(key)
    })

    redisMulti.exec(function (err, results) {
      if (err) return cb(err)

      var candidates = getCandidates(results, visitor)
      cb(err, getLocation(candidates))
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

function isCandidate (offer, visitor) {
  const state = visitor.state
  const device = visitor.device
  const hour = new Date(visitor.timestamp).getUTCHours()
  const day = new Date(visitor.timestamp).getUTCDay()

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
