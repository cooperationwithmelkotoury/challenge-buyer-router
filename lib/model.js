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

    data.offers.forEach(function (offer, index) {
      addIndex(offer.criteria, data.id, index)
    })

    cb(err, data)
  })
}

function addIndex (criteria, id, index) {
  criteria.device.forEach(function (device) {
    redisClient.sadd('device:' + device, id + '-' + index)
  })
  criteria.hour.forEach(function (hour) {
    redisClient.sadd('hour:' + hour, id + '-' + index)
  })
  criteria.day.forEach(function (day) {
    redisClient.sadd('day:' + day, id + '-' + index)
  })
  criteria.state.forEach(function (state) {
    redisClient.sadd('state:' + state, id + '-' + index)
  })
}

function routeVisitor (visitor, cb) {
  const state = 'state:' + visitor.state
  const device = 'device:' + visitor.device
  const hour = 'hour:' + new Date(visitor.timestamp).getUTCHours()
  const day = 'day:' + new Date(visitor.timestamp).getUTCDay()

  redisClient.sinter(state, device, day, hour, function (err, res) {
    if (err) return cb(err)

    var candidates = []
    var ids = getIds(res)

    redisClient.mget(ids, function (err, values) {
      if (err) return cb(err)

      values.forEach(function (value, index) {
        var offerId = getOfferIndex(res, index)
        candidates.push(JSON.parse(value).offers[offerId])
      })

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

function getOfferIndex (res, index) {
  return res[index].substring(res[index].indexOf('-') + 1)
}

function getIds (res) {
  var ids = []

  res.forEach(function (eachRes) {
    var id = eachRes.substring(0, eachRes.indexOf('-'))
    ids.push('buyer-' + id)
  })

  return ids
}
