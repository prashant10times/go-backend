module.exports = {
  apps: [{
    name: 'go-api-backend',
    script: './search-event-api',
    instances: 1,
    exec_mode: 'fork',
    interpreter: 'none',
    env: {
      PORT: 2000
    }
  }]
}
