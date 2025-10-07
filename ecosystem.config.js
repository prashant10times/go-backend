module.exports = {
  apps: [{
    name: 'go-api-backend',
    script: './main',
    instances: 1,
    exec_mode: 'fork',
    interpreter: 'none',
    env: {
      PORT: 2000
    }
  }]
}
