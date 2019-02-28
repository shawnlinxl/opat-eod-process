pipeline {
  agent any

  stages {
    stage('Download EOD prices') {
      steps {
        build 'stock_price_download'
      }
    }
    stage('Create EOD holdings') {
      steps {
        build 'create_holdings'
      }
    }
    stage('Create EOD pnl') {
      steps {
        build 'create_pnl'
      }
    }
    stage('Backup DB') {
      steps {
        build 'database_backup'
      }
    }
  }
}