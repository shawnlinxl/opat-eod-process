pipeline {
  agent any

  stages {

    stage('Download EOD prices') {
      steps {
        build 'stock_price_download'
      }
    }
    stage('Create Price') {
      steps {
        build 'create_price'
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
    stage('Create EOD nav') {
      steps {
        build 'create_nav'
      }
    }
    stage('Create EOD attribution') {
      steps {
        build 'create_attr'
      }
    }
    stage('Export CSV') {
      steps {
        build 'export_csv'
      }
    }
  }
}