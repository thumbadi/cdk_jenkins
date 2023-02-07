pipeline {
    
    agent any
    stages {
        stage('CDK bootstrap') {
            steps {
                withAWS(credentials: 'jenkins-cdk', region: 'us-east-2') {
                      
                          sh '''
                          PATH=/usr/local/bin/:$PATH
                          JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin
                          cdk bootstrap
                          
                          '''
                          echo 'bootstrap'
                    
                }
                
           }
        }
    
        stage('CDK synth') {
            steps {
                withAWS(credentials: 'jenkins-cdk', region: 'us-east-2') {
                      
                          sh '''
                          PATH=/usr/local/bin/:$PATH
                          cdk synth
                          '''
                    
                }
                
           }
        }
    
        stage('CDK deploy') {
            steps {
                withAWS(credentials: 'jenkins-cdk', region: 'us-east-2') {
                      
                          sh '''
                          PATH=/usr/local/bin/:$PATH
                          cdk deploy --require-approval=never
                          '''
                    
                }
                
           }
        }

  }
}
