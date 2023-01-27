pipeline {
    
    agent any
    stages {
        stage('CDK bootstrap') {
            steps {
                withAWS(credentials: 'jenkins-cdk', region: 'us-east-2') {
                      
                          sh '''
                     
                          JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.13.jdk/Contents/Home/bin/
                          PATH=/usr/local/bin/:$PATH
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
                          JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.13.jdk/Contents/Home/bin/
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
                          JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.13.jdk/Contents/Home/bin/
                          cdk deploy --require-approval=never
                          '''
                    
                }
                
           }
        }

  }
}
