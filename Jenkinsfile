//Jenkinsfile for Zeppelin repo for continuous integration.It contains script to build zeppelin.	

//To use variables inside shell block, they need to be defined as environment variables

/* ------------------------------------------
    BRANCH_NAME is jenkins defined variable for multibranch pipeline jobs to identify which branch is being built. 
    In this case, this is 'q-zep-0.6.0' 
    -----------------------------------------*/
env.branch=env.BRANCH_NAME

//qweez_branch is the branch from where you want qweez setup to be done
env.qweez_branch="master"

//pkg_name is the package being built
env.pkg_name="zeppelin"

env.cloud="aws"

/*  ----------------------------------------------------------------------------------
    To load a groovy script, it needs to be present on the node on which job is running, 
    therefore, jenkins repo needs to  be cloned. 
    Branch used for it is 'master'
    Pipeline jobs does not provide any other way to clone a repo, it has to be present inside the script
    -----------------------------------------------------------------------------------*/

env.jenkins_branch="master"

//Defining url which will be displayed on slack. It points to Blue ocean view now
env.url = "http://ci.qubole.net/blue/organizations/jenkins/zeppelin/detail/${env.branch}/${BUILD_NUMBER}/pipeline"

//slack channel you want notifications to be sent. A notification will be sent for failure and success
env.slack_channel="#jenkins-test-message"


//Build code starts from here

//node step defines where the job will run. It matches the node having this label
node('zeppelin-build')
{
    currentBuild.result="SUCCESS"
	
    //WORKSPACE is not an environment variable in pipeline jobs. Defining it explictly here
    env.WORKSPACE=pwd()

    //every stage appears as a step in DAG view
    stage('checkout-jenkins')
    {
        checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: jenkins_branch]], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'CloneOption', noTags: false, reference: '', shallow: true]], submoduleCfg: [], userRemoteConfigs: [[credentialsId:'b094a682-17c4-4558-bb14-f3c4ebb69ae3', url: 'git@bitbucket.org:qubole/jenkins.git']]]
    }

    stage('build-zeppelin')
    {
        def slack_color='good'
        try
        {
            //loading zeppelin-build.groovy which contains script for zeppelin build and is present on the node after jenkins checkout
            load 'zeppelin-build.groovy'
        }
        catch(err)
        {
            currentBuild.result="FAILURE"
            slack_color='bad'
            print "Build failed with error ${err}"
        }

        //notify on slack
        slackSend channel: env.slack_channel, color: slack_color, message: "${currentBuild.result}: Build [${env.pkg_name}] on branch [${env.BRANCH_NAME}]  [${env.BUILD_NUMBER}] [${env.url}]"
    }

    //archiving the artifacts
    step([$class: 'ArtifactArchiver',artifacts: 'ops/qweez-debug.log, ops/package-build-time.csv',excludes: null])

    //workspace cleanup
    step([$class: 'WsCleanup'])
}