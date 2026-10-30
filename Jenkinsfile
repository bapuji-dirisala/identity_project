// Top-level Jenkins entry point. Pulls shared library pipeline steps from
// your enterprise Jenkins library server. The actual deploy config
// (environments, S3 targets, Brickflow, Airflow) lives in
// products/identity_project/AutoJenkinsfile and is loaded automatically.

library identifier: "cop-pipeline-step",  changelog: false
library identifier: "cicd-pipeline",      changelog: false
library identifier: "dataeng-jsl",        changelog: false
library identifier: "ae-build-pipeline",  changelog: false
library identifier: "common-utils-cicd",  changelog: false


pipeline {
    agent none
    options {
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '30'))
        copyArtifactPermission("*")
    }
    stages {
        stage('Delta Build detector') {
            // Only build the subprojects touched by this commit (mono-repo optimization).
            agent any
            steps {
                reBuild([:])
                checkout([
                    $class                           : 'GitSCM',
                    branches                         : [[name: env.GIT_COMMIT]],
                    doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
                    extensions                       : scm.extensions,
                    userRemoteConfigs                : scm.userRemoteConfigs
                ])
                deltaBuild([:])
            }
        }
    }
}
