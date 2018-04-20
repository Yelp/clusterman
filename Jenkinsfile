@Library('jenkinsfile_stdlib') _
import com.yelpcorp.releng.Utils

yproperties()
utils = new Utils()

SERVICE_NAME = 'clusterman'
DEPLOY_GROUPS = ['prod.non_canary', 'dev.everything']
IRC_CHANNELS = ['clusterman']
EMAILS = ['distsys-compute@yelp.com']

commit = ''
authors = [:]

node {
    utils.emitEeMetric("${env.job_name}", "jenkins", "${env.job_name}-${env.build_id}", "start", '{"event_category": "deploy"}')
}

utils.handleInputRejection {
    ircMsgResult(IRC_CHANNELS) {
        emailResult(EMAILS) {
            node('trusty') {
                ystage('clone') {
                    clone("services/${SERVICE_NAME}")
                    commit = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
                    sh(script: "ee-metrics link sha ${commit} jenkins ${env.JOB_NAME}-${env.BUILD_ID}", returnStatus: true)

                    // Dont look, grossness
                    for (deploy_group in DEPLOY_GROUPS) {
                        current = sh(script: "paasta get-latest-deployment --service ${SERVICE_NAME} --deploy-group ${deploy_group} || git hash-object -t tree /dev/null", returnStdout: true).trim()
                        author = sh(script: "git log --format=%ae ${current}..${commit} | sort -u | cut -d@ -f1 | xargs --no-run-if-empty", returnStdout: true).trim()

                        authors[deploy_group] = author
                    }
                }

                ystage('test') {
                    status = "passed"
                    utils.emitEeMetric("${env.job_name}", "jenkins", "${env.job_name}-${env.build_id}-make-test", "start", '{"event_category": "test"}')
                    try {
                        sh(script: "make test")
                    } catch (e) {
                        status = "failed"
                        throw e
                    } finally {
                        utils.emitEeMetric("${env.job_name}", "jenkins", "${env.job_name}-${env.build_id}-make-test", "end", "{\"status\": \"${status}\"}")
                    }

                }

                ystage('paasta-itest') {
                    sh(script: $/paasta itest --service ${SERVICE_NAME} --commit ${commit}/$)
                }

                ystage('security-check') {
                    try {
                        sh(script: $/paasta security-check --service ${SERVICE_NAME} --commit ${commit}/$)
                    } catch (hudson.AbortException e) {
                        // Do Nothing
                    }
                }

                ystage('push-to-registry') {
                    sh(script: $/paasta push-to-registry --service ${SERVICE_NAME} --commit ${commit}/$)
                }

                ystage('performance-check') {
                    sh(script: $/paasta performance-check --service ${SERVICE_NAME}/$)

                    timeout(time: 1, unit: 'HOURS') { input "Click to advance to next step" }
                }
            }

            // Runs `make itest_${version}` and attempts to upload to apt server if not an automatically timed run
            // This will automatically break all the steps into stages for you
            debItestUpload("services/${SERVICE_NAME}", DIST)

            // Now do the paasta service deploy
            node('trusty') {

                ystage('prod.non_canary') {
                    paastaDeploy(SERVICE_NAME, commit, 'prod.non_canary', waitForDeployment: true, confirmation: false, deployTimeout: false, autoRollback: false)

                    ircMsgPaastaDeploy(SERVICE_NAME, IRC_CHANNELS, 'prod.non_canary', authors['prod.non_canary'])
                }

                ystage('dev.everything') {
                    paastaDeploy(SERVICE_NAME, commit, 'dev.everything', waitForDeployment: true, confirmation: false, deployTimeout: false, autoRollback: false)
                }
            }
        }
    }
}

node {
    utils.emitEeMetric("${env.job_name}", "jenkins", "${env.job_name}-${env.build_id}", "end")
}
