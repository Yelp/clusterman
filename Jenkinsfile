@Library('jenkinsfile_stdlib') _
import com.yelpcorp.releng.Utils
import com.yelpcorp.releng.EEMetrics

yproperties()
utils = new Utils()
eeMetrics = new EEMetrics()

SERVICE_NAME = 'clusterman'
DEPLOY_GROUPS = ['prod.non_canary', 'dev.everything']
IRC_CHANNELS = ['clusterman']
EMAILS = ['distsys-compute@yelp.com']
DIST = ['xenial']  # TODO need to resupport multiple versions (CLUSTERMAN-211)

commit = ''
authors = [:]

node {
    eeMetrics.emitEvent("${env.job_name}", 'jenkins', "${env.job_name}-${env.build_id}", 'start', '{"event_category": "deploy"}')
}

utils.handleInputRejection {
    ircMsgResult(IRC_CHANNELS) {
        emailResult(EMAILS) {
            node('trusty') {
                ystage('clone') {
                    commit = clone("services/${SERVICE_NAME}")['GIT_COMMIT']
                    eeMetrics.emitLink('sha', "${commit}", 'jenkins', "${env.JOB_NAME}-${env.BUILD_ID}")

                    // Dont look, grossness
                    for (deploy_group in DEPLOY_GROUPS) {
                        current = sh(script: "paasta get-latest-deployment --service ${SERVICE_NAME} --deploy-group ${deploy_group} || git hash-object -t tree /dev/null", returnStdout: true).trim()
                        author = sh(script: "git log --format=%ae ${current}..${commit} | sort -u | cut -d@ -f1 | xargs --no-run-if-empty", returnStdout: true).trim()

                        authors[deploy_group] = author
                    }
                }

                ystage(eeMetricsWorkflow: 'test', 'test') {
                    sh(script: 'make test')
                }

                ystage('itest') {
                    sh(script: $/paasta itest --service ${SERVICE_NAME} --commit ${commit}/$)
                }

                ystage('security-check') {
                    try {
                        sh(script: $/paasta security-check --service services-${SERVICE_NAME} --commit ${commit}/$)
                    } catch (hudson.AbortException e) {
                        // Do nothing
                    }
                }

                ystage('push-to-registry') {
                    sh(script: $/paasta push-to-registry --service ${SERVICE_NAME} --commit ${commit}/$)
                }

                ystage('performance-check') {
                    sh(script: $/paasta performance-check --service ${SERVICE_NAME}/$)

                    if (!wasTimerTriggered() && authors['prod.non_canary']) {
                        pingList = authors['prod.non_canary'].split(' ').collect{author -> "<@${author}>"}.join(', ')
                        utils.nodebot(IRC_CHANNELS, "Hey ${pingList}, go click the button! :easy_button: y/clusterman-jenkins")
                    }

                    timeout(time: 1, unit: 'HOURS') { input "Click to advance to next step" }
                }
            }

            // Runs `make itest_${version}` and attempts to upload to apt server if not an automatically timed run
            // This will automatically break all the steps into stages for you
            debItestUpload("services/${SERVICE_NAME}", DIST)

            // Now do the paasta service deploy
            node('trusty') {

                ystage(eeMetricsWorkflow: 'deploy-prod', 'prod.non_canary') {
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
    eeMetrics.emitEvent("${env.job_name}", 'jenkins', "${env.job_name}-${env.build_id}", 'end')
}

private boolean wasTimerTriggered() {
    (currentBuild.rawBuild.getCause(hudson.triggers.TimerTrigger$TimerTriggerCause)) ? true : false
}
